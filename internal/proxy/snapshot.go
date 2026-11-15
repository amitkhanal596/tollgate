package proxy

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/amitkhanal596/tollgate/internal/auth"
	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/health"
	"github.com/amitkhanal596/tollgate/internal/ratelimit"
)

// Snapshot is one fully built, immutable view of the gateway configuration.
// The reconcile loop builds it off to the side and publishes it with a
// single atomic Store; every request Loads it once and uses only that
// snapshot for its whole lifetime (ADR 0002).
//
// A Snapshot owns no goroutines and nothing that needs closing: it is plain
// data plus pointers to long-lived objects (Backends from the Registry,
// transports from the pool). An old snapshot is simply garbage collected
// after the last request using it finishes.
type Snapshot struct {
	// Version is the config version this snapshot was built from.
	Version int64
	// Config is the source config, kept for status endpoints and debugging.
	// It must not be modified.
	Config *config.Config

	router    router
	upstreams map[string]*upstreamRT

	// backendIDs and transportKeys describe what this snapshot references.
	// After the swap, the reconcile loop prunes the Registry and transport
	// pool down to them.
	backendIDs    map[string]struct{}
	transportKeys map[transportKey]struct{}

	// healthTargets is one active-probe target per backend of every
	// upstream with a health_check, handed to Checker.Sync after the swap.
	healthTargets []health.Target
}

// routeRT is the runtime form of one config.Route.
type routeRT struct {
	cfg      config.Route
	name     string
	host     string // normalized; "" = any host
	prefix   string
	upstream *upstreamRT
	timeouts resolvedTimeouts
	// proxy is built once per route per snapshot rather than per request:
	// its settings (transport, error handler) only change with the config.
	// The per-request backend reaches its Rewrite hook through the request
	// context.
	proxy *httputil.ReverseProxy

	// authn verifies credentials; nil for auth: none. One instance per auth
	// mode per snapshot, shared by every route using that mode.
	authn auth.Authenticator
	// challenge is the WWW-Authenticate value of this route's 401s. It
	// depends only on the mode, never on what was wrong with the request.
	challenge string
	// limit is the per-client rate limit; nil means not rate limited.
	limit *ratelimit.Limit
	// limitKeyPrefix is name + "\x00", precomputed so building the
	// per-request bucket key is a single concatenation.
	limitKeyPrefix string
}

// upstreamRT is the runtime form of one config.Upstream.
type upstreamRT struct {
	name     string
	lb       balancer.Balancer
	backends []*balancer.Backend // from the Registry: same pointers across swaps
	// passive is a private copy of the upstream's passive_health (the
	// Config must not be modified, and must not modify us); nil = off.
	passive *config.PassiveHealth
}

// WWW-Authenticate challenges, one per auth mode (RFC 7235 section 4.1).
// No error="invalid_token" detail (RFC 6750): that would tell a client
// whether its credentials were missing or wrong.
const (
	challengeAPIKey = `APIKey realm="tollgate"`
	challengeJWT    = `Bearer realm="tollgate"`
)

// resolvedTimeouts are fully defaulted: route field, then
// Defaults.Timeouts, then the built-in config.Default* constant.
type resolvedTimeouts struct {
	dial           time.Duration
	responseHeader time.Duration
	request        time.Duration
}

// builder carries the long-lived dependencies a snapshot build needs.
type builder struct {
	registry   *balancer.Registry
	transports *transportPool
	buffers    *bufferPool
	now        func() time.Time
	getenv     func(string) string          // JWT HS256 secret lookup; nil = os.Getenv
	readFile   func(string) ([]byte, error) // JWT RS256 key file; nil = os.ReadFile
	logger     *slog.Logger
	errLog     *log.Logger // bridge for httputil.ReverseProxy.ErrorLog
}

// build turns cfg into a Snapshot. It runs in two passes: the first only
// inspects cfg, constructs the authenticators and can fail; the second
// creates Registry entries and transports and cannot fail. That ordering
// means a rejected config never leaves half-registered Backends or orphan
// transports behind.
func (bd *builder) build(cfg *config.Config) (*Snapshot, error) {
	urls, err := checkConfig(cfg)
	if err != nil {
		return nil, err
	}
	authns, err := bd.buildAuthenticators(cfg)
	if err != nil {
		// A route whose authenticator cannot be built would have to either
		// fail open or reject everything; rejecting the whole config keeps
		// the old snapshot (and its working auth) serving instead.
		return nil, fmt.Errorf("proxy: build snapshot for version %d: %w", cfg.Version, err)
	}

	s := &Snapshot{
		Version:       cfg.Version,
		Config:        cfg,
		upstreams:     make(map[string]*upstreamRT, len(cfg.Upstreams)),
		backendIDs:    make(map[string]struct{}),
		transportKeys: make(map[transportKey]struct{}),
	}

	for _, u := range cfg.Upstreams {
		backends := make([]*balancer.Backend, 0, len(urls[u.Name]))
		seen := make(map[string]struct{}, len(urls[u.Name]))
		for _, bu := range urls[u.Name] {
			b := bd.registry.Get(u.Name, bu)
			// A backend listed twice would otherwise get double weight.
			if _, dup := seen[b.ID]; dup {
				continue
			}
			seen[b.ID] = struct{}{}
			s.backendIDs[b.ID] = struct{}{}
			backends = append(backends, b)
		}
		if u.Consul != nil {
			// Phase 5 fills Consul upstreams from discovery. Until then the
			// set is empty, so its routes answer 503 rather than failing
			// the whole config.
			bd.logger.Warn("consul upstreams are not supported yet; upstream has no backends",
				"upstream", u.Name)
		}
		if u.HealthCheck != nil {
			// Upstreams without a health_check contribute no targets, which
			// makes the Checker stop their probes and reset them to up.
			for _, b := range backends {
				s.healthTargets = append(s.healthTargets, health.Target{Backend: b, Check: *u.HealthCheck})
			}
		}
		var passive *config.PassiveHealth
		if u.PassiveHealth != nil {
			p := *u.PassiveHealth
			passive = &p
		}
		s.upstreams[u.Name] = &upstreamRT{
			name:     u.Name,
			lb:       bd.newBalancer(u, backends),
			backends: backends,
			passive:  passive,
		}
	}

	routes := make([]*routeRT, 0, len(cfg.Routes))
	for _, r := range cfg.Routes {
		t := resolveTimeouts(r.Timeouts, cfg.Defaults.Timeouts)
		key := transportKey{dial: t.dial, responseHeader: t.responseHeader}
		s.transportKeys[key] = struct{}{}
		upstream := s.upstreams[r.Upstream]
		rr := &routeRT{
			cfg:            r,
			name:           r.Name,
			host:           normalizeConfigHost(r.Host),
			prefix:         r.PathPrefix,
			upstream:       upstream,
			timeouts:       t,
			proxy:          bd.newRouteProxy(r.Name, bd.transports.get(key), upstream.passive),
			limitKeyPrefix: r.Name + "\x00",
		}
		switch r.Auth {
		case config.AuthAPIKey:
			rr.authn, rr.challenge = authns.apiKey, challengeAPIKey
		case config.AuthJWT:
			rr.authn, rr.challenge = authns.jwt, challengeJWT
		}
		if r.RateLimit != nil {
			rr.limit = &ratelimit.Limit{RatePerSecond: r.RateLimit.RequestsPerSecond, Burst: r.RateLimit.Burst}
		}
		routes = append(routes, rr)
	}
	s.router = newRouter(routes)
	return s, nil
}

// newBalancer picks the balancing algorithm for an upstream (ADR 0003).
func (bd *builder) newBalancer(u config.Upstream, backends []*balancer.Backend) balancer.Balancer {
	switch u.Algorithm {
	case config.AlgorithmLeastConn:
		return balancer.NewLeastConn(backends, bd.now)
	default: // "" or round_robin; checkConfig rejected anything else
		return balancer.NewRoundRobin(backends, bd.now)
	}
}

// authenticators holds the per-snapshot Authenticator for each auth mode;
// a nil field means no route uses that mode.
type authenticators struct {
	apiKey auth.Authenticator
	jwt    auth.Authenticator
}

// buildAuthenticators constructs each authenticator once per config, and
// only if some route needs it: an unused, half-configured jwt block (say,
// a secret env var not set on this host) must not reject the config.
// checkConfig has already ensured auth.jwt is set when a route needs it.
func (bd *builder) buildAuthenticators(cfg *config.Config) (authenticators, error) {
	var needAPIKey, needJWT bool
	for _, r := range cfg.Routes {
		needAPIKey = needAPIKey || r.Auth == config.AuthAPIKey
		needJWT = needJWT || r.Auth == config.AuthJWT
	}
	var out authenticators
	var errs []error
	if needAPIKey {
		a, err := auth.NewAPIKey(cfg.Auth.APIKeys)
		if err != nil {
			errs = append(errs, err)
		}
		out.apiKey = a
	}
	if needJWT {
		a, err := auth.NewJWT(*cfg.Auth.JWT, bd.getenv, bd.readFile, bd.now)
		if err != nil {
			errs = append(errs, err)
		}
		out.jwt = a
	}
	return out, errors.Join(errs...)
}

// checkConfig is the failing half of build: it verifies everything the
// snapshot depends on and parses backend URLs, without side effects. It
// returns every problem at once so an operator can fix them in one go.
// Config sources already Validate; these checks only cover what would
// otherwise make the snapshot unusable (or panic), so a hand-built or
// buggy config is rejected and the old snapshot keeps serving.
func checkConfig(cfg *config.Config) (map[string][]*url.URL, error) {
	if cfg == nil {
		return nil, errors.New("proxy: nil config")
	}
	var errs []error
	urls := make(map[string][]*url.URL, len(cfg.Upstreams))
	for _, u := range cfg.Upstreams {
		if _, dup := urls[u.Name]; dup {
			errs = append(errs, fmt.Errorf("upstream %q: duplicate name", u.Name))
			continue
		}
		switch u.Algorithm {
		case "", config.AlgorithmRoundRobin, config.AlgorithmLeastConn:
		default:
			errs = append(errs, fmt.Errorf("upstream %q: unknown algorithm %q", u.Name, u.Algorithm))
		}
		parsed := make([]*url.URL, 0, len(u.Backends))
		for _, raw := range u.Backends {
			bu, err := parseBackendURL(raw)
			if err != nil {
				errs = append(errs, fmt.Errorf("upstream %q: %w", u.Name, err))
				continue
			}
			parsed = append(parsed, bu)
		}
		urls[u.Name] = parsed
	}
	for _, r := range cfg.Routes {
		if _, ok := urls[r.Upstream]; !ok {
			errs = append(errs, fmt.Errorf("route %q: unknown upstream %q", r.Name, r.Upstream))
		}
		if !strings.HasPrefix(r.PathPrefix, "/") {
			errs = append(errs, fmt.Errorf("route %q: path_prefix %q must start with /", r.Name, r.PathPrefix))
		}
		// An unknown auth mode must fail closed here: at request time it
		// would otherwise look like "none" and serve the route unauthenticated.
		switch r.Auth {
		case "", config.AuthNone, config.AuthAPIKey:
		case config.AuthJWT:
			if cfg.Auth.JWT == nil {
				errs = append(errs, fmt.Errorf("route %q: auth jwt requires auth.jwt settings", r.Name))
			}
		default:
			errs = append(errs, fmt.Errorf("route %q: unknown auth mode %q", r.Name, r.Auth))
		}
		// The limiter treats an invalid Limit as unlimited (fail open), so
		// a bad one must be rejected here instead.
		if l := r.RateLimit; l != nil && !(l.RequestsPerSecond > 0 && !math.IsInf(l.RequestsPerSecond, 1) && l.Burst >= 1) {
			errs = append(errs, fmt.Errorf("route %q: rate_limit needs requests_per_second > 0 and burst >= 1", r.Name))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("proxy: build snapshot for version %d: %w", cfg.Version, err)
	}
	return urls, nil
}

// parseBackendURL accepts an absolute http(s) URL and reduces it to scheme
// and host, so equivalent spellings ("http://h:1" and "http://h:1/") get the
// same Registry ID and therefore share state.
func parseBackendURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("backend %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("backend %q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("backend %q: missing host", raw)
	}
	return &url.URL{Scheme: u.Scheme, Host: u.Host}, nil
}

// resolveTimeouts applies route -> defaults -> built-in for each field.
func resolveTimeouts(route, defaults config.Timeouts) resolvedTimeouts {
	return resolvedTimeouts{
		dial:           pickTimeout(route.Dial, defaults.Dial, config.DefaultDialTimeout),
		responseHeader: pickTimeout(route.ResponseHeader, defaults.ResponseHeader, config.DefaultResponseHeaderTimeout),
		request:        pickTimeout(route.Request, defaults.Request, config.DefaultRequestTimeout),
	}
}

// pickTimeout returns the first positive value. Zero means "inherit";
// negative values are invalid config and treated the same way rather than
// producing an already-expired deadline.
func pickTimeout(route, defaults config.Duration, builtin time.Duration) time.Duration {
	if d := time.Duration(route); d > 0 {
		return d
	}
	if d := time.Duration(defaults); d > 0 {
		return d
	}
	return builtin
}
