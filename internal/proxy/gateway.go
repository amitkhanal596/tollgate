// Package proxy is the data plane: it composes routing, authentication, rate
// limiting, load balancing, health checking and reverse proxying behind an
// atomically swapped, immutable Snapshot (docs/architecture.md sections 4-5,
// ADR 0002).
package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/discovery"
	"github.com/amitkhanal596/tollgate/internal/health"
	"github.com/amitkhanal596/tollgate/internal/metrics"
	"github.com/amitkhanal596/tollgate/internal/ratelimit"
)

// Options configures a Gateway. The zero value is usable: every nil field
// gets a production default.
type Options struct {
	// Logger receives structured logs. Nil discards them.
	Logger *slog.Logger
	// Now is the gateway's clock: passive-ejection expiry, JWT exp/nbf
	// checks, and the clock of the limiters the Gateway creates itself. Nil
	// uses time.Now. Tests inject a fake clock.
	Now func() time.Time

	// Limiter enforces the per-route rate limits (route.rate_limit), keyed
	// by route name + "\x00" + client identity.
	//
	// Nil (production) makes the Gateway create TWO ratelimit.TokenBuckets
	// (clock: Now, MaxKeys: LimiterMaxKeys), both swept by Gateway.Run: one
	// for authenticated identities (api_key/jwt routes) and one for
	// IP-keyed clients (auth: none routes). Kept apart so an anonymous flood
	// from many addresses, which can fill a key table, can never leave
	// authenticated clients without a slot (they would be denied with
	// AtCapacity).
	//
	// An injected Limiter replaces BOTH of them (one instance serves both
	// kinds of identity, so that isolation is then the caller's concern).
	// It is used as-is: the caller owns it and runs any sweeper it needs;
	// the Gateway never starts goroutines for it.
	Limiter ratelimit.Limiter
	// AuthLimiter is the failed-auth throttle (ADR 0004, architecture.md
	// section 11 row 10): keyed by client-IP key, fixed policy burst 10,
	// refill 1/s. It must be a different instance from Limiter, so an
	// auth-failure flood cannot spend the route limiters' key budget. Nil
	// makes the Gateway create and sweep its own (clock: Now, MaxKeys:
	// LimiterMaxKeys); an injected one is owned by the caller.
	AuthLimiter ratelimit.Limiter
	// Getenv resolves auth.jwt.hmac_secret_env when a snapshot is built.
	// Nil uses os.Getenv.
	Getenv func(string) string
	// ReadFile reads auth.jwt.public_key_file when a snapshot is built.
	// Nil uses os.ReadFile.
	ReadFile func(string) ([]byte, error)
	// HealthClient sends active health probes. The Checker uses a shallow
	// copy that never follows redirects and shares this client's Transport.
	// Nil gives the Checker a private transport whose idle connections are
	// closed when Run returns.
	HealthClient *http.Client
	// LimiterMaxKeys caps the buckets of EACH limiter the Gateway creates
	// itself (the authenticated and the IP-keyed route limiters and the
	// failed-auth throttle; injected limiters are not affected). 0 means
	// ratelimit.DefaultMaxKeys.
	LimiterMaxKeys int

	// Metrics receives the data-plane metrics. Nil: no metrics.
	Metrics *metrics.Metrics
	// AccessLogger receives one metrics.LogAccess line per request. Nil: no
	// access log.
	AccessLogger *slog.Logger
	// Resolver supplies the backends of upstreams with `consul:`. Nil: such
	// upstreams have no backends (503) and a warning is logged.
	Resolver discovery.Resolver
	// DiscoveryReadyTimeout bounds how long Ready() stays false after the
	// FIRST applied config while a Consul service it references has not
	// delivered its first endpoint set (ADR 0007, Startup). 0 means 10s.
	DiscoveryReadyTimeout time.Duration
	// BackendGrace is how long a backend that left the live set (a
	// discovery flap) stays in the Registry, so that a re-add within the
	// period gets the same *Backend with its ejection and in-flight state.
	// Measured with Now. 0 means 60s.
	BackendGrace time.Duration
}

// Defaults of the fix-round-5 options.
const (
	defaultDiscoveryReadyTimeout = 10 * time.Second
	defaultBackendGrace          = 60 * time.Second
)

// Failed-auth throttle policy (ADR 0004, architecture.md section 11 row 10).
// Constants rather than a package-level Limit var: no global mutable state.
const (
	authFailureRate  = 1.0 // tokens per second
	authFailureBurst = 10
)

func authFailureLimit() ratelimit.Limit {
	return ratelimit.Limit{RatePerSecond: authFailureRate, Burst: authFailureBurst}
}

// Gateway is the data-plane HTTP handler. It serves requests from the
// current Snapshot (lock-free) while Run, the single writer, builds and
// publishes new snapshots as configs arrive.
//
// Long-lived state that must survive reloads lives here, outside the
// snapshot: backend health and in-flight counts (Registry), pooled upstream
// connections, the active health Checker, and the rate limiters (so a
// reload neither refills a throttled client's bucket nor ignores a changed
// limit).
type Gateway struct {
	logger *slog.Logger
	now    func() time.Time

	// snap is read on every request and written only by Run. nil until the
	// first config has been applied (not ready, 503).
	snap atomic.Pointer[Snapshot]

	// ready is the startup gate of /readyz (ADR 0007): set once the first
	// applied config's Consul services have all answered or the
	// discovery-ready timeout has passed (at once for configs with nothing
	// to wait for), and never cleared. Written only by the reconcile loop,
	// read concurrently by Ready(); an atomic, so /readyz takes no lock.
	ready atomic.Bool
	// readyTimeout is Options.DiscoveryReadyTimeout, defaulted.
	readyTimeout time.Duration

	// grace is Options.BackendGrace, defaulted. departed maps the ID of
	// every Registry backend that has left the live set to when it left
	// (g.now). Such backends stay in the Registry until their grace runs
	// out, so a Consul flap gives back the SAME *Backend (ejection and
	// in-flight state intact). Touched only by the writer (Run, or apply
	// before Run starts), so it needs no lock.
	grace    time.Duration
	departed map[string]time.Time

	registry   *balancer.Registry
	transports *transportPool
	builder    *builder

	// checker probes the backends of every upstream with a health_check.
	// apply feeds it with Sync; Run owns its goroutine.
	checker *health.Checker

	// The limiters are read on the request path. All are safe for
	// concurrent use and never replaced after New, so no lock is needed.
	// limiter holds authenticated identities, anonLimiter IP-keyed ones
	// (auth: none); they are the same instance only if Options.Limiter was
	// injected. authLimiter is the failed-auth throttle.
	limiter     ratelimit.Limiter
	anonLimiter ratelimit.Limiter
	authLimiter ratelimit.Limiter
	// sweepers are the limiters this Gateway created (not injected ones);
	// Run runs their idle-bucket sweep for as long as it runs.
	sweepers []*ratelimit.TokenBucket

	// running enforces the single-writer rule: a second concurrent Run
	// would race the first one on Store and Prune.
	running atomic.Bool

	// resolver feeds consul upstreams; nil means they have no backends.
	resolver discovery.Resolver

	// metrics and accessLog may each be nil (off). observing is true if
	// either is set: only then does ServeHTTP wrap the ResponseWriter and
	// record the request.
	metrics   *metrics.Metrics
	accessLog *slog.Logger
	observing bool
	// series keeps the per-backend metric series in step with the live
	// snapshot; never nil (a no-op when metrics is nil).
	series *backendSeries
}

// New returns a Gateway with no config. It serves 503 until Run applies the
// first config. New starts no goroutines; Run does.
func New(opts Options) *Gateway {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	g := &Gateway{
		logger:      logger,
		now:         now,
		registry:    balancer.NewRegistry(),
		transports:  newTransportPool(),
		limiter:     opts.Limiter,
		anonLimiter: opts.Limiter,
		authLimiter: opts.AuthLimiter,
		resolver:    opts.Resolver,
		metrics:     opts.Metrics,
		accessLog:   opts.AccessLogger,
		observing:   opts.Metrics != nil || opts.AccessLogger != nil,
		series:      &backendSeries{m: opts.Metrics, now: now},

		readyTimeout: opts.DiscoveryReadyTimeout,
		grace:        opts.BackendGrace,
		departed:     make(map[string]time.Time),
	}
	if g.readyTimeout <= 0 {
		g.readyTimeout = defaultDiscoveryReadyTimeout
	}
	if g.grace <= 0 {
		g.grace = defaultBackendGrace
	}
	// own creates a limiter the Gateway owns: Run sweeps it.
	own := func() *ratelimit.TokenBucket {
		tb := ratelimit.NewTokenBucket(ratelimit.Options{Now: now, MaxKeys: opts.LimiterMaxKeys})
		g.sweepers = append(g.sweepers, tb)
		return tb
	}
	if opts.Limiter == nil {
		g.limiter, g.anonLimiter = own(), own()
	}
	if g.authLimiter == nil {
		g.authLimiter = own()
	}
	series := g.series
	g.checker = health.NewChecker(opts.HealthClient, now, func(b *balancer.Backend, up bool) {
		// Runs on the probe goroutine; logging is concurrency-safe and quick.
		logger.Info("backend active health changed", "backend", b.ID, "up", up)
		// Ignored for backends no longer in the live snapshot, so the
		// reset-to-up of a removed backend cannot resurrect its series.
		series.activeChanged(b)
	})
	g.builder = &builder{
		registry:   g.registry,
		transports: g.transports,
		buffers:    newBufferPool(),
		now:        now,
		getenv:     opts.Getenv,
		readFile:   opts.ReadFile,
		logger:     logger,
		errLog:     slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		series:     g.series,
		discovery:  opts.Resolver != nil,
	}
	return g
}

// Ready reports whether the gateway should receive traffic: a config has
// been applied and, for the first one, every Consul service it references
// has delivered its first endpoint set or Options.DiscoveryReadyTimeout has
// passed (ADR 0007, Startup). Once true it stays true: later config changes
// never take a serving gateway out of its load balancer.
func (g *Gateway) Ready() bool {
	return g.ready.Load() && g.snap.Load() != nil
}

// Version returns the version of the live config, or 0 if none.
func (g *Gateway) Version() int64 {
	if s := g.snap.Load(); s != nil {
		return s.Version
	}
	return 0
}

// Run is the reconcile loop: it watches src and applies every config it
// delivers, and it owns the discovery watches of the live config and
// rebuilds the snapshot whenever one of them reports a new endpoint set
// (same config, no new version). It is the only goroutine that writes the
// snapshot, the Registry, the transport pool and the watch state, and the
// only caller of Checker.Sync and of the source's Acknowledger methods.
//
// Run also owns the Gateway's background goroutines for exactly its own
// lifetime: the health Checker (and through it every probe), the
// idle-bucket sweepers of the limiters the Gateway created itself, the
// in-flight sampler (if metrics are on) and one forwarder per discovery
// watch.
//
// Run returns nil when ctx is canceled or src closes its channel, and the
// Watch error if src cannot start at all (in which case it started
// nothing). Before returning it cancels every watch and waits for every
// forwarder, stops the Checker, sweepers and sampler and waits for them,
// and closes idle upstream connections, so nothing it started outlives it;
// requests still in flight keep working.
func (g *Gateway) Run(ctx context.Context, src config.ConfigSource) error {
	if !g.running.CompareAndSwap(false, true) {
		return errors.New("proxy: Run is already running")
	}
	defer g.running.Store(false)

	updates, err := src.Watch(ctx)
	if err != nil {
		return fmt.Errorf("proxy: watch config: %w", err)
	}

	// Shutdown order, via LIFO defers: the loop below has returned (so no
	// further Sync and no new watch), then cancel every watch and wait for
	// its forwarder, then cancel the workers and wait for every one of them
	// (the Checker waits for its probes), and only then close idle upstream
	// connections. Sync never races this: it is called only from this
	// goroutine, and even a late Sync is a non-blocking store.
	bgCtx, stopBackground := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer g.transports.closeIdle()
	defer wg.Wait()
	defer stopBackground()

	// spawn runs fn on a goroutine tracked by wg; fn must return once
	// bgCtx is done.
	spawn := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}
	spawn(func() {
		// Only fails if already running, which g.running rules out.
		if err := g.checker.Run(bgCtx); err != nil {
			g.logger.Error("health checker stopped", "err", err)
		}
	})
	for _, tb := range g.sweepers {
		spawn(func() { _ = tb.Run(bgCtx) }) // returns nil once bgCtx is done
	}
	if g.metrics != nil {
		spawn(func() { g.series.runSampler(bgCtx) })
	}

	rc := newReconciler(bgCtx, g, src)
	// Cancels every watch, waits for every forwarder, stops the ready timer.
	defer rc.stop()

	for {
		select {
		case <-ctx.Done():
			// No need to drain updates: the source owns the channel and
			// closes it itself; its sends never block (latest wins).
			return nil
		case cfg, ok := <-updates:
			if !ok {
				return nil
			}
			rc.applyConfig(cfg)
		case u := <-rc.updates:
			rc.applyEndpoints(u)
		case <-rc.readyC: // nil (never fires) unless the startup gate is waiting
			rc.readyTimedOut()
		}
	}
}

// apply builds a snapshot for cfg with no discovered endpoints and swaps it
// in, outside of Run (tests use it to set up a Gateway synchronously). On a
// build error the current snapshot stays live (last known good). There is
// no discovery to wait for, so a successful apply makes the gateway ready.
func (g *Gateway) apply(cfg *config.Config) error {
	if _, err := g.rebuild(cfg, buildInput{prev: g.snap.Load()}, "config", true); err != nil {
		g.logger.Error("config rejected; keeping current config",
			"version", cfgVersion(cfg), "current_version", g.Version(), "err", err)
		return err
	}
	return nil
}

func cfgVersion(cfg *config.Config) int64 {
	if cfg == nil {
		return 0
	}
	return cfg.Version
}

// rebuild builds a snapshot from cfg and in and, if that succeeds,
// installs it; markReady also opens the readiness gate at the swap.
// trigger ("config" or "discovery") is for the log.
func (g *Gateway) rebuild(cfg *config.Config, in buildInput, trigger string, markReady bool) (*Snapshot, error) {
	start := time.Now()
	// BEFORE the build: a backend that comes back after its grace must get
	// a fresh *Backend from Registry.Get, not the stale one.
	expired := g.expireDeparted()
	s, err := g.builder.build(cfg, in)
	if err != nil {
		return nil, err
	}
	removed := g.install(s, markReady)
	g.logger.Info("config applied",
		"trigger", trigger,
		"version", s.Version,
		"routes", len(cfg.Routes),
		"upstreams", len(cfg.Upstreams),
		"backends", len(s.backendIDs),
		"backends_removed", len(removed)+expired,
		"backends_in_grace", len(g.departed),
		"health_targets", len(s.healthTargets),
		"build_duration", time.Since(start).String())
	return s, nil
}

// install swaps s in and brings the long-lived state in line with it. It
// returns the backends the Registry dropped. markReady sets the readiness
// flag right before the swap, so Ready() and Version() change together.
func (g *Gateway) install(s *Snapshot, markReady bool) (removed []*balancer.Backend) {
	prev := g.snap.Load()
	keep := g.trackDepartures(prev, s)

	if markReady {
		g.ready.Store(true)
	}
	// The swap. From here on new requests see s; requests already running
	// keep the snapshot they loaded and finish on it, with its routes, auth
	// rules and limits.
	g.snap.Store(s)

	// Prune only AFTER the Store, so no newly arriving request can pick a
	// Backend that is about to be forgotten. Backends that just left the
	// live set are in keep (grace period), so this normally drops nothing;
	// expired ones were dropped before the build (expireDeparted). Requests
	// still in flight on an OLD snapshot may hold a dropped Backend; that is
	// safe because Balancer.Done only decrements a counter on the *Backend
	// they already hold, never looks it up by ID.
	removed = g.registry.Prune(keep)
	g.transports.retain(s.transportKeys)

	// Forget the series of every backend that left the LIVE set (kept in
	// the Registry for the grace or not) and switch the metrics' live set
	// BEFORE Sync: Sync stops the probes of those backends, whose last act
	// is a reset-to-up callback, and that callback must already find them
	// gone or it would recreate the series just deleted (review note P2-6).
	g.series.swap(s)

	// Always pass the full current set: the Checker diffs by *Backend
	// pointer, so backends that left (kept for the grace or not), re-added
	// ones and upstreams that lost their health_check (reset to up) are all
	// handled from this one call. Sync never blocks, and is fine before the
	// Checker's Run has started.
	g.checker.Sync(s.healthTargets)

	g.forgetRemovedRoutes(prev, s)
	g.metrics.SetConfigVersion(s.Version)
	return removed
}

// trackDepartures updates the grace bookkeeping for the swap prev -> s and
// returns the IDs the Registry must keep: s's backends plus every departed
// backend still within its grace. Backends of prev that s no longer has
// start their grace now; backends of s stop being departed.
func (g *Gateway) trackDepartures(prev, s *Snapshot) map[string]struct{} {
	now := g.now()
	if prev != nil {
		for id := range prev.backendIDs {
			if _, live := s.backendIDs[id]; !live {
				if _, ok := g.departed[id]; !ok {
					g.departed[id] = now
				}
			}
		}
	}
	keep := make(map[string]struct{}, len(s.backendIDs)+len(g.departed))
	for id := range s.backendIDs {
		keep[id] = struct{}{}
		delete(g.departed, id) // back in the live set: its grace no longer runs
	}
	for id := range g.departed {
		keep[id] = struct{}{}
	}
	return keep
}

// expireDeparted drops from the Registry every departed backend whose grace
// has run out. It runs at the start of every rebuild, so departed backends
// are bounded: each is gone at the first rebuild after its grace, and any
// one is held at most until then (a Backend is a few atomics, and its
// series and probe already went when it left the live set, so a backend
// held longer because no rebuild comes along costs next to nothing).
// It returns how many backends it dropped.
func (g *Gateway) expireDeparted() int {
	if len(g.departed) == 0 {
		return 0
	}
	now := g.now()
	expired := false
	for id, left := range g.departed {
		if now.Sub(left) >= g.grace {
			delete(g.departed, id)
			expired = true
		}
	}
	if !expired {
		return 0
	}
	keep := make(map[string]struct{}, len(g.departed))
	if live := g.snap.Load(); live != nil {
		for id := range live.backendIDs {
			keep[id] = struct{}{}
		}
	}
	for id := range g.departed {
		keep[id] = struct{}{}
	}
	// Nothing else to clean up: their series were deleted and their probes
	// stopped when they left the live set.
	return len(g.registry.Prune(keep))
}

// forgetRemovedRoutes deletes the request, latency, rate-limit and
// auth-failure series of every route name prev had and s does not (a
// removed or renamed route), so they do not linger on /metrics forever.
// A request still finishing on prev for such a route may recreate a series
// after this; that is bounded by the routes' request timeout and by the
// number of removed names, and is accepted to keep the hot path free of a
// "is this route still live" check.
func (g *Gateway) forgetRemovedRoutes(prev, s *Snapshot) {
	if g.metrics == nil || prev == nil || prev.Config == s.Config {
		return // an endpoint-triggered rebuild has the same routes
	}
	names := make(map[string]struct{}, len(s.Config.Routes))
	for _, r := range s.Config.Routes {
		names[r.Name] = struct{}{}
	}
	for _, r := range prev.Config.Routes {
		if _, ok := names[r.Name]; !ok {
			g.metrics.ForgetRoute(r.Name)
		}
	}
}
