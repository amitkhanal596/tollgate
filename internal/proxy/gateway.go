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
	"github.com/amitkhanal596/tollgate/internal/health"
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
}

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
	g.checker = health.NewChecker(opts.HealthClient, now, func(b *balancer.Backend, up bool) {
		// Runs on the probe goroutine; logging is concurrency-safe and quick.
		logger.Info("backend active health changed", "backend", b.ID, "up", up)
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
	}
	return g
}

// Ready reports whether a config has been applied, i.e. whether the
// gateway can route traffic.
func (g *Gateway) Ready() bool {
	return g.snap.Load() != nil
}

// Version returns the version of the live config, or 0 if none.
func (g *Gateway) Version() int64 {
	if s := g.snap.Load(); s != nil {
		return s.Version
	}
	return 0
}

// Run is the reconcile loop: it watches src and applies every config it
// delivers. It is the only goroutine that writes the snapshot, the Registry
// and the transport pool, and the only caller of Checker.Sync.
//
// Run also owns the Gateway's background goroutines for exactly its own
// lifetime: the health Checker (and through it every probe) and the
// idle-bucket sweepers of the limiters the Gateway created itself.
//
// Run returns nil when ctx is canceled or src closes its channel, and the
// Watch error if src cannot start at all (in which case it started
// nothing). Before returning it stops the Checker and sweepers, waits for
// all of them to exit, and closes idle upstream connections, so nothing it
// started outlives it; requests still in flight keep working.
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
	// further Sync), then cancel the workers, wait for every one of them
	// (the Checker waits for its probes), and only then close idle upstream
	// connections. Sync never races this: it is called only from this
	// goroutine, and even a late Sync is a non-blocking store.
	bgCtx, stopBackground := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer g.transports.closeIdle()
	defer wg.Wait()
	defer stopBackground()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Only fails if already running, which g.running rules out.
		if err := g.checker.Run(bgCtx); err != nil {
			g.logger.Error("health checker stopped", "err", err)
		}
	}()
	for _, tb := range g.sweepers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tb.Run(bgCtx) // returns nil once bgCtx is done
		}()
	}

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
			g.apply(cfg)
		}
	}
}

// apply builds a snapshot for cfg and swaps it in. On a build error the
// current snapshot stays live (last known good).
func (g *Gateway) apply(cfg *config.Config) {
	start := time.Now()
	s, err := g.builder.build(cfg)
	if err != nil {
		version := int64(0)
		if cfg != nil {
			version = cfg.Version
		}
		g.logger.Error("config rejected; keeping current config",
			"version", version, "current_version", g.Version(), "err", err)
		return
	}

	// The swap. From here on new requests see s; requests already running
	// keep the snapshot they loaded and finish on it, with its routes, auth
	// rules and limits.
	g.snap.Store(s)

	// Prune only AFTER the Store, so no newly arriving request can pick a
	// Backend that is about to be forgotten. Requests still in flight on
	// the OLD snapshot may hold a Backend that this drops; that is safe
	// because Balancer.Done only decrements a counter on the *Backend they
	// already hold, never looks it up by ID. If the same backend is added
	// back by a later config, Registry.Get creates a fresh Backend with
	// reset health and in-flight state, while the stragglers decrement the
	// old object, so the new count never goes negative.
	removed := g.registry.Prune(s.backendIDs)
	g.transports.retain(s.transportKeys)

	// Always pass the full current set: the Checker diffs by *Backend
	// pointer, so pruned or re-added backends and upstreams that lost their
	// health_check (reset to up) are all handled from this one call. Sync
	// never blocks, and is fine before the Checker's Run has started.
	g.checker.Sync(s.healthTargets)

	g.logger.Info("config applied",
		"version", s.Version,
		"routes", len(cfg.Routes),
		"upstreams", len(cfg.Upstreams),
		"backends", len(s.backendIDs),
		"backends_removed", len(removed),
		"health_targets", len(s.healthTargets),
		"build_duration", time.Since(start).String())
}
