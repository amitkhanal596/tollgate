// Package proxy is the data plane: it composes routing, load balancing and
// reverse proxying behind an atomically swapped, immutable Snapshot
// (docs/architecture.md sections 4-5, ADR 0002).
package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
)

// Options configures a Gateway. The zero value is usable.
type Options struct {
	// Logger receives structured logs. Nil discards them.
	Logger *slog.Logger
	// Now is the clock used for passive-ejection expiry. Nil uses time.Now.
	Now func() time.Time
}

// Gateway is the data-plane HTTP handler. It serves requests from the
// current Snapshot (lock-free) while Run, the single writer, builds and
// publishes new snapshots as configs arrive.
//
// Long-lived state that must survive reloads (backend health and in-flight
// counts, pooled upstream connections) lives here, outside the snapshot.
type Gateway struct {
	logger *slog.Logger

	// snap is read on every request and written only by Run. nil until the
	// first config has been applied (not ready, 503).
	snap atomic.Pointer[Snapshot]

	registry   *balancer.Registry
	transports *transportPool
	builder    *builder

	// running enforces the single-writer rule: a second concurrent Run
	// would race the first one on Store and Prune.
	running atomic.Bool
}

// New returns a Gateway with no config. It serves 503 until Run applies the
// first config.
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
		logger:     logger,
		registry:   balancer.NewRegistry(),
		transports: newTransportPool(),
	}
	g.builder = &builder{
		registry:   g.registry,
		transports: g.transports,
		buffers:    newBufferPool(),
		now:        now,
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
// and the transport pool.
//
// Run returns nil when ctx is canceled or src closes its channel, and the
// Watch error if src cannot start at all. Before returning it closes idle
// upstream connections so their goroutines do not outlive it; requests
// still in flight keep working.
func (g *Gateway) Run(ctx context.Context, src config.ConfigSource) error {
	if !g.running.CompareAndSwap(false, true) {
		return errors.New("proxy: Run is already running")
	}
	defer g.running.Store(false)

	updates, err := src.Watch(ctx)
	if err != nil {
		return fmt.Errorf("proxy: watch config: %w", err)
	}
	defer g.transports.closeIdle()

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
	// keep the snapshot they loaded and finish on it.
	g.snap.Store(s)

	// Prune only AFTER the Store, so no newly arriving request can pick a
	// Backend that is about to be forgotten. Requests still in flight on
	// the OLD snapshot may hold a Backend that this drops; that is safe
	// because Balancer.Done only decrements a counter on the *Backend they
	// already hold, never looks it up by ID. If the same backend is added
	// back by a later config, Registry.Get creates a fresh Backend with
	// reset health and in-flight state, while the stragglers decrement the
	// old object, so the new count never goes negative.
	g.registry.Prune(s.backendIDs)
	g.transports.retain(s.transportKeys)

	g.logger.Info("config applied",
		"version", s.Version,
		"routes", len(cfg.Routes),
		"upstreams", len(cfg.Upstreams),
		"backends", len(s.backendIDs),
		"build_duration", time.Since(start).String())
}
