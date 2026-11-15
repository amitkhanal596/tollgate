package proxy

import (
	"context"
	"sync"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/discovery"
)

// reconciler is the state of one Gateway.Run: the config of the live
// snapshot and the discovery watches it references (ADR 0007,
// architecture.md section 5). Every field is owned by the Run goroutine and
// touched by nobody else, so there is no lock; the forwarder goroutines
// only ever talk to it through the updates channel.
type reconciler struct {
	g        *Gateway
	ctx      context.Context     // parent of every watch ctx
	ack      config.Acknowledger // nil if the source does not implement it
	resolver discovery.Resolver  // nil: consul upstreams get no backends
	cfg      *config.Config      // config of the live snapshot; nil until the first apply
	watches  map[discovery.Query]*watch
	nextGen  uint64
	// updates carries every forwarder's endpoint sets to the loop.
	// Unbuffered: a forwarder hands over a set only when the loop takes it,
	// and every send also selects on the watch ctx, so a cancelled watch can
	// never block anyone.
	updates chan endpointUpdate
	fwd     sync.WaitGroup // one per forwarder goroutine
}

// watch is one running Resolver.Watch, shared by every upstream that
// references its (service, tag), whatever their schemes.
type watch struct {
	// gen is assigned by the loop when the watch starts. An update carrying
	// another generation comes from a watch that was cancelled (and maybe
	// replaced) since, and is dropped (section 11 row 4).
	gen    uint64
	cancel context.CancelFunc
	// eps is the latest set; nil until the first one arrives, which builds
	// the upstream with no backends (503) while the gateway is ready.
	eps []discovery.Endpoint
}

// endpointUpdate is one endpoint set, stamped by its forwarder.
type endpointUpdate struct {
	key discovery.Query
	gen uint64
	eps []discovery.Endpoint
}

func newReconciler(ctx context.Context, g *Gateway, src config.ConfigSource) *reconciler {
	ack, _ := src.(config.Acknowledger) // nil if not implemented (FileSource)
	return &reconciler{
		g:        g,
		ctx:      ctx,
		ack:      ack,
		resolver: g.resolver,
		watches:  make(map[discovery.Query]*watch),
		updates:  make(chan endpointUpdate),
	}
}

// endpoints is the build's view of the latest endpoint sets.
func (rc *reconciler) endpoints(q discovery.Query) []discovery.Endpoint {
	if w := rc.watches[q]; w != nil {
		return w.eps
	}
	return nil
}

// applyConfig handles a config delivered by the source: build, swap,
// acknowledge, then start or stop watches to match it. Acknowledgements are
// made here and only here, so a rebuild triggered by discovery is never
// acknowledged, and each delivered config gets exactly one Applied or
// Rejected, on this goroutine, in delivery order (config/ack.go).
func (rc *reconciler) applyConfig(cfg *config.Config) {
	var version int64
	if cfg != nil {
		version = cfg.Version
	}
	s, err := rc.g.rebuild(cfg, rc.endpoints, "config")
	if err != nil {
		rc.g.logger.Error("config rejected; keeping current config",
			"version", version, "current_version", rc.g.Version(), "err", err)
		rc.g.metrics.ConfigApplyFailed()
		if rc.ack != nil {
			rc.ack.Rejected(version, err)
		}
		// The watches still match the live (old) config: leave them alone.
		return
	}
	rc.cfg = cfg
	// After the swap: Version() already reports the new version, which is
	// what the source may now persist and report as running.
	if rc.ack != nil {
		rc.ack.Applied(s.Version)
	}
	// Diffed against the config that is now LIVE. Doing it before the build
	// would cancel watches the still-serving old snapshot needs whenever the
	// new config is then rejected. Services this config adds have no set
	// yet; their first update triggers a rebuild.
	rc.syncWatches(cfg)
}

// applyEndpoints handles one forwarded endpoint set: if it is from the
// current watch for its key, remember it and rebuild with the same config.
func (rc *reconciler) applyEndpoints(u endpointUpdate) {
	w := rc.watches[u.key]
	if w == nil || w.gen != u.gen {
		// Sent by a watch cancelled since (the forwarder's send can win the
		// race against its ctx in a select): never use it.
		rc.g.logger.Debug("dropping endpoint update from a stale watch",
			"service", u.key.Service, "tag", u.key.Tag, "generation", u.gen)
		return
	}
	w.eps = u.eps
	if rc.cfg == nil {
		return // unreachable: watches exist only for a live config
	}
	// Kept even if this rebuild fails, so the next successful build (the
	// next config) uses the newest set.
	if _, err := rc.g.rebuild(rc.cfg, rc.endpoints, "discovery"); err != nil {
		// Not a config: no version, no acknowledgement, no apply-failure
		// count. The old snapshot (with the previous endpoint set) keeps
		// serving; this can only happen if something the build reads from
		// the environment (a JWT secret, a key file) has gone away since
		// the config was applied.
		rc.g.logger.Error("rebuild after an endpoint change failed; keeping the current snapshot",
			"service", u.key.Service, "tag", u.key.Tag, "endpoints", len(u.eps),
			"version", rc.cfg.Version, "err", err)
	}
}

// syncWatches starts a watch for every (service, tag) cfg references that is
// not watched yet and cancels the watches it no longer references. A
// watch that stays referenced keeps running and keeps its latest set.
func (rc *reconciler) syncWatches(cfg *config.Config) {
	want := make(map[discovery.Query]struct{})
	for _, u := range cfg.Upstreams {
		if u.Consul != nil {
			want[consulQuery(u.Consul)] = struct{}{}
		}
	}
	for q, w := range rc.watches {
		if _, ok := want[q]; !ok {
			// Its forwarder exits on the cancel; any update it already
			// handed over is dropped by the generation check.
			w.cancel()
			delete(rc.watches, q)
		}
	}
	if rc.resolver == nil {
		return // builder already warned once per consul upstream
	}
	// In config order, so watches start in a deterministic order.
	for _, u := range cfg.Upstreams {
		if u.Consul == nil {
			continue
		}
		if q := consulQuery(u.Consul); rc.watches[q] == nil {
			rc.startWatch(q)
		}
	}
}

// startWatch starts a Resolver watch and its forwarder.
func (rc *reconciler) startWatch(q discovery.Query) {
	ctx, cancel := context.WithCancel(rc.ctx)
	ch, err := rc.resolver.Watch(ctx, q)
	if err != nil {
		// Only invalid input fails (Resolver point 2), which Validate
		// already rules out; the upstream simply stays without backends.
		cancel()
		rc.g.logger.Error("cannot watch service; its upstreams have no backends",
			"service", q.Service, "tag", q.Tag, "err", err)
		return
	}
	rc.nextGen++
	gen := rc.nextGen
	rc.watches[q] = &watch{gen: gen, cancel: cancel}
	rc.fwd.Add(1)
	go rc.forward(ctx, q, gen, ch)
}

// forward relays one watch's endpoint sets into the loop's channel. It
// exits when its watch ctx is cancelled (unreferenced, or Run returning) or
// when the resolver closes the channel. Some resolvers never close it, so
// ctx is checked on its own and not only through the channel closing.
func (rc *reconciler) forward(ctx context.Context, q discovery.Query, gen uint64, ch <-chan []discovery.Endpoint) {
	defer rc.fwd.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case eps, ok := <-ch:
			if !ok {
				return
			}
			// Never a bare send: the loop may have stopped receiving
			// (shutdown) or cancelled this watch, and a forwarder blocked
			// here would then hang Run's final Wait forever.
			select {
			case rc.updates <- endpointUpdate{key: q, gen: gen, eps: eps}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// stopWatches cancels every watch and waits for every forwarder. Run defers
// it, so it returns only after nothing can send on updates any more.
func (rc *reconciler) stopWatches() {
	for q, w := range rc.watches {
		w.cancel()
		delete(rc.watches, q)
	}
	rc.fwd.Wait()
}
