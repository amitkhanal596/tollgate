package proxy

import (
	"context"
	"sync"
	"time"

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

	// The startup readiness gate (ADR 0007, fix round 5 D1). pending holds
	// the queries of the FIRST applied config that have not answered yet;
	// readyTimer/readyC fire Options.DiscoveryReadyTimeout after that config
	// was applied. All nil when the gate is not waiting (readyC nil makes
	// Run's select ignore it). Owned by the loop like everything else here;
	// stop() stops the timer.
	pending    map[discovery.Query]struct{}
	readyTimer *time.Timer
	readyC     <-chan time.Time
}

// watch is one running Resolver.Watch, shared by every upstream that
// references its (service, tag), whatever their schemes.
type watch struct {
	// gen is assigned by the loop when the watch starts. An update carrying
	// another generation comes from a watch that was cancelled (and maybe
	// replaced) since, and is dropped (section 11 row 4).
	gen    uint64
	cancel context.CancelFunc
	// answered is false until the first set arrives; until then the
	// upstreams using this watch keep their previous backends (if any).
	// eps is the latest set, with invalid addresses already dropped.
	answered bool
	eps      []discovery.Endpoint
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
func (rc *reconciler) endpoints(q discovery.Query) ([]discovery.Endpoint, bool) {
	if w := rc.watches[q]; w != nil && w.answered {
		return w.eps, true
	}
	return nil, false
}

// input is the build input for the next rebuild. The live snapshot is the
// one built from rc.cfg (rejected configs never replace it).
func (rc *reconciler) input(reuseAuth bool) buildInput {
	return buildInput{endpoints: rc.endpoints, prev: rc.g.snap.Load(), reuseAuth: reuseAuth}
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
	// Only the first config applied in the gateway's life gates readiness;
	// once ready, later configs never take it back (ADR 0007, Startup).
	first := rc.cfg == nil && !rc.g.ready.Load()
	// With nothing that could answer, the gateway is ready at the swap.
	wait := first && rc.resolver != nil && referencesConsul(cfg)
	s, err := rc.g.rebuild(cfg, rc.input(false), "config", first && !wait)
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
	if wait {
		rc.armReadyGate()
	}
}

// referencesConsul reports whether cfg has an upstream fed by discovery.
func referencesConsul(cfg *config.Config) bool {
	for _, u := range cfg.Upstreams {
		if u.Consul != nil {
			return true
		}
	}
	return false
}

// armReadyGate starts waiting for the first answer of every watch the
// first config started, bounded by the discovery-ready timeout. The timer
// is owned by the loop: Run's select receives from readyC, and stop()
// stops it, so no goroutine or timer outlives Run.
func (rc *reconciler) armReadyGate() {
	rc.pending = make(map[discovery.Query]struct{}, len(rc.watches))
	for q, w := range rc.watches {
		if !w.answered {
			rc.pending[q] = struct{}{}
		}
	}
	if len(rc.pending) == 0 {
		rc.openReadyGate() // every Watch call failed: nothing can answer
		return
	}
	rc.readyTimer = time.NewTimer(rc.g.readyTimeout)
	rc.readyC = rc.readyTimer.C
}

// settled removes q from the queries the gate waits for (it answered, or
// no longer matters) and opens the gate when none is left.
func (rc *reconciler) settled(q discovery.Query) {
	if rc.pending == nil {
		return
	}
	delete(rc.pending, q)
	if len(rc.pending) == 0 {
		rc.g.logger.Info("every discovered service has answered; gateway is ready")
		rc.openReadyGate()
	}
}

// readyTimedOut opens the gate although some services have not answered:
// a Consul agent that is down at startup must not keep the gateway out of
// rotation forever (its static routes, at least, work).
func (rc *reconciler) readyTimedOut() {
	services := make([]string, 0, len(rc.pending))
	for q := range rc.pending {
		services = append(services, q.Service)
	}
	rc.g.logger.Warn("discovery ready timeout passed with services that have not answered; becoming ready anyway",
		"timeout", rc.g.readyTimeout.String(), "services", services)
	rc.openReadyGate()
}

func (rc *reconciler) openReadyGate() {
	rc.g.ready.Store(true)
	rc.pending = nil
	if rc.readyTimer != nil {
		rc.readyTimer.Stop()
		rc.readyTimer, rc.readyC = nil, nil
	}
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
	firstAnswer := !w.answered
	// Kept even if this rebuild fails, so the next successful build (the
	// next config) uses the newest set.
	w.eps, w.answered = rc.validEndpoints(u.key, u.eps), true
	if rc.cfg == nil {
		return // unreachable: watches exist only for a live config
	}
	if firstAnswer && len(w.eps) == 0 {
		rc.warnEmptyFirstAnswer(u.key)
	}
	// Same config: reuse the live snapshot's authenticators (D5).
	if _, err := rc.g.rebuild(rc.cfg, rc.input(true), "discovery", false); err != nil {
		// Not a config: no version, no acknowledgement, no apply-failure
		// count. The old snapshot (with the previous endpoint set) keeps
		// serving. Not expected any more: this config already built once,
		// and the authenticators (the only part that reads files and env
		// vars) are reused rather than rebuilt (D5). Kept as a defensive
		// path.
		rc.g.logger.Error("rebuild after an endpoint change failed; keeping the current snapshot",
			"service", u.key.Service, "tag", u.key.Tag, "endpoints", len(u.eps),
			"version", rc.cfg.Version, "err", err)
	}
	// After the rebuild, so /readyz turns ready only once the snapshot
	// serves this set.
	rc.settled(u.key)
}

// warnEmptyFirstAnswer logs one WARN per upstream fed by q when q's first
// answer has no (valid) instances: usually a typo in the service name or
// tag, which would otherwise only show up as a silent 503.
func (rc *reconciler) warnEmptyFirstAnswer(q discovery.Query) {
	for _, u := range rc.cfg.Upstreams {
		if u.Consul != nil && consulQuery(u.Consul) == q {
			rc.g.logger.Warn("consul reports no passing instances for this upstream's service; it has no backends and answers 503 (check the service name and tag)",
				"upstream", u.Name, "service", q.Service, "tag", q.Tag)
		}
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
			rc.settled(q) // an unreferenced service can no longer hold readiness back
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

// stop cancels every watch, waits for every forwarder and stops the ready
// timer. Run defers it, so it returns only after nothing can send on updates
// any more.
func (rc *reconciler) stop() {
	if rc.readyTimer != nil {
		rc.readyTimer.Stop()
		rc.readyTimer, rc.readyC = nil, nil
	}
	for q, w := range rc.watches {
		w.cancel()
		delete(rc.watches, q)
	}
	rc.fwd.Wait()
}
