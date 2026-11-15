package proxy

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/metrics"
)

// inFlightSampleInterval is how often Run copies each live backend's
// in-flight count into tollgate_backend_inflight. Sampling instead of
// updating the gauge on every request start and finish keeps the request
// path free of a labelled-gauge lookup (a label hash plus a map lookup, twice
// per request); a gauge scraped every 15 s or so loses nothing by being
// refreshed twice a second.
const inFlightSampleInterval = 500 * time.Millisecond

// backendSeries keeps the tollgate_backend_* series (up, inflight,
// ejections_total) in step with the LIVE snapshot (architecture.md section
// 9, section 11 row 19, review note P2-6).
//
// The rule: once a backend has been pruned and its series forgotten, nothing
// may create them again. Three writers could otherwise do so after the
// prune: the Checker's reset-to-up callback for a backend whose probe Sync
// just stopped, the in-flight sampler, and a passive ejection by a request
// still running on an old snapshot. All three therefore write only for a
// *Backend POINTER that is in live, and swap (which replaces live and
// forgets the pruned backends) runs under the same mutex. Without the lock a
// writer could check live, lose the CPU to swap, and then recreate the
// series it just saw deleted.
//
// Why a mutex and not an atomic set: the check and the gauge write must be
// one step relative to swap, which an atomic.Pointer cannot give. It is
// never taken on the normal request path: only on a passive ejection (once
// per cooloff per backend), an active-health transition, a swap, and the
// sampler tick.
//
// Pointer identity, not ID: a backend pruned and later re-added under the
// same ID is a NEW pointer (fresh state) from the Registry. The old
// pointer's late callbacks must not touch the new pointer's series.
type backendSeries struct {
	m *metrics.Metrics // nil: every method is a no-op

	mu   sync.Mutex
	live map[*balancer.Backend]string // backend -> upstream name
}

// swap makes s's backends the live set. removed are the backends that
// Registry.Prune just dropped: their series are deleted. Backends that are
// new in s get their tollgate_backend_up series. It must run after the
// Store and the Prune and BEFORE Checker.Sync, so that the reset-to-up
// callbacks Sync triggers for removed backends already find them not live.
func (bs *backendSeries) swap(s *Snapshot, removed []*balancer.Backend) {
	if bs.m == nil {
		return
	}
	live := make(map[*balancer.Backend]string, len(s.backendIDs))
	for _, u := range s.upstreams {
		for _, b := range u.backends {
			live[b] = u.name
		}
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()
	for _, b := range removed {
		upstream, ok := bs.live[b]
		if !ok {
			// Not expected (the Registry holds exactly the previous live
			// set), but the ID always starts with the upstream name.
			upstream, _, _ = strings.Cut(b.ID, "|")
		}
		bs.m.ForgetBackend(upstream, b.ID)
	}
	for b, upstream := range live {
		if _, seen := bs.live[b]; !seen {
			bs.m.SetBackendUp(upstream, b.ID, b.ActiveHealthy())
		}
	}
	bs.live = live
}

// activeChanged is the Checker's onChange: it runs on a probe goroutine.
// It reads the verdict from the Backend rather than trusting the callback's
// argument, so the gauge always ends at the latest verdict whatever order
// concurrent writers take the lock in.
func (bs *backendSeries) activeChanged(b *balancer.Backend) {
	if bs.m == nil {
		return
	}
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if upstream, ok := bs.live[b]; ok {
		bs.m.SetBackendUp(upstream, b.ID, b.ActiveHealthy())
	}
}

// ejected counts one passive ejection of b. It runs on a request goroutine,
// but only for the one request whose failure ejected b.
func (bs *backendSeries) ejected(b *balancer.Backend) {
	if bs.m == nil {
		return
	}
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if upstream, ok := bs.live[b]; ok {
		bs.m.BackendEjected(upstream, b.ID)
	}
}

// sampleInFlight copies every live backend's in-flight count into its gauge.
func (bs *backendSeries) sampleInFlight() {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	for b, upstream := range bs.live {
		bs.m.SetBackendInFlight(upstream, b.ID, b.InFlight())
	}
}

// runSampler samples in-flight counts until ctx is done. Gateway.Run starts
// it (only when metrics are enabled) and waits for it.
func (bs *backendSeries) runSampler(ctx context.Context) {
	t := time.NewTicker(inFlightSampleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			bs.sampleInFlight()
		}
	}
}
