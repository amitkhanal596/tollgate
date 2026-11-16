package proxy

import (
	"context"
	"sync"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/metrics"
)

// inFlightSampleInterval is how often Run copies each live backend's
// in-flight count into tollgate_backend_inflight and its "in rotation"
// verdict into tollgate_backend_up. Sampling instead of updating the gauges
// on every request start and finish keeps the request path free of a
// labelled-gauge lookup (a label hash plus a map lookup, twice per request);
// a gauge scraped every 15 s or so loses nothing by being refreshed twice a
// second. The sampler is also what returns backend_up to 1 when a passive
// ejection expires: ejections expire lazily (no timer, no event).
const inFlightSampleInterval = 500 * time.Millisecond

// backendSeries keeps the tollgate_backend_* series (up, inflight,
// ejections_total) in step with the LIVE snapshot (architecture.md section
// 9, section 11 row 19, review note P2-6).
//
// The rule: once a backend has left the live set and its series have been
// forgotten, nothing may create them again until it rejoins the live set.
// (It may stay in the Registry for a grace period, but it is out of
// rotation and has no series.) Three writers could otherwise do so: the
// Checker's reset-to-up callback for a backend whose probe Sync just
// stopped, the sampler, and a passive ejection by a request still running
// on an old snapshot. All three therefore write only for a *Backend POINTER
// that is in live, and swap (which replaces live and forgets the departed
// backends) runs under the same mutex. Without the lock a writer could
// check live, lose the CPU to swap, and then recreate the series it just
// saw deleted.
//
// tollgate_backend_up means "in rotation": Backend.Available(now), i.e. the
// active verdict AND not passively ejected.
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
	m   *metrics.Metrics // nil: every method is a no-op
	now func() time.Time // the gateway's clock (ejection deadlines use it)

	mu   sync.Mutex
	live map[*balancer.Backend]string // backend -> upstream name
}

// swap makes s's backends the live set. Every backend that was live and is
// not any more loses its series, whether the Registry still keeps it for
// its grace period or not; backends that are new in s (including one coming
// back within its grace) get their tollgate_backend_up series. It must run
// after the Store and BEFORE Checker.Sync, so that the reset-to-up callbacks
// Sync triggers for departed backends already find them not live.
func (bs *backendSeries) swap(s *Snapshot) {
	if bs.m == nil {
		return
	}
	live := make(map[*balancer.Backend]string, len(s.backendIDs))
	for _, u := range s.upstreams {
		for _, b := range u.backends {
			live[b] = u.name
		}
	}
	now := bs.now()

	bs.mu.Lock()
	defer bs.mu.Unlock()
	for b, upstream := range bs.live {
		if _, still := live[b]; !still {
			bs.m.ForgetBackend(upstream, b.ID)
		}
	}
	for b, upstream := range live {
		if _, seen := bs.live[b]; !seen {
			bs.m.SetBackendUp(upstream, b.ID, b.Available(now))
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
		bs.m.SetBackendUp(upstream, b.ID, b.Available(bs.now()))
	}
}

// ejected counts one passive ejection of b and takes it out of rotation on
// the gauge at once (the sampler would only notice within a tick). It runs
// on a request goroutine, but only for the one request whose failure
// ejected b.
func (bs *backendSeries) ejected(b *balancer.Backend) {
	if bs.m == nil {
		return
	}
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if upstream, ok := bs.live[b]; ok {
		bs.m.BackendEjected(upstream, b.ID)
		bs.m.SetBackendUp(upstream, b.ID, b.Available(bs.now()))
	}
}

// sample copies every live backend's in-flight count and in-rotation
// verdict into its gauges.
func (bs *backendSeries) sample() {
	now := bs.now()
	bs.mu.Lock()
	defer bs.mu.Unlock()
	for b, upstream := range bs.live {
		bs.m.SetBackendInFlight(upstream, b.ID, b.InFlight())
		bs.m.SetBackendUp(upstream, b.ID, b.Available(now))
	}
}

// runSampler samples the gauges until ctx is done. Gateway.Run starts
// it (only when metrics are enabled) and waits for it.
func (bs *backendSeries) runSampler(ctx context.Context) {
	t := time.NewTicker(inFlightSampleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			bs.sample()
		}
	}
}
