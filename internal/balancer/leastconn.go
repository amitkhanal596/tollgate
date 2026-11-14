package balancer

import (
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// leastConn sends each request to the available backend with the fewest
// in-flight requests (ADR 0003). The in-flight counts live on the shared
// *Backend, so the view is accurate across config swaps and across
// balancers that share backends.
type leastConn struct {
	backends []*Backend // immutable after construction
	now      func() time.Time
	// cursor is the rotating scan start. It only breaks ties: without it
	// every tie (e.g. all backends idle) would go to backends[0].
	cursor atomic.Uint64
}

// NewLeastConn returns a least-connections Balancer over backends. The slice
// is copied, so the caller may reuse its own slice; the *Backend values are
// shared on purpose (their state is what survives config swaps). now is the
// clock used to evaluate passive-ejection expiry.
func NewLeastConn(backends []*Backend, now func() time.Time) Balancer {
	lc := &leastConn{
		backends: append([]*Backend(nil), backends...),
		now:      now,
	}
	if n := len(lc.backends); n > 0 {
		// Random start so a fleet of gateways reloading together does not
		// all break their first ties towards backends[0] (ADR 0003).
		lc.cursor.Store(rand.Uint64N(uint64(n)))
	}
	return lc
}

// Next implements Balancer.
//
// Concurrency: the scan reads each in-flight counter atomically but without
// a lock, then increments the winner. Two concurrent calls can read the
// same minimum and both pick the same backend. We accept that rather than
// using a CAS loop (ADR 0003) because:
//   - both increments are counted, so accounting stays exact (Done always
//     balances Next); only the choice is momentarily suboptimal;
//   - the skew is bounded by the number of simultaneous pickers and
//     self-correcting: the very next scan sees the higher count;
//   - a CAS on "count == min" would retry under exactly the contention it
//     is meant to fix, and the counts change again microseconds later
//     anyway, so the extra precision buys nothing measurable.
func (lc *leastConn) Next() (*Backend, error) {
	n := uint64(len(lc.backends))
	if n == 0 {
		return nil, ErrNoHealthyBackends
	}
	now := lc.now()

	// Each call gets its own start position, so equal backends are chosen
	// in rotation; strict "<" below keeps the first minimum seen from start.
	start := lc.cursor.Add(1) - 1

	var best *Backend
	var bestInFlight int64
	for i := uint64(0); i < n; i++ {
		b := lc.backends[(start+i)%n]
		if !b.Available(now) {
			continue
		}
		if f := b.InFlight(); best == nil || f < bestInFlight {
			best, bestInFlight = b, f
		}
	}
	if best == nil {
		return nil, ErrNoHealthyBackends
	}
	best.acquire()
	return best, nil
}

// Done implements Balancer. It works for any b returned by Next, even one
// this balancer no longer contains after a config swap, because the count
// lives on the shared *Backend. b must be non-nil.
func (lc *leastConn) Done(b *Backend) {
	b.release()
}
