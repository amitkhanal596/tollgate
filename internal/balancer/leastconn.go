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
	// ties is a rotating counter used ONLY to break ties: among the k
	// backends tied at the minimum, a call picks the (ties mod k)-th one.
	// Without it every tie (e.g. all backends idle) would go to the first
	// tied backend.
	ties atomic.Uint64
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
		// all break their first ties towards the same backend (ADR 0003).
		lc.ties.Store(rand.Uint64N(uint64(n)))
	}
	return lc
}

// Next implements Balancer.
//
// Tie-break: two passes over the backends, no allocation. Pass 1 finds the
// minimum in-flight count among available backends and how many (k) are tied
// at it. Pass 2 returns the (ties mod k)-th of those tied backends in slice
// order, so with a stable tied set every tied backend gets exactly 1/k of the
// picks.
//
// Why not the simpler "rotate the scan START and keep the first minimum
// seen" (the previous version): the start rotates over ALL n backends, but
// only the tied ones can win. A start that lands on an excluded backend (down,
// ejected, or busier) falls through to the next tied backend after it, so that
// backend wins for its own start position AND for every excluded position
// just before it. With [down, A, B], A won starts 0 and 1 and B only start 2:
// a 2:1 split instead of 1:1.
//
// Concurrency: the scans read each in-flight counter atomically but without
// a lock, then increment the winner. Two concurrent calls can read the
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
	if len(lc.backends) == 0 {
		return nil, ErrNoHealthyBackends
	}
	now := lc.now()

	// Pass 1: the minimum and the size of the tied set.
	var first *Backend // first backend at the minimum; the fallback below
	var minInFlight int64
	var tied uint64
	for _, b := range lc.backends {
		if !b.Available(now) {
			continue
		}
		switch f := b.InFlight(); {
		case first == nil || f < minInFlight:
			first, minInFlight, tied = b, f, 1
		case f == minInFlight:
			tied++
		}
	}
	if first == nil {
		return nil, ErrNoHealthyBackends
	}

	pick := first
	if tied > 1 {
		// Pass 2: the want-th tied backend. Counts and availability may have
		// changed since pass 1 (concurrent requests, an ejection); "<=" keeps
		// a backend that just got even less busy, and if fewer than want+1
		// candidates remain we keep the last one seen (or first). Either way
		// the choice is a least-loaded one as of pass 1, which is all the
		// lock-free design promises.
		want := (lc.ties.Add(1) - 1) % tied
		var seen uint64
		for _, b := range lc.backends {
			if !b.Available(now) || b.InFlight() > minInFlight {
				continue
			}
			pick = b
			if seen == want {
				break
			}
			seen++
		}
	}
	pick.acquire()
	return pick, nil
}

// Done implements Balancer. It works for any b returned by Next, even one
// this balancer no longer contains after a config swap, because the count
// lives on the shared *Backend. b must be non-nil.
func (lc *leastConn) Done(b *Backend) {
	b.release()
}
