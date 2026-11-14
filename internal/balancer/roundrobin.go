package balancer

import (
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// roundRobin hands out available backends in rotation using a single atomic
// cursor, so Next is lock-free and costs one atomic add in the common case.
type roundRobin struct {
	backends []*Backend // immutable after construction
	now      func() time.Time
	cursor   atomic.Uint64
}

// NewRoundRobin returns a round-robin Balancer over backends. The slice is
// copied, so the caller may reuse its own slice; the *Backend values are
// shared on purpose (their state is what survives config swaps). now is the
// clock used to evaluate passive-ejection expiry.
func NewRoundRobin(backends []*Backend, now func() time.Time) Balancer {
	rr := &roundRobin{
		backends: append([]*Backend(nil), backends...),
		now:      now,
	}
	if n := len(rr.backends); n > 0 {
		// Random start so a fleet of gateways reloading together does not
		// all send their first request to backends[0] (ADR 0003).
		rr.cursor.Store(rand.Uint64N(uint64(n)))
	}
	return rr
}

// Next implements Balancer.
func (rr *roundRobin) Next() (*Backend, error) {
	n := uint64(len(rr.backends))
	if n == 0 {
		return nil, ErrNoHealthyBackends
	}
	now := rr.now()

	// Add-then-subtract claims a unique ticket per call without a CAS loop:
	// concurrent callers each get a distinct value, so healthy backends are
	// split exactly evenly. The cursor is a free-running counter reduced
	// modulo n on use. uint64 wrap-around is well defined in Go; at the
	// wrap the rotation skips at most one position once every 2^64 picks,
	// which is harmless.
	start := rr.cursor.Add(1) - 1

	// Try each position at most once, so an all-down set returns instead of
	// spinning.
	for i := uint64(0); i < n; i++ {
		b := rr.backends[(start+i)%n]
		if !b.Available(now) {
			continue
		}
		if i > 0 {
			// We skipped i unavailable backends. Move the cursor past them
			// too, otherwise the backend right after a down one would get
			// its own turn plus the down one's turn (2/N instead of 1/(N-1)).
			// Under concurrency this is approximate, which is fine.
			rr.cursor.Add(i)
		}
		// The backend may become unavailable between the check above and
		// this increment; sending one request to it is benign and the
		// passive/active health checks will catch real failures.
		b.acquire()
		return b, nil
	}
	return nil, ErrNoHealthyBackends
}

// Done implements Balancer. It works for any b returned by Next, even one
// this balancer no longer "owns" after a config swap, because the count
// lives on the shared *Backend. b must be non-nil.
func (rr *roundRobin) Done(b *Backend) {
	b.release()
}
