package balancer

import (
	"net/url"
	"time"
)

// NewBackend returns a Backend for base URL u with zero-value runtime state:
// actively healthy, never ejected, no failures and nothing in flight. u is
// stored as-is (not copied); the caller must not mutate it afterwards.
//
// Production code should obtain Backends from a Registry so that state
// survives config swaps; NewBackend is the low-level constructor.
func NewBackend(id string, u *url.URL) *Backend {
	return &Backend{ID: id, URL: u}
}

// Available reports whether the backend may receive new requests at now: it
// is not actively marked down and any passive ejection has expired
// (now >= deadline). Ejection expires lazily here, so no timer goroutine is
// needed to re-admit a backend.
func (b *Backend) Available(now time.Time) bool {
	if b.activeDown.Load() {
		return false
	}
	// 0 means "never ejected"; any real time is after the epoch, so this
	// comparison also covers that case without a branch.
	return now.UnixNano() >= b.ejectedUntilUnixNano.Load()
}

// InFlight returns the number of requests currently proxied to this backend.
func (b *Backend) InFlight() int64 {
	return b.inFlight.Load()
}

// SetActiveHealthy records the active checker's verdict and reports whether
// it differed from the previous one, so the caller can log or emit a metric
// only on transitions.
func (b *Backend) SetActiveHealthy(up bool) (changed bool) {
	down := !up
	// Swap returns the old value atomically, so concurrent callers can never
	// both report the same transition.
	return b.activeDown.Swap(down) != down
}

// ActiveHealthy reports the active checker's last verdict (true = up).
func (b *Backend) ActiveHealthy() bool {
	return !b.activeDown.Load()
}

// RecordFailure increments the consecutive passive-failure counter and
// returns the new value. It does not change availability by itself; the
// health package decides whether to call EjectUntil.
func (b *Backend) RecordFailure() int32 {
	return b.consecutiveFailures.Add(1)
}

// RecordSuccess resets the consecutive passive-failure counter to zero.
func (b *Backend) RecordSuccess() {
	b.consecutiveFailures.Store(0)
}

// EjectUntil passively ejects the backend until t (exclusive: it becomes
// available again at exactly t). The zero Time clears the ejection. The
// last writer wins; deciding whether to extend an existing ejection is the
// caller's job.
func (b *Backend) EjectUntil(t time.Time) {
	if t.IsZero() {
		// time.Time{}.UnixNano() is outside int64's range and undefined, so
		// map the zero Time to our "never ejected" sentinel explicitly.
		b.ejectedUntilUnixNano.Store(0)
		return
	}
	b.ejectedUntilUnixNano.Store(t.UnixNano())
}

// EjectedUntil returns the passive-ejection deadline, or the zero Time if
// the backend was never ejected (or the ejection was cleared).
func (b *Backend) EjectedUntil() time.Time {
	ns := b.ejectedUntilUnixNano.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// acquire records one more in-flight request. Balancers call it from Next
// after choosing b; it lives on Backend (not the Balancer) so every
// algorithm, and least-connections in particular, sees the same count.
func (b *Backend) acquire() {
	b.inFlight.Add(1)
}

// release records that an in-flight request finished. Because the counter
// is on the shared *Backend, this is correct even if the Balancer that
// handed b out has since been replaced or no longer contains b.
func (b *Backend) release() {
	b.inFlight.Add(-1)
}
