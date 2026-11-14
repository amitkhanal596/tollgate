package health

import (
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
)

// ObservePassive records the outcome of one proxied attempt to b and ejects
// b for p.Cooloff once p.ConsecutiveFailures failures have happened in a row
// (ADR 0006). It reports whether THIS call performed the ejection, so the
// caller can log or count each ejection exactly once.
//
// A failure is err != nil (dial refused, reset, the gateway's own timeout)
// or status >= 500; anything else is a success. err wins over status.
//
// Callers must not report client-canceled requests (the inbound request
// context was canceled, logged as 499): they say nothing about the backend,
// and reporting them would let clients that hang up eject healthy backends
// (docs/architecture.md §11 row 11). The outcome is taken at response-header
// time; body-copy errors are not reported.
//
// ObservePassive is stateless: the failure streak and the ejection deadline
// live on the shared *Backend, so they survive config swaps. Only a success
// resets the streak, NOT the end of the cooloff, so a backend that comes
// back and fails again is re-ejected on its first failure (a natural
// half-open probe). Failures while b is already ejected (requests that were
// in flight when it was ejected) neither extend the cooloff nor report a
// second ejection.
//
// p.ConsecutiveFailures <= 0 disables passive health: nothing is recorded
// and the call never ejects. p.Cooloff <= 0 is treated the same way, since
// an ejection of zero length would be a no-op that still reported "ejected"
// on every failure (config.Normalize defaults both fields anyway).
//
// Safe for concurrent use; under a burst of concurrent failures exactly one
// caller gets true.
func ObservePassive(b *balancer.Backend, p config.PassiveHealth, status int, err error, now time.Time) (ejected bool) {
	if p.ConsecutiveFailures <= 0 || p.Cooloff <= 0 {
		return false
	}
	if err == nil && status < 500 {
		b.RecordSuccess()
		return false
	}
	// RecordFailure is an atomic add, so concurrent callers each see a
	// distinct streak value and none of them is lost.
	if n := b.RecordFailure(); int64(n) < int64(p.ConsecutiveFailures) {
		return false
	}
	// The streak is at or past the threshold. ">=" rather than "==" is what
	// gives the re-eject-on-first-failure-after-cooloff behaviour; TryEject's
	// CAS makes sure only one caller ejects and that an ejection that is
	// still running is not extended by stragglers.
	return b.TryEject(now, now.Add(time.Duration(p.Cooloff)))
}
