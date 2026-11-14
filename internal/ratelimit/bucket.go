package ratelimit

import (
	"math"
	"time"
)

// nanoTokensPerToken is the unit of bucket.nanoTokens.
//
// Why nanotokens: with tokens counted in whole tokens, a refill is
// elapsed.Seconds()*rate, and values like 0.1s are not exact in binary, so ten
// 100ms refills at 1/s sum to 0.9999999999999999 and the token arrives one
// call late (and with burst 1 the late surplus is then lost to the cap). In
// nanotokens the refill is elapsed_ns*rate, which is an exact integer for any
// rate with up to nine decimal places, so whole-token boundaries are exact and
// the long-run admission count does not drift.
const nanoTokensPerToken = 1e9

// bucket is one key's token-bucket state (ADR 0004), refilled lazily on each
// call: there is no timer or goroutine per bucket.
//
// Times are offsets from the limiter's epoch (TokenBucket.offset) instead of
// time.Time values. That keeps the struct at 24 bytes with no pointers, so the
// GC never has to scan bucket memory, and the offsets come from Time.Sub, which
// uses the monotonic clock when the injected clock provides one.
type bucket struct {
	nanoTokens float64       // credit at time last, in [0, burst*1e9 of the last call]
	last       time.Duration // when nanoTokens was last computed; never moves backwards
	fullAt     time.Duration // when the bucket is full again under the last call's limit
}

// maxDuration is the saturation value for every time computation below.
const maxDuration = time.Duration(math.MaxInt64)

// newBucket returns the state of a key never seen before: full.
func newBucket(burst int, now time.Duration) bucket {
	return bucket{nanoTokens: float64(burst) * nanoTokensPerToken, last: now}
}

// take applies the lazy refill at now under the limit (rate, burst) and, if
// consume is true, spends one token when one is available. It is a pure
// function on a copy: Allow stores the returned bucket, Peek discards it, so
// both produce bit-identical decisions from the same state.
func (b bucket) take(now time.Duration, rate float64, burst int, consume bool) (bucket, Decision) {
	// Callers read the clock before taking the shard lock, so a goroutine can
	// arrive with a now slightly older than a concurrent caller's last; an
	// injected clock can also jump backwards. Either way, charge zero elapsed
	// time and never move last backwards (a later call would re-earn tokens).
	var lag time.Duration
	if now < b.last {
		lag = b.last - now
		now = b.last
	}

	// Refill at the rate passed to THIS call, and clamp to the burst passed to
	// this call, so a limit change applies from now on (ADR 0004).
	capacity := float64(burst) * nanoTokensPerToken
	b.nanoTokens = min(capacity, accrue(b.nanoTokens, now-b.last, rate))
	b.last = now

	d := Decision{Limit: burst}
	if b.nanoTokens >= nanoTokensPerToken {
		d.Allowed = true
		if consume {
			b.nanoTokens -= nanoTokensPerToken
		}
		d.Remaining = wholeTokens(b.nanoTokens)
	} else {
		// Remaining stays 0: less than one whole token is left.
		// lag is zero unless the caller's clock is behind last; adding it keeps
		// "advance the caller's clock by RetryAfter, then Allow" admitting.
		d.RetryAfter = addSat(timeUntil(b.nanoTokens, nanoTokensPerToken, rate), lag)
	}
	b.fullAt = addSat(now, timeUntil(b.nanoTokens, capacity, rate))
	return b, d
}

// accrue returns the credit nt plus what rate (tokens/s) earns over d, in
// nanotokens. It is the single place that does this arithmetic, so take and
// timeUntil agree to the last bit.
func accrue(nt float64, d time.Duration, rate float64) float64 {
	// The outer float64() conversion forbids the compiler from fusing the
	// multiply and add into one FMA instruction (allowed by the Go spec, and
	// done on arm64). Without it, inlined copies of this expression could
	// round differently and break the RetryAfter guarantee in timeUntil.
	return nt + float64(float64(d)*rate)
}

// timeUntil returns the wait, rounded UP to a whole nanosecond, after which
// accrue(nt, wait, rate) >= target. It saturates at maxDuration (about 292
// years) for tiny rates instead of overflowing.
//
// Callers pass target <= capacity, so the clamp in take cannot hide the result.
func timeUntil(nt, target, rate float64) time.Duration {
	if nt >= target {
		return 0
	}
	// nanotokens / (tokens per second) = nanoseconds.
	ns := math.Ceil((target - nt) / rate)
	// float64(maxDuration) rounds up to 2^63, so "<" guarantees the conversion
	// below fits in an int64. The negated form also catches NaN.
	if !(ns < float64(maxDuration)) {
		return maxDuration
	}
	d := time.Duration(ns)
	// The division above and the multiplication in accrue each round, so d can
	// be an ulp short of reaching target. Nudge it up (doubling the step, so
	// tiny rates where 1ns earns less than an ulp still finish in a few
	// iterations) until the exact expression take uses agrees. This is what
	// makes "advance the clock by exactly RetryAfter, then Allow" always admit.
	for step := time.Duration(1); accrue(nt, d, rate) < target; step *= 2 {
		if d > maxDuration-step {
			return maxDuration
		}
		d += step
	}
	return d
}

// wholeTokens returns floor(nt / 1e9) for nt >= 0. Integer division avoids a
// float quotient rounding up to the next integer just below a token boundary.
func wholeTokens(nt float64) int {
	if nt >= 1<<62 { // beyond int64 range for the conversion; burst > 4.6e9 is not realistic
		return int(nt / nanoTokensPerToken)
	}
	return int(int64(nt) / nanoTokensPerToken)
}

// addSat returns a+b for b >= 0, saturating at maxDuration instead of
// wrapping negative.
func addSat(a, b time.Duration) time.Duration {
	if a > maxDuration-b {
		return maxDuration
	}
	return a + b
}

// validLimit reports whether lim can drive a bucket. NaN and +Inf rates are
// rejected too (NaN fails "> 0"), since they would poison the float arithmetic.
func validLimit(lim Limit) bool {
	return lim.RatePerSecond > 0 && !math.IsInf(lim.RatePerSecond, 1) && lim.Burst >= 1
}
