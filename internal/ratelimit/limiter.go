// Package ratelimit implements per-client request rate limiting with a
// hand-written token bucket (ADR 0004).
//
// Dependency rule: this package imports only the standard library and no
// other internal package (docs/architecture.md). The proxy converts the
// route's config.RateLimit into a Limit.
//
// Planned concrete type (Phase 3):
//
//	// NewTokenBucket returns a sharded, memory-bounded token-bucket limiter.
//	func NewTokenBucket(opts Options) *TokenBucket
//	func (tb *TokenBucket) Allow(key string, limit Limit) Decision // implements Limiter
//	// Run evicts idle buckets every opts.SweepInterval until ctx is done,
//	// then returns ctx.Err(). It is the limiter's only goroutine.
//	func (tb *TokenBucket) Run(ctx context.Context) error
//	func (tb *TokenBucket) Len() int // number of tracked keys, for metrics/tests
//
// The TokenBucket lives for the whole process, OUTSIDE the config snapshot.
// Because the Limit is passed on every call, a config change to a route's
// rate or burst takes effect on the next request with no bucket migration.
package ratelimit

import "time"

// Limit is the rate-limit policy for one route.
type Limit struct {
	// RatePerSecond is the sustained refill rate in tokens per second.
	// Must be > 0. Fractional values are allowed (0.5 = one request per 2s).
	RatePerSecond float64

	// Burst is the bucket capacity: the most requests a client can make
	// back-to-back after being idle. Must be >= 1.
	Burst int
}

// Decision is the outcome of one Allow call. It carries everything the
// proxy needs to answer 429 and to set rate-limit response headers.
type Decision struct {
	// Allowed reports whether the request may proceed. When true, one token
	// has been consumed.
	Allowed bool

	// Limit is the bucket capacity (Limit.Burst), for X-RateLimit-Limit.
	Limit int

	// Remaining is the number of whole tokens left after this decision
	// (rounded down), for X-RateLimit-Remaining.
	Remaining int

	// RetryAfter is zero when Allowed is true. Otherwise it is the exact time
	// until one full token will be available: (1 - tokens) / RatePerSecond.
	// The proxy rounds it UP to whole seconds (minimum 1) for the
	// Retry-After header, so a client that obeys the header does not retry
	// early and get a second 429.
	RetryAfter time.Duration
}

// Limiter decides whether a request identified by key may proceed.
//
// Contract:
//   - key identifies the bucket. The proxy builds it as
//     route name + "\x00" + client identity (API-key client ID, JWT subject,
//     or client IP when the route has no auth). The NUL separator prevents
//     two different (route, client) pairs from producing the same key.
//   - The check-and-consume is atomic per key: two concurrent callers can
//     never both take the last token.
//   - Allow never blocks on I/O and never sleeps.
//   - Time comes from the clock injected at construction (Options.Now),
//     never from time.Now directly, so tests are deterministic. If the clock
//     appears to go backwards, elapsed time is treated as zero.
//   - An invalid Limit (RatePerSecond <= 0 or Burst < 1) is treated as
//     "unlimited" and returns Allowed=true. Config validation rejects such
//     values, so this is a fail-open safety net, not a feature.
type Limiter interface {
	Allow(key string, limit Limit) Decision
}

// Options configures NewTokenBucket. Zero values select the defaults below.
type Options struct {
	// Now is the clock. Nil means time.Now. Tests inject a fake clock.
	Now func() time.Time

	// MaxKeys bounds memory: the maximum number of buckets kept at once.
	// When full, inserting a new key evicts an arbitrary existing bucket
	// in the same shard (the evicted client briefly gets a fresh bucket,
	// i.e. the limiter fails open for that one client rather than refusing
	// new clients).
	MaxKeys int

	// SweepInterval is how often Run deletes idle buckets. A bucket idle for
	// at least Burst/RatePerSecond seconds has refilled completely and is
	// indistinguishable from a brand-new bucket, so deleting it loses no
	// information.
	SweepInterval time.Duration
}

// Defaults used when the corresponding Options field is zero.
const (
	DefaultMaxKeys       = 100_000
	DefaultSweepInterval = time.Minute
)
