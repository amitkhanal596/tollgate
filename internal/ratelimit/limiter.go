// Package ratelimit implements per-client request rate limiting with a
// hand-written token bucket (ADR 0004).
//
// Dependency rule: this package imports only the standard library and no
// other internal package (docs/architecture.md). The proxy converts the
// route's config.RateLimit into a Limit.
//
// Concrete type (tokenbucket.go):
//
//	// NewTokenBucket returns a sharded, memory-bounded token-bucket limiter.
//	func NewTokenBucket(opts Options) *TokenBucket
//	func (tb *TokenBucket) Allow(key string, limit Limit) Decision // implements Limiter
//	func (tb *TokenBucket) Peek(key string, limit Limit) Decision // read-only Allow
//	// Sweep runs one synchronous idle-eviction pass at time now and returns
//	// how many buckets it removed. Run calls it every SweepInterval.
//	func (tb *TokenBucket) Sweep(now time.Time) (removed int)
//	// Run evicts idle buckets every opts.SweepInterval until ctx is done,
//	// then returns nil. It is the limiter's only goroutine.
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
	// (rounded down), for X-RateLimit-Remaining. For Peek nothing is
	// consumed, so it is the whole tokens available now.
	Remaining int

	// RetryAfter is zero when Allowed is true. Otherwise it is the exact time
	// until one full token will be available: (1 - tokens) / RatePerSecond,
	// rounded up to the next nanosecond so that waiting exactly RetryAfter
	// always admits.
	// The proxy rounds it UP to whole seconds (minimum 1) for the
	// Retry-After header, so a client that obeys the header does not retry
	// early and get a second 429.
	RetryAfter time.Duration

	// AtCapacity is true only when the request was denied because the limiter
	// is full of buckets that still hold state and this key is new (ADR 0004,
	// architecture.md section 11 item 10). In that case Allowed is false and
	// RetryAfter is one second. The proxy counts it as
	// tollgate_ratelimited_total{reason="capacity"}.
	AtCapacity bool
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
//   - An invalid Limit (RatePerSecond <= 0, NaN or +Inf, or Burst < 1) is
//     treated as "unlimited": it returns Decision{Allowed: true} with zero
//     Limit and Remaining, and touches no bucket. Config validation rejects
//     such values, so this is a fail-open safety net, not a feature.
type Limiter interface {
	Allow(key string, limit Limit) Decision

	// Peek returns the decision Allow would make for key right now, without
	// consuming a token, without writing the bucket back and without creating
	// a bucket. An unknown key peeks as Allowed with Remaining = limit.Burst.
	// For a known key that would be allowed, Remaining is the number of whole
	// tokens available now (nothing is consumed). A denied Peek equals the
	// denied Allow at the same instant, field for field.
	// It is used by the failed-auth throttle (ADR 0004).
	Peek(key string, limit Limit) Decision
}

// Options configures NewTokenBucket. Zero values select the defaults below.
type Options struct {
	// Now is the clock. Nil means time.Now. Tests inject a fake clock.
	Now func() time.Time

	// MaxKeys bounds memory: the maximum number of buckets kept at once. It
	// is enforced per shard as ceil(MaxKeys/32), so the total can round up
	// to at most 32*ceil(MaxKeys/32). When a new key arrives at a full shard
	// the limiter samples up to 8 buckets and evicts the first idle one
	// (now >= fullAt, which loses nothing). If none is idle the new key is
	// DENIED with Decision{Allowed: false, AtCapacity: true, RetryAfter: 1s}.
	// Buckets that still hold state are never evicted: the limiter fails
	// closed for new keys only (ADR 0004).
	MaxKeys int

	// SweepInterval is how often Run deletes idle buckets. A bucket idle for
	// at least Burst/RatePerSecond seconds has refilled completely and is
	// indistinguishable from a brand-new bucket, so deleting it loses no
	// information.
	//
	// Accepted bound (architecture.md section 11 item 18): fullAt is computed
	// under the limit in force at a bucket's last Allow. If a route's limit
	// changes while a key is idle, the sweeper may delete it as full and it is
	// recreated full at the new burst, so over-admission is less than one
	// burst of the NEW limit, per key, once per limit change.
	SweepInterval time.Duration
}

// Defaults used when the corresponding Options field is zero.
const (
	DefaultMaxKeys       = 100_000
	DefaultSweepInterval = time.Minute
)
