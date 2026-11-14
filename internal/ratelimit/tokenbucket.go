package ratelimit

import (
	"context"
	"hash/maphash"
	"sync"
	"time"
)

const (
	// numShards splits the keyspace so concurrent requests for different
	// clients rarely contend on the same lock. Must be a power of two.
	numShards = 32

	// evictionSample bounds the work a new key at a full shard can cause under
	// the shard lock, so a key flood cannot become a CPU amplification.
	evictionSample = 8

	// capacityRetryAfter is the RetryAfter of an AtCapacity denial (ADR 0004).
	capacityRetryAfter = time.Second
)

// shard is one lock and the buckets whose keys hash to it.
//
// Why a mutex and not atomics/CAS: Go maps are not safe for concurrent use,
// and inserts, the bounded idle eviction at capacity and the sweeper all
// mutate the map, so the map needs a lock regardless. Under that lock the
// bucket update (a read-modify-write of tokens, last and fullAt together) is
// free and trivially atomic. The critical section is a map lookup plus a few
// float operations, so sharding keeps contention low.
type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket

	// Pad the 16-byte shard to 128 bytes (the cache-line size on Apple
	// silicon, and the x86 adjacent-line prefetch unit) so locking one shard
	// does not invalidate the cache line holding its neighbours' locks.
	_ [112]byte
}

// TokenBucket is the sharded, memory-bounded token-bucket Limiter of ADR 0004.
// It is safe for concurrent use. Create it with NewTokenBucket and start Run
// in a goroutine to evict idle buckets.
type TokenBucket struct {
	now      func() time.Time
	epoch    time.Time // origin for the bucket time offsets
	seed     maphash.Seed
	shardCap int // ceil(MaxKeys / numShards)
	interval time.Duration

	shards [numShards]shard
}

var _ Limiter = (*TokenBucket)(nil)

// NewTokenBucket returns a sharded, memory-bounded token-bucket limiter.
// Zero Options fields select the defaults (time.Now, DefaultMaxKeys,
// DefaultSweepInterval); negative values are treated as zero.
func NewTokenBucket(opts Options) *TokenBucket {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxKeys <= 0 {
		opts.MaxKeys = DefaultMaxKeys
	}
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = DefaultSweepInterval
	}
	shardCap := opts.MaxKeys / numShards
	if opts.MaxKeys%numShards != 0 {
		shardCap++ // ceil without the overflow risk of (n + 31) / 32
	}

	tb := &TokenBucket{
		now:   opts.Now,
		epoch: opts.Now(),
		// A per-limiter random seed (rather than a fixed hash such as FNV-1a)
		// means an attacker cannot precompute keys that all land in one shard
		// and exhaust that shard's ceil(MaxKeys/32) slots with 1/32 of the effort.
		seed:     maphash.MakeSeed(),
		shardCap: shardCap,
		interval: opts.SweepInterval,
	}
	for i := range tb.shards {
		tb.shards[i].buckets = make(map[string]*bucket)
	}
	return tb
}

// Allow implements Limiter. It consumes one token for key if one is available
// under limit. A new key arriving at a full shard is admitted only if an idle
// bucket can be evicted for it; otherwise it is denied with AtCapacity set and
// nothing is created.
func (tb *TokenBucket) Allow(key string, limit Limit) Decision {
	if !validLimit(limit) {
		return Decision{Allowed: true} // fail open, see Limiter
	}
	now := tb.offset(tb.now()) // read the clock before locking: keeps it out of the critical section
	s := tb.shardFor(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.buckets[key]
	if !ok {
		if len(s.buckets) >= tb.shardCap && !s.evictIdle(now) {
			return Decision{Limit: limit.Burst, RetryAfter: capacityRetryAfter, AtCapacity: true}
		}
		nb := newBucket(limit.Burst, now) // new keys start full
		b = &nb
		s.buckets[key] = b
	}
	next, d := b.take(now, limit.RatePerSecond, limit.Burst, true)
	*b = next
	return d
}

// Peek implements Limiter: the decision Allow would make now, computed on a
// copy of the bucket. It never consumes, never writes back and never creates
// a bucket. When the decision is Allowed, Remaining is the number of whole
// tokens available now (nothing is consumed).
func (tb *TokenBucket) Peek(key string, limit Limit) Decision {
	if !validLimit(limit) {
		return Decision{Allowed: true}
	}
	now := tb.offset(tb.now())
	s := tb.shardFor(key)

	s.mu.Lock()
	b, ok := s.buckets[key]
	var cp bucket
	if ok {
		cp = *b // copy under the lock, compute outside it
	}
	s.mu.Unlock()

	if !ok {
		cp = newBucket(limit.Burst, now) // an unknown key is a full bucket
	}
	_, d := cp.take(now, limit.RatePerSecond, limit.Burst, false)
	return d
}

// Sweep runs one synchronous idle-eviction pass at time now and returns how
// many buckets it removed. A bucket is idle when now >= fullAt: it has
// refilled completely and is indistinguishable from a brand-new bucket, so
// deleting it loses nothing. Shards are locked one at a time, so a sweep
// never blocks Allow on more than one shard.
func (tb *TokenBucket) Sweep(now time.Time) (removed int) {
	off := tb.offset(now)
	for i := range tb.shards {
		s := &tb.shards[i]
		s.mu.Lock()
		for k, b := range s.buckets {
			if off >= b.fullAt {
				delete(s.buckets, k) // deleting during range is safe in Go
				removed++
			}
		}
		s.mu.Unlock()
	}
	return removed
}

// Run calls Sweep every SweepInterval until ctx is done, then returns nil.
// It is the limiter's only goroutine.
func (tb *TokenBucket) Run(ctx context.Context) error {
	ticker := time.NewTicker(tb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			tb.Sweep(tb.now())
		}
	}
}

// Len returns the number of tracked keys, for metrics and tests. Shards are
// counted one at a time, so under concurrent writes it is a close estimate
// rather than a snapshot.
func (tb *TokenBucket) Len() int {
	n := 0
	for i := range tb.shards {
		s := &tb.shards[i]
		s.mu.Lock()
		n += len(s.buckets)
		s.mu.Unlock()
	}
	return n
}

// evictIdle deletes the first idle bucket (now >= fullAt) among up to
// evictionSample buckets and reports whether it found one. Buckets that still
// hold state are never evicted: that would hand a throttled client a fresh
// bucket (ADR 0004). The caller holds s.mu.
func (s *shard) evictIdle(now time.Duration) bool {
	sampled := 0
	// Map iteration starts at a random position, so successive calls sample
	// different buckets without any bookkeeping.
	for k, b := range s.buckets {
		if now >= b.fullAt {
			delete(s.buckets, k)
			return true
		}
		sampled++
		if sampled == evictionSample {
			break
		}
	}
	return false
}

func (tb *TokenBucket) shardFor(key string) *shard {
	return &tb.shards[maphash.String(tb.seed, key)&(numShards-1)]
}

// offset converts a clock reading to the limiter's internal time base.
// Time.Sub uses the monotonic reading when both times carry one, and
// saturates instead of overflowing.
func (tb *TokenBucket) offset(t time.Time) time.Duration {
	return t.Sub(tb.epoch)
}
