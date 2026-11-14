package ratelimit

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestTimeUntilAlwaysReachesTarget(t *testing.T) {
	// Property behind "advance by exactly RetryAfter, then Allow admits":
	// for awkward rates and fractional credit, the returned wait is enough.
	rng := rand.New(rand.NewSource(1))
	rates := []float64{1, 3, 7, 0.1, 0.3, 1.0 / 3, 13.37, 1e-3, 999.999, 1e6}
	for _, rate := range rates {
		for i := 0; i < 2000; i++ {
			nt := rng.Float64() * nanoTokensPerToken
			d := timeUntil(nt, nanoTokensPerToken, rate)
			if accrue(nt, d, rate) < nanoTokensPerToken {
				t.Fatalf("rate %v nt %v: wait %v falls short", rate, nt, d)
			}
			if d > 0 && accrue(nt, d-time.Millisecond, rate) >= nanoTokensPerToken && rate < 1e5 {
				t.Fatalf("rate %v nt %v: wait %v overshoots by >= 1ms", rate, nt, d)
			}
		}
	}
}

func TestTinyRateSaturatesInsteadOfOverflowing(t *testing.T) {
	tests := []float64{1e-9, 1e-12, math.SmallestNonzeroFloat64}
	for _, rate := range tests {
		tb, clk := newTB(0)
		lim := Limit{RatePerSecond: rate, Burst: 1}
		if !tb.Allow("k", lim).Allowed {
			t.Fatalf("rate %v: first call denied", rate)
		}
		d := tb.Allow("k", lim)
		if d.Allowed || d.RetryAfter <= 0 {
			t.Fatalf("rate %v: decision %+v, want denied with positive RetryAfter", rate, d)
		}
		clk.Advance(time.Hour)
		if n := tb.Sweep(clk.Now()); n != 0 {
			t.Errorf("rate %v: swept a bucket that is far from full", rate)
		}
	}
	if got := timeUntil(0, nanoTokensPerToken, math.SmallestNonzeroFloat64); got != maxDuration {
		t.Errorf("timeUntil = %v, want saturation at maxDuration", got)
	}
	if got := addSat(maxDuration-1, 5); got != maxDuration {
		t.Errorf("addSat = %v, want maxDuration", got)
	}
}

func TestNonFiniteRateFailsOpen(t *testing.T) {
	for _, rate := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		tb, _ := newTB(0)
		for i := 0; i < 5; i++ {
			if d := tb.Allow("k", Limit{RatePerSecond: rate, Burst: 1}); !d.Allowed {
				t.Fatalf("rate %v denied: %+v", rate, d)
			}
		}
		if tb.Len() != 0 {
			t.Errorf("rate %v: invalid limit created a bucket", rate)
		}
	}
}

func TestStaleClockReadIsChargedNothingAndRetryAfterStillAdmits(t *testing.T) {
	// A caller whose clock read lags the bucket's last update (lock reordering,
	// or a clock that stepped back) must not earn tokens, and its RetryAfter
	// must include the lag so obeying it from its own clock still admits.
	b := newBucket(1, 10*time.Second)
	b, d := b.take(10*time.Second, 1, 1, true)
	if !d.Allowed {
		t.Fatal("first take denied")
	}
	_, d = b.take(9*time.Second, 1, 1, true)
	if d.Allowed {
		t.Fatal("stale clock earned a token")
	}
	if want := 2 * time.Second; d.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want %v (1s lag + 1s refill)", d.RetryAfter, want)
	}
	if _, d2 := b.take(9*time.Second+d.RetryAfter, 1, 1, true); !d2.Allowed {
		t.Error("obeying RetryAfter from the lagging clock was denied")
	}
}

func TestWholeTokens(t *testing.T) {
	tests := []struct {
		nt   float64
		want int
	}{
		{0, 0},
		{nanoTokensPerToken - 1e-6, 0},
		{nanoTokensPerToken, 1},
		{5*nanoTokensPerToken - 1, 4},
		{1 << 63, 9223372036}, // float path: floor(2^63 / 1e9)
	}
	for _, tc := range tests {
		if got := wholeTokens(tc.nt); got != tc.want {
			t.Errorf("wholeTokens(%v) = %d, want %d", tc.nt, got, tc.want)
		}
	}
}

func TestEvictIdleSamplesAtMostEight(t *testing.T) {
	s := &shard{buckets: make(map[string]*bucket)}
	for i := 0; i < 100; i++ {
		s.buckets[string(rune('A'+i))] = &bucket{fullAt: time.Hour}
	}
	if s.evictIdle(time.Minute) {
		t.Fatal("evicted a bucket that still holds state")
	}
	if len(s.buckets) != 100 {
		t.Fatalf("len = %d, want 100", len(s.buckets))
	}
	if !s.evictIdle(time.Hour) || len(s.buckets) != 99 {
		t.Fatalf("expected exactly one idle eviction, len = %d", len(s.buckets))
	}
}

func TestMaxKeysRoundsUpPerShard(t *testing.T) {
	tests := []struct{ maxKeys, wantCap int }{
		{0, DefaultMaxKeys / numShards}, // 100000/32 = 3125 exactly
		{-5, DefaultMaxKeys / numShards},
		{1, 1},
		{32, 1},
		{33, 2},
		{64, 2},
	}
	for _, tc := range tests {
		tb := NewTokenBucket(Options{MaxKeys: tc.maxKeys})
		if tb.shardCap != tc.wantCap {
			t.Errorf("MaxKeys %d: shardCap = %d, want %d", tc.maxKeys, tb.shardCap, tc.wantCap)
		}
	}
}

func TestRunReturnsNilWhenContextAlreadyDone(t *testing.T) {
	tb := NewTokenBucket(Options{})
	if tb.interval != DefaultSweepInterval {
		t.Errorf("interval = %v, want default", tb.interval)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tb.Run(ctx); err != nil {
		t.Errorf("Run = %v, want nil", err)
	}
}
