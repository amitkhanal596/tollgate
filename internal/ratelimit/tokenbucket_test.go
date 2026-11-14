package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)

// fakeClock is a goroutine-safe manual clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTB(maxKeys int) (*TokenBucket, *fakeClock) {
	clk := newFakeClock()
	return NewTokenBucket(Options{Now: clk.Now, MaxKeys: maxKeys}), clk
}

func approx(t *testing.T, name string, got, want, tol time.Duration) {
	t.Helper()
	if d := got - want; d < -tol || d > tol {
		t.Errorf("%s = %v, want %v (+/- %v)", name, got, want, tol)
	}
}

// drain makes n Allow calls and returns how many were allowed.
func drain(tb *TokenBucket, key string, lim Limit, n int) int {
	ok := 0
	for i := 0; i < n; i++ {
		if tb.Allow(key, lim).Allowed {
			ok++
		}
	}
	return ok
}

func TestZeroOptionsDefaults(t *testing.T) {
	tb := NewTokenBucket(Options{})
	d := tb.Allow("k", Limit{RatePerSecond: 1, Burst: 2})
	if !d.Allowed || d.Limit != 2 || d.Remaining != 1 {
		t.Fatalf("decision = %+v", d)
	}
}

func TestNewKeyStartsFull(t *testing.T) {
	for _, burst := range []int{1, 2, 5, 50} {
		t.Run(fmt.Sprintf("burst=%d", burst), func(t *testing.T) {
			tb, _ := newTB(0)
			lim := Limit{RatePerSecond: 1, Burst: burst}
			for i := 0; i < burst; i++ {
				d := tb.Allow("k", lim)
				if !d.Allowed {
					t.Fatalf("request %d denied", i+1)
				}
				if d.Limit != burst {
					t.Errorf("Limit = %d, want %d", d.Limit, burst)
				}
				if d.Remaining != burst-1-i {
					t.Errorf("request %d Remaining = %d, want %d", i+1, d.Remaining, burst-1-i)
				}
				if d.RetryAfter != 0 {
					t.Errorf("RetryAfter = %v on allowed request", d.RetryAfter)
				}
				if d.AtCapacity {
					t.Error("AtCapacity set on allowed request")
				}
			}
			d := tb.Allow("k", lim)
			if d.Allowed {
				t.Fatal("burst+1 request allowed")
			}
			if d.Remaining != 0 || d.Limit != burst || d.RetryAfter <= 0 || d.AtCapacity {
				t.Errorf("denied decision = %+v", d)
			}
		})
	}
}

func TestRefill(t *testing.T) {
	tests := []struct {
		name    string
		lim     Limit
		advance time.Duration
		want    int // allowed after emptying the bucket then advancing
	}{
		{"one token after 1/rate", Limit{1, 5}, time.Second, 1},
		{"one token after 1/rate at 10/s", Limit{10, 5}, 100 * time.Millisecond, 1},
		{"just short of a token", Limit{1, 5}, 999 * time.Millisecond, 0},
		{"fractional rate 0.5/s needs 2s", Limit{0.5, 5}, 2 * time.Second, 1},
		{"fractional rate 0.5/s not at 1s", Limit{0.5, 5}, time.Second, 0},
		{"two tokens", Limit{1, 5}, 2 * time.Second, 2},
		{"long idle caps at burst", Limit{1, 5}, 24 * time.Hour, 5},
		{"cap at burst 1", Limit{100, 1}, time.Hour, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tb, clk := newTB(0)
			if got := drain(tb, "k", tc.lim, tc.lim.Burst); got != tc.lim.Burst {
				t.Fatalf("initial burst allowed %d, want %d", got, tc.lim.Burst)
			}
			if tb.Allow("k", tc.lim).Allowed {
				t.Fatal("expected denial after burst")
			}
			clk.Advance(tc.advance)
			if got := drain(tb, "k", tc.lim, tc.lim.Burst+10); got != tc.want {
				t.Errorf("allowed after refill = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestFractionalAccumulation(t *testing.T) {
	// rate 3/s, 100ms steps: 0.3 token per step, so tokens accumulate across calls.
	// Burst must be >= 2 here: with burst 1 the cap discards the fraction earned
	// between the 300ms and 400ms steps, and the correct answer is 26, not 31.
	tb, clk := newTB(0)
	lim := Limit{RatePerSecond: 3, Burst: 2}
	allowed := 0
	for i := 0; i <= 100; i++ { // t = 0 .. 10s
		if tb.Allow("k", lim).Allowed {
			allowed++
		}
		clk.Advance(100 * time.Millisecond)
	}
	want := 2 + 30 // initial burst + 3/s * 10s
	if allowed < want-1 || allowed > want+1 {
		t.Errorf("allowed = %d, want %d +/- 1", allowed, want)
	}
}

func TestRetryAfter(t *testing.T) {
	tests := []struct {
		name    string
		lim     Limit
		wait    time.Duration // time passed after emptying, before the denied call
		want    time.Duration
		wantNil bool
	}{
		{"1/s just emptied", Limit{1, 1}, 0, time.Second, false},
		{"10/s just emptied", Limit{10, 1}, 0, 100 * time.Millisecond, false},
		{"1/s half-way", Limit{1, 1}, 500 * time.Millisecond, 500 * time.Millisecond, false},
		{"10/s half-way", Limit{10, 3}, 50 * time.Millisecond, 50 * time.Millisecond, false},
		{"0.5/s just emptied", Limit{0.5, 1}, 0, 2 * time.Second, false},
		{"3/s just emptied", Limit{3, 2}, 0, time.Second / 3, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tb, clk := newTB(0)
			drain(tb, "k", tc.lim, tc.lim.Burst)
			clk.Advance(tc.wait)
			d := tb.Allow("k", tc.lim)
			if d.Allowed {
				t.Fatal("expected denial")
			}
			if d.RetryAfter <= 0 {
				t.Fatalf("RetryAfter = %v, must be > 0 when denied", d.RetryAfter)
			}
			approx(t, "RetryAfter", d.RetryAfter, tc.want, time.Millisecond)

			// Obeying the hint exactly must succeed (RetryAfter must not round down).
			clk.Advance(d.RetryAfter)
			if d2 := tb.Allow("k", tc.lim); !d2.Allowed {
				t.Errorf("Allow after exactly RetryAfter (%v) denied: %+v", d.RetryAfter, d2)
			}
		})
	}
}

func TestAccuracyUnderHammering(t *testing.T) {
	tests := []Limit{
		{1, 1}, {5, 10}, {10, 1}, {3, 7}, {100, 20}, {0.5, 2},
	}
	for _, lim := range tests {
		t.Run(fmt.Sprintf("rate=%v/burst=%d", lim.RatePerSecond, lim.Burst), func(t *testing.T) {
			tb, clk := newTB(0)
			step := time.Duration(float64(time.Second) / (10 * lim.RatePerSecond))
			allowed := 0
			for el := time.Duration(0); el <= 60*time.Second; el += step {
				if tb.Allow("k", lim).Allowed {
					allowed++
				}
				clk.Advance(step)
			}
			want := float64(lim.Burst) + lim.RatePerSecond*60
			if math.Abs(float64(allowed)-want) > 1 {
				t.Errorf("allowed = %d, want %v +/- 1", allowed, want)
			}
		})
	}
}

func TestKeysIndependent(t *testing.T) {
	tests := []struct{ name, a, b string }{
		{"plain", "a", "b"},
		{"same client different route", "api\x00alice", "web\x00alice"},
		{"nul boundary differs", "a\x00bc", "ab\x00c"},
		{"prefix", "route\x00", "route\x00x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tb, _ := newTB(0)
			lim := Limit{RatePerSecond: 1, Burst: 3}
			if drain(tb, tc.a, lim, 10) != 3 {
				t.Fatal("key A should allow exactly burst")
			}
			if tb.Allow(tc.a, lim).Allowed {
				t.Fatal("key A should be exhausted")
			}
			d := tb.Allow(tc.b, lim)
			if !d.Allowed || d.Remaining != 2 {
				t.Errorf("key B affected by key A: %+v", d)
			}
			if tb.Len() != 2 {
				t.Errorf("Len = %d, want 2", tb.Len())
			}
		})
	}
}

func TestLimitChange(t *testing.T) {
	t.Run("lowering burst clamps tokens", func(t *testing.T) {
		tb, _ := newTB(0)
		tb.Allow("k", Limit{1, 10}) // 9 tokens left
		d := tb.Allow("k", Limit{1, 3})
		if !d.Allowed || d.Limit != 3 || d.Remaining != 2 {
			t.Fatalf("decision = %+v, want allowed, Limit 3, Remaining 2", d)
		}
		if got := drain(tb, "k", Limit{1, 3}, 10); got != 2 {
			t.Errorf("further allowed = %d, want 2", got)
		}
	})
	t.Run("raising rate refills faster from now", func(t *testing.T) {
		tb, clk := newTB(0)
		drain(tb, "k", Limit{1, 10}, 10)
		clk.Advance(500 * time.Millisecond) // 0.5 token at the old rate, 5 at the new
		if got := drain(tb, "k", Limit{10, 10}, 20); got != 5 {
			t.Errorf("allowed = %d, want 5", got)
		}
	})
	t.Run("raising burst does not grant tokens by itself", func(t *testing.T) {
		tb, _ := newTB(0)
		drain(tb, "k", Limit{1, 2}, 2)
		if tb.Allow("k", Limit{1, 100}).Allowed {
			t.Error("raising burst must not refill an empty bucket")
		}
	})
	t.Run("decision reflects passed limit", func(t *testing.T) {
		tb, _ := newTB(0)
		tb.Allow("k", Limit{1, 4})
		if d := tb.Allow("k", Limit{2, 8}); d.Limit != 8 {
			t.Errorf("Limit = %d, want 8", d.Limit)
		}
	})
	t.Run("idle across a limit change over-admits less than one new burst", func(t *testing.T) {
		// architecture.md section 11 item 18.
		tb, clk := newTB(0)
		oldLim, newLim := Limit{1, 5}, Limit{1, 100}
		drain(tb, "k", oldLim, 5) // empty; fullAt = now+5s under the old limit
		clk.Advance(5 * time.Second)
		if removed := tb.Sweep(clk.Now()); removed != 1 {
			t.Fatalf("Sweep removed %d, want 1", removed)
		}
		d := tb.Allow("k", newLim)
		if !d.Allowed || d.Remaining != newLim.Burst-1 {
			t.Errorf("decision = %+v, want Remaining %d", d, newLim.Burst-1)
		}
	})
	t.Run("without a sweep the refill is honest", func(t *testing.T) {
		tb, clk := newTB(0)
		drain(tb, "k", Limit{1, 5}, 5)
		clk.Advance(5 * time.Second)
		d := tb.Allow("k", Limit{1, 100})
		if !d.Allowed || d.Remaining != 4 {
			t.Errorf("decision = %+v, want Remaining 4", d)
		}
	})
}

func TestClockGoingBackwards(t *testing.T) {
	tb, clk := newTB(0)
	lim := Limit{RatePerSecond: 1, Burst: 2}
	drain(tb, "k", lim, 2)
	clk.Advance(-time.Hour)
	if tb.Allow("k", lim).Allowed {
		t.Error("backwards clock must not refill")
	}
	clk.Advance(time.Hour + time.Second) // one real second after the last call
	if !tb.Allow("k", lim).Allowed {
		t.Error("expected one token after clock recovers")
	}
}

func TestPeek(t *testing.T) {
	lim := Limit{RatePerSecond: 1, Burst: 3}

	t.Run("unknown key peeks full and creates nothing", func(t *testing.T) {
		tb, _ := newTB(0)
		d := tb.Peek("new", lim)
		if !d.Allowed || d.Remaining != lim.Burst || d.Limit != lim.Burst || d.RetryAfter != 0 {
			t.Errorf("decision = %+v", d)
		}
		if tb.Len() != 0 {
			t.Errorf("Len = %d after Peek of unknown key, want 0", tb.Len())
		}
	})

	t.Run("matches Allow at every state and consumes nothing", func(t *testing.T) {
		tb, clk := newTB(0)
		tb.Allow("k", lim)
		for step := 0; step < 8; step++ {
			p1 := tb.Peek("k", lim)
			p2 := tb.Peek("k", lim)
			if p1 != p2 {
				t.Fatalf("step %d: Peek not idempotent: %+v vs %+v", step, p1, p2)
			}
			a := tb.Allow("k", lim)
			if p1.Allowed != a.Allowed || p1.Limit != a.Limit || p1.AtCapacity != a.AtCapacity {
				t.Fatalf("step %d: Peek %+v vs Allow %+v", step, p1, a)
			}
			if !a.Allowed {
				// Denied: all fields, including Remaining and RetryAfter, match.
				if p1 != a {
					t.Fatalf("step %d: denied Peek %+v != Allow %+v", step, p1, a)
				}
			} else if p1.RetryAfter != 0 {
				t.Fatalf("step %d: allowed Peek has RetryAfter %v", step, p1.RetryAfter)
			}
			clk.Advance(400 * time.Millisecond)
		}
	})

	t.Run("peek on throttled key reports retry without charging", func(t *testing.T) {
		tb, clk := newTB(0)
		drain(tb, "k", lim, 3)
		for i := 0; i < 100; i++ {
			d := tb.Peek("k", lim)
			if d.Allowed || d.RetryAfter <= 0 {
				t.Fatalf("decision = %+v", d)
			}
		}
		clk.Advance(time.Second)
		if !tb.Allow("k", lim).Allowed {
			t.Error("Peek must not have delayed refill")
		}
	})

	t.Run("100 peeks leave the full burst", func(t *testing.T) {
		tb, _ := newTB(0)
		tb.Allow("k", lim) // create the bucket (2 left)
		for i := 0; i < 100; i++ {
			tb.Peek("k", lim)
		}
		if got := drain(tb, "k", lim, 10); got != 2 {
			t.Errorf("allowed = %d, want 2", got)
		}
		tb2, _ := newTB(0)
		for i := 0; i < 100; i++ {
			tb2.Peek("fresh", lim)
		}
		if got := drain(tb2, "fresh", lim, 10); got != 3 {
			t.Errorf("fresh key allowed = %d, want full burst 3", got)
		}
	})
}

func TestSweep(t *testing.T) {
	lim := Limit{RatePerSecond: 1, Burst: 4}

	t.Run("removes idle keeps stateful", func(t *testing.T) {
		tb, clk := newTB(0)
		tb.Allow("idle", Limit{10, 1}) // refills in 100ms
		drain(tb, "throttled", lim, 4) // empty, needs 4s
		tb.Allow("partial", lim)       // 3 of 4, needs 1s
		clk.Advance(500 * time.Millisecond)
		if removed := tb.Sweep(clk.Now()); removed != 1 {
			t.Fatalf("removed = %d, want 1", removed)
		}
		if tb.Len() != 2 {
			t.Errorf("Len = %d, want 2", tb.Len())
		}
		if tb.Peek("throttled", lim).Allowed {
			t.Error("throttled bucket lost its state")
		}
		// partial still has 3 + 0.5 tokens: exactly 3 more allowed.
		if got := drain(tb, "partial", lim, 10); got != 3 {
			t.Errorf("partial allowed = %d, want 3", got)
		}
	})

	t.Run("boundary now >= fullAt", func(t *testing.T) {
		tb, clk := newTB(0)
		l := Limit{RatePerSecond: 1, Burst: 2}
		drain(tb, "k", l, 2) // fullAt = t0+2s
		t0 := clk.Now()
		if n := tb.Sweep(t0.Add(2*time.Second - time.Nanosecond)); n != 0 {
			t.Errorf("removed %d just before fullAt", n)
		}
		if n := tb.Sweep(t0.Add(2 * time.Second)); n != 1 {
			t.Errorf("removed %d at fullAt, want 1", n)
		}
		if tb.Len() != 0 {
			t.Errorf("Len = %d, want 0", tb.Len())
		}
	})

	t.Run("lossless: evicted key behaves as never evicted", func(t *testing.T) {
		tb, clk := newTB(0)
		fresh, _ := newTB(0)
		drain(tb, "k", lim, 3)
		clk.Advance(10 * time.Second)
		if tb.Sweep(clk.Now()) != 1 {
			t.Fatal("expected eviction")
		}
		for i := 0; i < 6; i++ {
			got, want := tb.Allow("k", lim), fresh.Allow("k", lim)
			if got.Allowed != want.Allowed || got.Remaining != want.Remaining || got.Limit != want.Limit {
				t.Fatalf("call %d: after eviction %+v, never evicted %+v", i, got, want)
			}
		}
	})

	t.Run("sweep on empty limiter", func(t *testing.T) {
		tb, clk := newTB(0)
		if n := tb.Sweep(clk.Now()); n != 0 {
			t.Errorf("removed = %d", n)
		}
	})
}

func TestCapacityFailsClosedForNewKeys(t *testing.T) {
	const maxKeys = 64 // 2 per shard
	const shardCap = 2
	const totalCap = 32 * shardCap
	lim := Limit{RatePerSecond: 1, Burst: 1} // one Allow leaves the bucket throttled

	tb, clk := newTB(maxKeys)

	var admitted []string
	capacityDenials := 0
	// Insert far more keys than the cap so every shard fills regardless of hash.
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("flood-%d\x00ip", i)
		before := tb.Len()
		d := tb.Allow(k, lim)
		switch {
		case d.Allowed:
			admitted = append(admitted, k)
		case d.AtCapacity:
			capacityDenials++
			if d.RetryAfter != time.Second {
				t.Fatalf("capacity RetryAfter = %v, want 1s", d.RetryAfter)
			}
			if tb.Len() != before {
				t.Fatal("capacity denial must not create a bucket")
			}
		default:
			t.Fatalf("new key %q denied without AtCapacity: %+v", k, d)
		}
		if tb.Len() > totalCap {
			t.Fatalf("Len = %d exceeds cap %d", tb.Len(), totalCap)
		}
	}
	if capacityDenials == 0 {
		t.Fatal("expected at least one AtCapacity denial")
	}
	if len(admitted) != tb.Len() {
		t.Fatalf("admitted %d keys but Len = %d: a throttled bucket was evicted", len(admitted), tb.Len())
	}
	// Every admitted key is still throttled (nothing evicted and recreated full).
	for _, k := range admitted {
		d := tb.Peek(k, lim)
		if d.Allowed || d.AtCapacity {
			t.Fatalf("admitted key %q lost its throttled state: %+v", k, d)
		}
	}
	// An existing throttled key is not "new": plain 429, not AtCapacity.
	if d := tb.Allow(admitted[0], lim); d.Allowed || d.AtCapacity {
		t.Errorf("existing throttled key got %+v", d)
	}

	// Idle buckets are reclaimed for new keys: let everything refill, no sweep.
	clk.Advance(time.Hour)
	d := tb.Allow("newcomer\x00ip", lim)
	if !d.Allowed || d.AtCapacity {
		t.Fatalf("new key with idle buckets present: %+v", d)
	}
	if tb.Len() > totalCap {
		t.Errorf("Len = %d exceeds cap %d", tb.Len(), totalCap)
	}
	if tb.Len() < len(admitted) {
		t.Errorf("Len = %d shrank below %d: eviction must replace, one for one", tb.Len(), len(admitted))
	}
}

func TestInvalidLimitFailsOpen(t *testing.T) {
	tests := []struct {
		name string
		lim  Limit
	}{
		{"zero rate", Limit{0, 5}},
		{"negative rate", Limit{-1, 5}},
		{"zero burst", Limit{1, 0}},
		{"negative burst", Limit{1, -3}},
		{"both zero", Limit{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tb, _ := newTB(0)
			for i := 0; i < 50; i++ {
				if d := tb.Allow("k", tc.lim); !d.Allowed {
					t.Fatalf("call %d denied: %+v", i, d)
				}
			}
			if d := tb.Peek("k", tc.lim); !d.Allowed {
				t.Errorf("Peek denied: %+v", d)
			}
		})
	}
}

func TestConcurrentSingleKeyNoOverAdmission(t *testing.T) {
	const goroutines, calls, burst = 64, 2000, 100
	tb, _ := newTB(0)
	lim := Limit{RatePerSecond: 1, Burst: burst} // clock frozen: no refill
	var wg sync.WaitGroup
	counts := make([]int, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < calls; i++ {
				if tb.Allow("hot", lim).Allowed {
					counts[g]++
				}
			}
		}()
	}
	wg.Wait()
	total := 0
	for _, c := range counts {
		total += c
	}
	if total != burst {
		t.Errorf("allowed = %d, want exactly %d", total, burst)
	}
}

func TestConcurrentManyKeysMixed(t *testing.T) {
	tb, clk := newTB(0)
	lim := Limit{RatePerSecond: 50, Burst: 5}
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				k := fmt.Sprintf("route\x00client-%d", (g*31+i)%1000)
				switch i % 5 {
				case 0:
					tb.Peek(k, lim)
				case 1:
					clk.Advance(time.Millisecond)
					tb.Sweep(clk.Now())
				default:
					tb.Allow(k, lim)
				}
				_ = tb.Len()
			}
		}()
	}
	wg.Wait()
	if n := tb.Len(); n > 1000 {
		t.Errorf("Len = %d, want <= 1000 distinct keys", n)
	}
}

func TestRunSweepsAndShutsDown(t *testing.T) {
	before := runtime.NumGoroutine()

	clk := newFakeClock()
	tb := NewTokenBucket(Options{Now: clk.Now, SweepInterval: 5 * time.Millisecond})
	for i := 0; i < 20; i++ {
		tb.Allow(fmt.Sprintf("k%d", i), Limit{RatePerSecond: 1, Burst: 2})
	}
	if tb.Len() != 20 {
		t.Fatalf("Len = %d, want 20", tb.Len())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tb.Run(ctx) }()

	clk.Advance(time.Hour) // every bucket is now idle; the sweeper must notice by itself
	deadline := time.Now().Add(3 * time.Second)
	for tb.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := tb.Len(); n != 0 {
		t.Errorf("Run did not evict idle buckets: Len = %d", n)
	}

	cancel()
	select {
	case err := <-done:
		// limiter.go documents ctx.Err(); the task brief says nil. Accept both.
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}

	deadline = time.Now().Add(time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("goroutine leak: %d before, %d after", before, after)
	}
}

func BenchmarkAllowParallel(b *testing.B) {
	clk := newFakeClock()
	tb := NewTokenBucket(Options{Now: clk.Now})
	lim := Limit{RatePerSecond: 1000, Burst: 100}
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("route\x00client-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tb.Allow(keys[i%len(keys)], lim)
			i++
		}
	})
}
