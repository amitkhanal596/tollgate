package health

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
)

func TestActiveHealthyBackendStaysAvailable(t *testing.T) {
	f := newFakeBackend(t)
	b := f.backend(t, "svc")
	h := newHarness(t, nil)
	h.c.Sync([]Target{{b, check("/healthz", 2, 3)}})
	h.start(t)

	eventually(t, wait, "at least 5 probes", func() bool { return f.hits.Load() >= 5 })
	if !b.ActiveHealthy() || !b.Available(time.Now()) {
		t.Fatal("healthy backend must stay available")
	}
	if ev := h.rec.snapshot(); len(ev) != 0 {
		t.Fatalf("onChange fired without a transition: %v", ev)
	}
}

// Hysteresis, fall side: threshold-1 failures followed by a success must
// reset the count, so the backend is never marked down.
func TestFallThresholdNotReachedKeepsBackendUp(t *testing.T) {
	for _, fall := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("fall=%d", fall), func(t *testing.T) {
			f := newFakeBackend(t)
			b := f.backend(t, "svc")
			pattern := make([]int, 0, fall)
			for i := 0; i < fall-1; i++ {
				pattern = append(pattern, 500)
			}
			pattern = append(pattern, 200) // fall-1 failures, then success, repeat
			f.setPattern(pattern...)

			h := newHarness(t, nil)
			h.c.Sync([]Target{{b, check("/healthz", 2, fall)}})
			h.start(t)

			always(t, 400*time.Millisecond, "backend never marked down", b.ActiveHealthy)
			if f.hits.Load() < int64(fall*3) {
				t.Fatalf("only %d probes; pattern was not exercised", f.hits.Load())
			}
			if ev := h.rec.snapshot(); len(ev) != 0 {
				t.Fatalf("unexpected transitions: %v", ev)
			}
		})
	}
}

func TestFallThresholdReachedMarksDown(t *testing.T) {
	for _, fall := range []int{1, 3} {
		t.Run(fmt.Sprintf("fall=%d", fall), func(t *testing.T) {
			f := newFakeBackend(t)
			b := f.backend(t, "svc")
			lb := balancer.NewRoundRobin([]*balancer.Backend{b}, time.Now)
			h := newHarness(t, nil)
			h.c.Sync([]Target{{b, check("/healthz", 2, fall)}})
			h.start(t)
			eventually(t, wait, "first probe", func() bool { return f.hits.Load() >= 1 })

			f.set(500)
			eventually(t, wait, "backend marked down", func() bool { return !b.ActiveHealthy() })
			if got, err := lb.Next(); got != nil || err != balancer.ErrNoHealthyBackends {
				t.Fatalf("balancer still picks a down backend: %v, %v", got, err)
			}
		})
	}
}

// Hysteresis, rise side: a down backend needs healthy_threshold consecutive
// successes; a failure in between resets the success count.
func TestRiseThreshold(t *testing.T) {
	f := newFakeBackend(t)
	b := f.backend(t, "svc")
	f.set(500)
	h := newHarness(t, nil)
	h.c.Sync([]Target{{b, check("/healthz", 3, 1)}})
	h.start(t)
	eventually(t, wait, "down", func() bool { return !b.ActiveHealthy() })

	// rise-1 successes then a failure, repeated: must stay down.
	f.setPattern(200, 200, 500)
	always(t, 400*time.Millisecond, "stays down on 2-of-3 successes", func() bool { return !b.ActiveHealthy() })

	f.set(200)
	eventually(t, wait, "up after consecutive successes", b.ActiveHealthy)
}

func TestWhatCountsAsProbeFailure(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(f *fakeBackend)
		wantDown bool
	}{
		{"200 is success", func(f *fakeBackend) { f.set(200) }, false},
		{"204 is success (any 2xx)", func(f *fakeBackend) { f.set(204) }, false},
		// ADR 0006: "a 2xx response counts as success"; everything else fails.
		{"302 is failure", func(f *fakeBackend) { f.set(302) }, true},
		{"404 is failure", func(f *fakeBackend) { f.set(404) }, true},
		{"500 is failure", func(f *fakeBackend) { f.set(500) }, true},
		{"503 is failure", func(f *fakeBackend) { f.set(503) }, true},
		{"connection refused is failure", func(f *fakeBackend) { f.close() }, true},
		{"hang past timeout is failure", func(f *fakeBackend) { f.set(modeHang) }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeBackend(t)
			b := f.backend(t, "svc")
			tc.setup(f)
			c := check("/healthz", 1, 2)
			c.Timeout = 60 * 1e6 // 60ms
			h := newHarness(t, nil)
			h.c.Sync([]Target{{b, c}})
			h.start(t)

			if tc.wantDown {
				eventually(t, wait, "marked down", func() bool { return !b.ActiveHealthy() })
				return
			}
			time.Sleep(300 * time.Millisecond)
			if !b.ActiveHealthy() {
				t.Fatal("marked down on a success status")
			}
		})
	}
}

// capturing round tripper: pins request shape.
type capture struct {
	next http.RoundTripper
	mu   sync.Mutex
	reqs []*http.Request
	dl   []time.Duration // remaining time to ctx deadline at send, -1 if none
}

func (c *capture) RoundTrip(r *http.Request) (*http.Response, error) {
	rem := time.Duration(-1)
	if d, ok := r.Context().Deadline(); ok {
		rem = time.Until(d)
	}
	c.mu.Lock()
	c.reqs = append(c.reqs, r)
	c.dl = append(c.dl, rem)
	c.mu.Unlock()
	return c.next.RoundTrip(r)
}

func TestProbeRequestShape(t *testing.T) {
	f := newFakeBackend(t)
	b := f.backend(t, "svc")
	cap := &capture{next: &http.Transport{DisableKeepAlives: true}}
	h := newHarness(t, &http.Client{Transport: cap})
	c := check("/status/ready", 1, 1)
	c.Timeout = 500 * 1e6 // 500ms
	h.c.Sync([]Target{{b, c}})
	h.start(t)
	eventually(t, wait, "3 probes", func() bool { return f.hits.Load() >= 3 })

	cap.mu.Lock()
	defer cap.mu.Unlock()
	for i, r := range cap.reqs {
		if r.Method != http.MethodGet {
			t.Errorf("probe %d method = %s, want GET", i, r.Method)
		}
		if r.URL.Path != "/status/ready" {
			t.Errorf("probe %d path = %q, want /status/ready", i, r.URL.Path)
		}
		if r.URL.Host != b.URL.Host {
			t.Errorf("probe %d host = %q, want %q", i, r.URL.Host, b.URL.Host)
		}
		if cap.dl[i] <= 0 || cap.dl[i] > 500*time.Millisecond {
			t.Errorf("probe %d ctx deadline remaining = %v, want in (0, timeout=500ms]", i, cap.dl[i])
		}
	}
	// Probes must be sequential per backend: never more than one in flight
	// is not observable here, but spacing must be about Interval, not a busy loop.
	if n := f.hits.Load(); n > 100 {
		t.Errorf("%d probes in a short window; interval not honoured", n)
	}
}

// A probe that hangs for `timeout` must not delay probing of other backends.
func TestHangingProbeDoesNotStallOtherBackends(t *testing.T) {
	hung := newFakeBackend(t)
	hung.set(modeHang)
	live := newFakeBackend(t)
	bh, bl := hung.backend(t, "a"), live.backend(t, "b")

	slow := check("/healthz", 1, 1)
	slow.Timeout = 1500 * 1e6 // 1.5s
	fast := check("/healthz", 1, 2)

	h := newHarness(t, nil)
	h.c.Sync([]Target{{bh, slow}, {bl, fast}})
	h.start(t)
	eventually(t, wait, "live backend probed", func() bool { return live.hits.Load() >= 2 })
	eventually(t, wait, "hung probe in progress", func() bool { return hung.hits.Load() >= 1 })

	live.set(500)
	start := time.Now()
	eventually(t, time.Second, "live backend marked down while the other probe hangs", func() bool { return !bl.ActiveHealthy() })
	if el := time.Since(start); el > 1200*time.Millisecond {
		t.Fatalf("took %v; probing was serialised behind the hanging probe", el)
	}
	if !bh.ActiveHealthy() {
		t.Log("hung backend already down (timeout elapsed); fine")
	}
	eventually(t, 5*time.Second, "hung backend eventually down", func() bool { return !bh.ActiveHealthy() })
}

func TestOnChangeFiresOncePerTransition(t *testing.T) {
	f := newFakeBackend(t)
	b := f.backend(t, "svc")
	h := newHarness(t, nil)
	h.c.Sync([]Target{{b, check("/healthz", 2, 2)}})
	h.start(t)
	eventually(t, wait, "probing", func() bool { return f.hits.Load() >= 2 })

	steps := []struct {
		status int
		up     bool
	}{{500, false}, {200, true}, {500, false}}
	for i, s := range steps {
		f.set(s.status)
		want := i + 1
		eventually(t, wait, fmt.Sprintf("transition %d", want), func() bool { return len(h.rec.snapshot()) >= want })
		// Let many more probes of the same verdict pass: no extra callbacks.
		n := f.hits.Load()
		eventually(t, wait, "more probes", func() bool { return f.hits.Load() >= n+6 })
		ev := h.rec.snapshot()
		if len(ev) != want {
			t.Fatalf("after step %d: %d events %v, want exactly %d", want, len(ev), ev, want)
		}
		if ev[i].up != s.up || ev[i].id != b.ID {
			t.Fatalf("event %d = %+v, want {%s %v}", i, ev[i], b.ID, s.up)
		}
	}
}

func TestSyncBeforeRunIsHonoured(t *testing.T) {
	f := newFakeBackend(t)
	f.set(500)
	b := f.backend(t, "svc")
	h := newHarness(t, nil)
	h.c.Sync([]Target{{b, check("/healthz", 2, 2)}}) // before Run
	if f.hits.Load() != 0 {
		t.Fatal("probe sent before Run")
	}
	h.start(t)
	eventually(t, wait, "down", func() bool { return !b.ActiveHealthy() })
}

func TestSyncAddsAndRemovesTargets(t *testing.T) {
	fa, fb := newFakeBackend(t), newFakeBackend(t)
	a, b := fa.backend(t, "a"), fb.backend(t, "b")
	h := newHarness(t, nil)
	h.start(t)

	h.c.Sync([]Target{{a, check("/healthz", 1, 1)}})
	eventually(t, wait, "a probed", func() bool { return fa.hits.Load() >= 2 })
	if fb.hits.Load() != 0 {
		t.Fatal("b probed before being added")
	}

	h.c.Sync([]Target{{a, check("/healthz", 1, 1)}, {b, check("/healthz", 1, 1)}})
	eventually(t, wait, "b probed after add", func() bool { return fb.hits.Load() >= 2 })

	h.c.Sync([]Target{{b, check("/healthz", 1, 1)}})
	time.Sleep(100 * time.Millisecond) // settle: in-flight probe may land
	n := fa.hits.Load()
	time.Sleep(200 * time.Millisecond)
	if got := fa.hits.Load(); got != n {
		t.Fatalf("removed target still probed: %d -> %d hits", n, got)
	}
	if fb.hits.Load() < 5 {
		t.Fatalf("remaining target b not probed (%d hits)", fb.hits.Load())
	}
}

func TestSyncChangedPathRestartsProbe(t *testing.T) {
	f := newFakeBackend(t)
	b := f.backend(t, "svc")
	h := newHarness(t, nil)
	h.c.Sync([]Target{{b, check("/old", 1, 1)}})
	h.start(t)
	eventually(t, wait, "/old probed", func() bool { return f.hitsOn("/old") >= 2 })

	h.c.Sync([]Target{{b, check("/new", 1, 1)}})
	eventually(t, wait, "/new probed", func() bool { return f.hitsOn("/new") >= 2 })
	time.Sleep(100 * time.Millisecond)
	n := f.hitsOn("/old")
	time.Sleep(200 * time.Millisecond)
	if got := f.hitsOn("/old"); got != n {
		t.Fatalf("old path still probed after settings change: %d -> %d", n, got)
	}
}

func TestSyncUnchangedTargetIsNotRestarted(t *testing.T) {
	// Re-syncing identical targets must not disturb probing: the probe rate
	// stays about one per interval rather than a restart storm.
	f := newFakeBackend(t)
	b := f.backend(t, "svc")
	h := newHarness(t, nil)
	tg := []Target{{b, check("/healthz", 1, 1)}}
	h.c.Sync(tg)
	h.start(t)
	eventually(t, wait, "probing", func() bool { return f.hits.Load() >= 2 })
	n0 := f.hits.Load()
	start := time.Now()
	for time.Since(start) < 300*time.Millisecond {
		h.c.Sync(tg)
		time.Sleep(time.Millisecond)
	}
	got := f.hits.Load() - n0
	// 300ms / 20ms interval = ~15 expected; a restart per Sync would give
	// far more (restart probes immediately) or far fewer (restart delays).
	if got < 5 || got > 40 {
		t.Fatalf("%d probes in 300ms with 20ms interval; unchanged Sync disturbed the probe", got)
	}
}

func TestSyncLatestWins(t *testing.T) {
	fs := make([]*fakeBackend, 4)
	bs := make([]*balancer.Backend, 4)
	for i := range fs {
		fs[i] = newFakeBackend(t)
		bs[i] = fs[i].backend(t, fmt.Sprintf("u%d", i))
	}
	h := newHarness(t, nil)
	h.start(t)
	for i := 0; i < 200; i++ {
		h.c.Sync([]Target{{bs[i%3], check("/healthz", 1, 1)}})
	}
	h.c.Sync([]Target{{bs[3], check("/healthz", 1, 1)}}) // final

	eventually(t, wait, "final target probed", func() bool { return fs[3].hits.Load() >= 3 })
	time.Sleep(150 * time.Millisecond)
	var before [3]int64
	for i := 0; i < 3; i++ {
		before[i] = fs[i].hits.Load()
	}
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if fs[i].hits.Load() != before[i] {
			t.Errorf("superseded target %d still probed", i)
		}
	}
}

// docs/architecture.md section 11 row 3: same ID, new pointer.
func TestSyncPointerIdentityNotID(t *testing.T) {
	f := newFakeBackend(t)
	p1 := f.backend(t, "svc")
	p2 := f.backend(t, "svc") // same ID and URL, different pointer (Registry prune + re-add)
	if p1.ID != p2.ID || p1 == p2 {
		t.Fatal("test setup: want same ID, distinct pointers")
	}
	h := newHarness(t, nil)
	h.c.Sync([]Target{{p1, check("/healthz", 1, 2)}})
	h.start(t)
	eventually(t, wait, "p1 probed", func() bool { return f.hits.Load() >= 2 })

	h.c.Sync([]Target{{p2, check("/healthz", 1, 2)}})
	f.set(500)
	eventually(t, wait, "p2 marked down", func() bool { return !p2.ActiveHealthy() })
	// The old pointer's probe is stopped and, per row 7, it is reset to up.
	eventually(t, wait, "p1 no longer probed / reset up", p1.ActiveHealthy)
	always(t, 150*time.Millisecond, "p1 stays up (not probed anymore)", p1.ActiveHealthy)
}

// docs/architecture.md section 11 row 7.
func TestRemovedTargetOrRemovedHealthCheckResetsToUp(t *testing.T) {
	tests := []struct {
		name string
		next func(other *balancer.Backend) []Target
	}{
		{"Sync(nil)", func(*balancer.Backend) []Target { return nil }},
		{"Sync without it (health_check removed, other upstream remains)", func(o *balancer.Backend) []Target {
			return []Target{{o, check("/healthz", 1, 1)}}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, fo := newFakeBackend(t), newFakeBackend(t)
			f.set(500)
			b, other := f.backend(t, "svc"), fo.backend(t, "other")
			h := newHarness(t, nil)
			h.c.Sync([]Target{{b, check("/healthz", 1, 2)}, {other, check("/healthz", 1, 1)}})
			h.start(t)
			eventually(t, wait, "marked down", func() bool { return !b.ActiveHealthy() })

			h.c.Sync(tc.next(other))
			eventually(t, wait, "reset to up", b.ActiveHealthy)
			// Backend still returns 500, but nobody probes it anymore.
			always(t, 200*time.Millisecond, "stays up", b.ActiveHealthy)
			if !b.Available(time.Now()) {
				t.Fatal("reset backend must be available")
			}
		})
	}
}

// Restart because settings changed keeps the verdict (ADR 0006).
func TestSettingsChangeKeepsVerdict(t *testing.T) {
	t.Run("stays down while new probe still failing", func(t *testing.T) {
		f := newFakeBackend(t)
		f.set(500)
		b := f.backend(t, "svc")
		h := newHarness(t, nil)
		h.c.Sync([]Target{{b, check("/old", 1, 2)}})
		h.start(t)
		eventually(t, wait, "down", func() bool { return !b.ActiveHealthy() })

		h.c.Sync([]Target{{b, check("/new", 1, 2)}})
		always(t, 300*time.Millisecond, "never flips up during restart", func() bool { return !b.ActiveHealthy() })
		if f.hitsOn("/new") == 0 {
			t.Fatal("new probe never ran")
		}
	})
	t.Run("stays down until rise reached on new probe, then recovers", func(t *testing.T) {
		f := newFakeBackend(t)
		f.setPath("/old", 500)
		f.setPath("/new", 200)
		b := f.backend(t, "svc")
		slow := func(p string) (c Target) {
			c = Target{b, check(p, 3, 1)}
			c.Check.Interval = 50 * 1e6 // 50ms: rise=3 needs >= ~100ms
			return
		}
		h := newHarness(t, nil)
		h.c.Sync([]Target{slow("/old")})
		h.start(t)
		eventually(t, wait, "down", func() bool { return !b.ActiveHealthy() })

		h.c.Sync([]Target{slow("/new")})
		if b.ActiveHealthy() {
			t.Fatal("verdict flipped up immediately on restart")
		}
		eventually(t, wait, "up after new probe proves it", b.ActiveHealthy)
	})
}

func TestRunShutdown(t *testing.T) {
	fs := []*fakeBackend{newFakeBackend(t), newFakeBackend(t), newFakeBackend(t)}
	var targets []Target
	for i, f := range fs {
		targets = append(targets, Target{f.backend(t, fmt.Sprintf("u%d", i)), check("/healthz", 1, 1)})
	}
	fs[2].set(modeHang) // a probe in flight at cancel time
	baseline := runtimeGoroutines()

	h := newHarness(t, nil)
	h.c.Sync(targets)
	h.start(t)
	eventually(t, wait, "probing", func() bool { return fs[0].hits.Load() >= 3 && fs[2].hits.Load() >= 1 })

	if err := h.stop(t); err != nil {
		t.Fatalf("Run returned %v after cancel, want nil", err)
	}
	goroutinesSettleTo(t, baseline)

	time.Sleep(50 * time.Millisecond) // let any in-flight request land
	var n [3]int64
	for i, f := range fs {
		n[i] = f.hits.Load()
	}
	time.Sleep(150 * time.Millisecond)
	for i, f := range fs {
		if f.hits.Load() != n[i] {
			t.Errorf("backend %d probed after Run returned", i)
		}
	}
}

func TestRunWithZeroTargets(t *testing.T) {
	baseline := runtimeGoroutines()
	h := newHarness(t, nil)
	h.start(t)
	time.Sleep(50 * time.Millisecond)
	h.c.Sync(nil)
	h.c.Sync([]Target{})
	if err := h.stop(t); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	goroutinesSettleTo(t, baseline)
}

func TestManyBackends(t *testing.T) {
	const n = 50
	fs := make([]*fakeBackend, n)
	bs := make([]*balancer.Backend, n)
	targets := make([]Target, n)
	for i := range fs {
		fs[i] = newFakeBackend(t)
		bs[i] = fs[i].backend(t, fmt.Sprintf("u%d", i))
		targets[i] = Target{bs[i], check("/healthz", 1, 2)}
	}
	h := newHarness(t, nil)
	h.c.Sync(targets)
	h.start(t)
	for i := 0; i < n; i += 2 {
		fs[i].set(500)
	}
	eventually(t, 5*time.Second, "even backends down, odd up", func() bool {
		for i, b := range bs {
			if b.ActiveHealthy() == (i%2 == 0) {
				return false
			}
		}
		return true
	})
}

func TestConcurrentSyncWhileProbing(t *testing.T) {
	fs := make([]*fakeBackend, 5)
	bs := make([]*balancer.Backend, 5)
	for i := range fs {
		fs[i] = newFakeBackend(t)
		bs[i] = fs[i].backend(t, fmt.Sprintf("u%d", i))
	}
	h := newHarness(t, nil)
	h.start(t)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				var ts []Target
				for j := range bs {
					if (i+g+j)%2 == 0 {
						ts = append(ts, Target{bs[j], check(fmt.Sprintf("/p%d", i%3), 1, 1)})
					}
				}
				h.c.Sync(ts)
				if i%10 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()

	// After the storm settles, the final state is whatever the last Sync said;
	// a deterministic Sync must converge.
	h.c.Sync([]Target{{bs[4], check("/final", 1, 1)}})
	eventually(t, wait, "converged on final target", func() bool { return fs[4].hitsOn("/final") >= 3 })
	if err := h.stop(t); err != nil {
		t.Fatal(err)
	}
}
