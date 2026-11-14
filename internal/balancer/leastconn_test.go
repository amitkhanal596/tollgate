package balancer

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

// hold calls Next n times on lb and returns the picked backends.
func hold(t *testing.T, lb Balancer, n int) []*Backend {
	t.Helper()
	out := make([]*Backend, 0, n)
	for i := 0; i < n; i++ {
		b, err := lb.Next()
		if err != nil {
			t.Fatalf("Next #%d: %v", i, err)
		}
		out = append(out, b)
	}
	return out
}

func TestLeastConnPicksFewestInFlight(t *testing.T) {
	bs := makeBackends(t, 3)
	a, b, c := bs[0], bs[1], bs[2]
	// Preload through a round-robin balancer sharing the same backends:
	// in-flight lives on the Backend, so this is the only setup needed.
	for i := 0; i < 3; i++ {
		a.acquire()
	}
	b.acquire()

	lb := NewLeastConn(bs, (&fakeClock{t: t0}).Now)

	got, err := lb.Next()
	if err != nil || got != c {
		t.Fatalf("first pick = %v, %v; want backend C (0 in flight)", got, err)
	}
	// Now C=1, B=1, A=3: tie between B and C is broken by the rotating
	// start, so either is fine, but never A.
	got, err = lb.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got == a {
		t.Fatalf("second pick = A (3 in flight); want B or C")
	}
	// Now one of B/C is at 2, the other at 1: the one at 1 must be next.
	other := b
	if got == b {
		other = c
	}
	got, err = lb.Next()
	if err != nil || got != other {
		t.Fatalf("third pick = %v; want the backend still at 1 in flight", got.ID)
	}
	if a.InFlight() != 3 {
		t.Errorf("A in flight = %d, want 3", a.InFlight())
	}
}

func TestLeastConnNextDoneAccounting(t *testing.T) {
	bs := makeBackends(t, 2)
	lb := NewLeastConn(bs, (&fakeClock{t: t0}).Now)

	picked := hold(t, lb, 4)
	if bs[0].InFlight()+bs[1].InFlight() != 4 {
		t.Fatalf("total in flight = %d, want 4", bs[0].InFlight()+bs[1].InFlight())
	}
	if bs[0].InFlight() != 2 || bs[1].InFlight() != 2 {
		t.Errorf("in flight = %d,%d; want 2,2", bs[0].InFlight(), bs[1].InFlight())
	}
	for _, p := range picked {
		lb.Done(p)
	}
	for _, b := range bs {
		if b.InFlight() != 0 {
			t.Errorf("%s in flight = %d after Done, want 0", b.ID, b.InFlight())
		}
	}
}

// ADR 0003: the scan starts at a rotating offset so ties are spread.
func TestLeastConnTieBreakRotates(t *testing.T) {
	bs := makeBackends(t, 3)
	lb := NewLeastConn(bs, (&fakeClock{t: t0}).Now)

	counts := map[*Backend]int{}
	for i := 0; i < 300; i++ {
		b, err := lb.Next()
		if err != nil {
			t.Fatal(err)
		}
		counts[b]++
		lb.Done(b)
	}
	for _, b := range bs {
		if counts[b] < 90 { // rotation gives exactly 100; allow slack
			t.Errorf("%s got %d of 300 picks; ties must rotate, not stick to index 0 (counts=%v)", b.ID, counts[b], len(counts))
		}
	}
}

func TestLeastConnSlowBackendAvoided(t *testing.T) {
	bs := makeBackends(t, 3)
	slow := bs[0]
	lb := NewLeastConn(bs, (&fakeClock{t: t0}).Now)

	// S never finishes 50 requests.
	for i := 0; i < 50; i++ {
		slow.acquire()
	}
	for i := 0; i < 100; i++ {
		b, err := lb.Next()
		if err != nil {
			t.Fatal(err)
		}
		if b == slow {
			t.Fatalf("pick %d went to the slow backend (%d in flight)", i, slow.InFlight())
		}
		lb.Done(b)
	}
	if slow.InFlight() != 50 {
		t.Errorf("slow in flight = %d, want 50", slow.InFlight())
	}
}

func TestLeastConnSkipsUnavailable(t *testing.T) {
	clk := &fakeClock{t: t0}

	tests := []struct {
		name  string
		setup func(bs []*Backend)
		want  func(bs []*Backend) *Backend // nil result with wantErr
		err   bool
	}{
		{
			name: "active down skipped despite fewest connections",
			setup: func(bs []*Backend) {
				bs[0].SetActiveHealthy(false)
				bs[1].acquire()
				bs[2].acquire()
				bs[2].acquire()
			},
			want: func(bs []*Backend) *Backend { return bs[1] },
		},
		{
			name: "ejected skipped despite fewest connections",
			setup: func(bs []*Backend) {
				bs[0].EjectUntil(t0.Add(time.Minute))
				bs[1].acquire()
				bs[2].acquire()
				bs[2].acquire()
			},
			want: func(bs []*Backend) *Backend { return bs[1] },
		},
		{
			name: "all unavailable",
			setup: func(bs []*Backend) {
				bs[0].SetActiveHealthy(false)
				bs[1].EjectUntil(t0.Add(time.Minute))
				bs[2].SetActiveHealthy(false)
			},
			err: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bs := makeBackends(t, 3)
			tc.setup(bs)
			before := [3]int64{bs[0].InFlight(), bs[1].InFlight(), bs[2].InFlight()}
			lb := NewLeastConn(bs, clk.Now)
			got, err := lb.Next()
			if tc.err {
				if !errors.Is(err, ErrNoHealthyBackends) || got != nil {
					t.Fatalf("Next = %v, %v; want nil, ErrNoHealthyBackends", got, err)
				}
				for i, b := range bs {
					if b.InFlight() != before[i] {
						t.Errorf("failed Next changed in-flight of backend %d", i)
					}
				}
				return
			}
			if err != nil || got != tc.want(bs) {
				t.Fatalf("Next = %v, %v; want %s", got, err, tc.want(bs).ID)
			}
		})
	}
}

func TestLeastConnEmpty(t *testing.T) {
	for _, in := range [][]*Backend{nil, {}} {
		lb := NewLeastConn(in, (&fakeClock{t: t0}).Now)
		b, err := lb.Next()
		if b != nil || !errors.Is(err, ErrNoHealthyBackends) {
			t.Fatalf("Next on empty = %v, %v", b, err)
		}
	}
}

func TestLeastConnEjectionExpiryUsesInjectedClock(t *testing.T) {
	clk := &fakeClock{t: t0}
	bs := makeBackends(t, 2)
	lb := NewLeastConn(bs, clk.Now)
	bs[0].EjectUntil(t0.Add(30 * time.Second))
	bs[1].EjectUntil(t0.Add(30 * time.Second))

	if b, err := lb.Next(); b != nil || !errors.Is(err, ErrNoHealthyBackends) {
		t.Fatalf("while ejected: %v, %v", b, err)
	}
	clk.Advance(29 * time.Second)
	if b, _ := lb.Next(); b != nil {
		t.Fatal("still ejected 1s before deadline")
	}
	clk.Advance(time.Second)
	b, err := lb.Next()
	if err != nil || b == nil {
		t.Fatalf("after expiry: %v, %v; want a backend", b, err)
	}
}

func TestLeastConnSharedStateAcrossBalancers(t *testing.T) {
	clk := &fakeClock{t: t0}
	bs := makeBackends(t, 2)

	tests := []struct {
		name string
		mk   func([]*Backend) Balancer
	}{
		{"round-robin plus least-conn", func(b []*Backend) Balancer { return NewRoundRobin(b, clk.Now) }},
		{"old and new least-conn snapshots", func(b []*Backend) Balancer { return NewLeastConn(b, clk.Now) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, b := range bs {
				if b.InFlight() != 0 {
					t.Fatalf("leaked in-flight from previous case")
				}
			}
			old := tc.mk(bs)
			cur := NewLeastConn(bs, clk.Now)

			// Old snapshot has 3 requests in flight on whatever it chose.
			var held []*Backend
			for i := 0; i < 3; i++ {
				b, err := old.Next()
				if err != nil {
					t.Fatal(err)
				}
				held = append(held, b)
			}
			total := bs[0].InFlight() + bs[1].InFlight()
			if total != 3 {
				t.Fatalf("total in flight = %d, want 3", total)
			}
			// The new balancer must see those counts and even things out:
			// after 3 more picks the totals differ by at most 1... and
			// never pile onto the busier backend.
			busier, lighter := bs[0], bs[1]
			if busier.InFlight() < lighter.InFlight() {
				busier, lighter = lighter, busier
			}
			if busier.InFlight() > lighter.InFlight() {
				got, err := cur.Next()
				if err != nil || got != lighter {
					t.Fatalf("new balancer picked %v; must see old balancer's in-flight and pick the lighter backend", got)
				}
				cur.Done(got)
			}
			for _, b := range held {
				cur.Done(b) // Done from a different balancer than Next
			}
			for _, b := range bs {
				if b.InFlight() != 0 {
					t.Errorf("%s in flight = %d, want 0", b.ID, b.InFlight())
				}
			}
		})
	}
}

func TestLeastConnDoneForBackendNoLongerInSlice(t *testing.T) {
	bs := makeBackends(t, 3)
	clk := &fakeClock{t: t0}
	old := NewLeastConn(bs, clk.Now)
	b, err := old.Next()
	if err != nil {
		t.Fatal(err)
	}
	// New snapshot dropped b.
	var rest []*Backend
	for _, x := range bs {
		if x != b {
			rest = append(rest, x)
		}
	}
	cur := NewLeastConn(rest, clk.Now)
	cur.Done(b)
	if b.InFlight() != 0 {
		t.Fatalf("in flight = %d, want 0", b.InFlight())
	}
}

func TestLeastConnConcurrent(t *testing.T) {
	bs := makeBackends(t, 4)
	lb := NewLeastConn(bs, (&fakeClock{t: t0}).Now)

	const goroutines, iters = 32, 2000
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				b, err := lb.Next()
				if err != nil {
					errs <- err
					return
				}
				runtime.Gosched() // hold the slot briefly so picks overlap
				lb.Done(b)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Next: %v", err)
	}
	var minIF, maxIF int64 = 1 << 62, -1
	for _, b := range bs {
		n := b.InFlight()
		if n < minIF {
			minIF = n
		}
		if n > maxIF {
			maxIF = n
		}
		if n != 0 {
			t.Errorf("%s in flight = %d at end, want 0", b.ID, n)
		}
	}
	if maxIF-minIF != 0 {
		t.Errorf("in-flight spread at end = %d, want 0", maxIF-minIF)
	}
}
