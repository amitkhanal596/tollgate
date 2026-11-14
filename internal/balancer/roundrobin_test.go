package balancer

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
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

func makeBackends(t testing.TB, n int) []*Backend {
	t.Helper()
	bs := make([]*Backend, n)
	for i := range bs {
		bs[i] = NewBackend(fmt.Sprintf("svc|http://10.0.0.%d:80", i), mustURL(t, fmt.Sprintf("http://10.0.0.%d:80", i)))
	}
	return bs
}

func TestRoundRobinEvenDistributionSequential(t *testing.T) {
	bs := makeBackends(t, 3)
	clk := &fakeClock{t: t0}
	lb := NewRoundRobin(bs, clk.Now)

	counts := map[*Backend]int{}
	var prev *Backend
	var order []*Backend
	for i := 0; i < 300; i++ {
		b, err := lb.Next()
		if err != nil {
			t.Fatalf("Next #%d: %v", i, err)
		}
		counts[b]++
		order = append(order, b)
		if prev != nil && b == prev {
			t.Fatalf("pick %d repeated the previous backend", i)
		}
		prev = b
		lb.Done(b)
	}
	for _, b := range bs {
		if counts[b] != 100 {
			t.Errorf("%s got %d picks, want 100", b.ID, counts[b])
		}
	}
	// After the first full cycle every window of 3 consecutive picks has 3 distinct backends.
	for i := 0; i+2 < len(order); i++ {
		if order[i] == order[i+1] || order[i] == order[i+2] || order[i+1] == order[i+2] {
			t.Fatalf("window %d not a rotation", i)
		}
	}
}

func TestRoundRobinInFlightAccounting(t *testing.T) {
	bs := makeBackends(t, 3)
	lb := NewRoundRobin(bs, (&fakeClock{t: t0}).Now)

	var held []*Backend
	for i := 0; i < 6; i++ {
		b, err := lb.Next()
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, b)
	}
	for _, b := range bs {
		if got := b.InFlight(); got != 2 {
			t.Errorf("%s InFlight = %d, want 2", b.ID, got)
		}
	}
	for i, b := range held {
		lb.Done(b)
		if got := b.InFlight(); got < 0 {
			t.Fatalf("release %d: InFlight went negative (%d)", i, got)
		}
	}
	for _, b := range bs {
		if got := b.InFlight(); got != 0 {
			t.Errorf("%s InFlight = %d, want 0", b.ID, got)
		}
	}
}

func TestRoundRobinSkipsUnavailable(t *testing.T) {
	tests := []struct {
		name  string
		setup func(bs []*Backend)
		want  []int // indexes eligible
	}{
		{"one active down", func(bs []*Backend) { bs[1].SetActiveHealthy(false) }, []int{0, 2}},
		{"one ejected", func(bs []*Backend) { bs[0].EjectUntil(t0.Add(time.Hour)) }, []int{1, 2}},
		{"down and ejected", func(bs []*Backend) {
			bs[0].SetActiveHealthy(false)
			bs[2].EjectUntil(t0.Add(time.Hour))
		}, []int{1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bs := makeBackends(t, 3)
			tc.setup(bs)
			lb := NewRoundRobin(bs, (&fakeClock{t: t0}).Now)
			const picks = 120
			counts := map[*Backend]int{}
			for i := 0; i < picks; i++ {
				b, err := lb.Next()
				if err != nil {
					t.Fatalf("Next: %v", err)
				}
				counts[b]++
				lb.Done(b)
			}
			per := picks / len(tc.want)
			for _, i := range tc.want {
				// Allow +-1 since skipping may shift the cursor.
				if c := counts[bs[i]]; c < per-1 || c > per+1 {
					t.Errorf("backend %d got %d, want about %d", i, c, per)
				}
			}
			total := 0
			for _, i := range tc.want {
				total += counts[bs[i]]
			}
			if total != picks {
				t.Errorf("picks leaked to unavailable backends: eligible got %d of %d", total, picks)
			}
		})
	}
}

func TestRoundRobinBackendRejoins(t *testing.T) {
	bs := makeBackends(t, 3)
	lb := NewRoundRobin(bs, (&fakeClock{t: t0}).Now)
	bs[1].SetActiveHealthy(false)

	for i := 0; i < 10; i++ {
		b, _ := lb.Next()
		if b == bs[1] {
			t.Fatal("picked down backend")
		}
		lb.Done(b)
	}
	bs[1].SetActiveHealthy(true)
	seen := false
	for i := 0; i < 9; i++ {
		b, err := lb.Next()
		if err != nil {
			t.Fatal(err)
		}
		if b == bs[1] {
			seen = true
		}
		lb.Done(b)
	}
	if !seen {
		t.Error("recovered backend never picked")
	}
}

func TestRoundRobinNoHealthyBackends(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T) []*Backend
	}{
		{"nil slice", func(*testing.T) []*Backend { return nil }},
		{"empty slice", func(*testing.T) []*Backend { return []*Backend{} }},
		{"all active down", func(t *testing.T) []*Backend {
			bs := makeBackends(t, 3)
			for _, b := range bs {
				b.SetActiveHealthy(false)
			}
			return bs
		}},
		{"all ejected", func(t *testing.T) []*Backend {
			bs := makeBackends(t, 3)
			for _, b := range bs {
				b.EjectUntil(t0.Add(time.Hour))
			}
			return bs
		}},
		{"mixed", func(t *testing.T) []*Backend {
			bs := makeBackends(t, 2)
			bs[0].SetActiveHealthy(false)
			bs[1].EjectUntil(t0.Add(time.Hour))
			return bs
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bs := tc.build(t)
			lb := NewRoundRobin(bs, (&fakeClock{t: t0}).Now)
			b, err := lb.Next()
			if !errors.Is(err, ErrNoHealthyBackends) {
				t.Errorf("err = %v, want ErrNoHealthyBackends", err)
			}
			if b != nil {
				t.Errorf("backend = %v, want nil", b)
			}
			for _, x := range bs {
				if x.InFlight() != 0 {
					t.Errorf("%s InFlight = %d, want 0", x.ID, x.InFlight())
				}
			}
		})
	}
}

func TestRoundRobinEjectionExpires(t *testing.T) {
	bs := makeBackends(t, 1)
	clk := &fakeClock{t: t0}
	lb := NewRoundRobin(bs, clk.Now)
	bs[0].EjectUntil(t0.Add(30 * time.Second))

	if _, err := lb.Next(); !errors.Is(err, ErrNoHealthyBackends) {
		t.Fatalf("before expiry err = %v", err)
	}
	clk.Advance(29 * time.Second)
	if _, err := lb.Next(); !errors.Is(err, ErrNoHealthyBackends) {
		t.Fatalf("just before expiry err = %v", err)
	}
	clk.Advance(time.Second) // exactly at deadline
	b, err := lb.Next()
	if err != nil || b != bs[0] {
		t.Fatalf("after expiry got (%v, %v), want backend", b, err)
	}
	lb.Done(b)
}

func TestRoundRobinConcurrent(t *testing.T) {
	bs := makeBackends(t, 3)
	lb := NewRoundRobin(bs, (&fakeClock{t: t0}).Now)
	const goroutines, iters = 16, 3000

	var mu sync.Mutex
	counts := map[*Backend]int{}
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := map[*Backend]int{}
			for i := 0; i < iters; i++ {
				b, err := lb.Next()
				if err != nil {
					t.Errorf("Next: %v", err)
					return
				}
				local[b]++
				lb.Done(b)
			}
			mu.Lock()
			for b, c := range local {
				counts[b] += c
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	total := goroutines * iters
	even := float64(total) / 3
	for _, b := range bs {
		if got := b.InFlight(); got != 0 {
			t.Errorf("%s InFlight = %d, want 0", b.ID, got)
		}
		dev := (float64(counts[b]) - even) / even
		if dev < -0.01 || dev > 0.01 {
			t.Errorf("%s got %d picks, want within 1%% of %.0f", b.ID, counts[b], even)
		}
	}
}

func TestRoundRobinDoneAfterConfigSwap(t *testing.T) {
	bs := makeBackends(t, 3)
	clk := &fakeClock{t: t0}
	a := NewRoundRobin(bs, clk.Now)

	b, err := a.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b.InFlight() != 1 {
		t.Fatalf("InFlight = %d, want 1", b.InFlight())
	}

	// New snapshot whose balancer no longer contains b.
	var rest []*Backend
	for _, x := range bs {
		if x != b {
			rest = append(rest, x)
		}
	}
	_ = NewRoundRobin(rest, clk.Now)

	a.Done(b) // old balancer, backend removed from the new one
	if got := b.InFlight(); got != 0 {
		t.Errorf("InFlight = %d, want 0", got)
	}
}
