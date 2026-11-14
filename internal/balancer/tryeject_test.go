package balancer

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTryEject(t *testing.T) {
	deadline := t0.Add(30 * time.Second)
	tests := []struct {
		name      string
		prior     time.Time // existing ejection deadline; zero = none
		now       time.Time
		wantWon   bool
		wantUntil time.Time
	}{
		{"fresh backend", time.Time{}, t0, true, deadline},
		{"already ejected", t0.Add(time.Minute), t0, false, t0.Add(time.Minute)},
		{"ejection expired exactly now", t0, t0, true, deadline},
		{"ejection expired earlier", t0.Add(-time.Second), t0, true, deadline},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackend(t)
			b.EjectUntil(tc.prior)
			if got := b.TryEject(tc.now, deadline); got != tc.wantWon {
				t.Errorf("TryEject = %v, want %v", got, tc.wantWon)
			}
			if got := b.EjectedUntil(); !got.Equal(tc.wantUntil) {
				t.Errorf("EjectedUntil = %v, want %v", got, tc.wantUntil)
			}
		})
	}
}

func TestTryEjectConcurrentExactlyOneWinner(t *testing.T) {
	b := newTestBackend(t)
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if b.TryEject(t0, t0.Add(time.Minute)) {
				wins.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("winners = %d, want exactly 1", got)
	}
}

func TestRegistryPruneReturnsRemoved(t *testing.T) {
	r := NewRegistry()
	a := r.Get("u", mustURL(t, "http://a:1"))
	b := r.Get("u", mustURL(t, "http://b:1"))
	c := r.Get("u", mustURL(t, "http://c:1"))

	if got := r.Prune(map[string]struct{}{a.ID: {}, b.ID: {}, c.ID: {}}); got != nil {
		t.Fatalf("Prune keeping all = %v, want nil", got)
	}
	got := r.Prune(map[string]struct{}{b.ID: {}})
	ids := make([]string, 0, len(got))
	for _, x := range got {
		ids = append(ids, x.ID)
	}
	sort.Strings(ids)
	if len(got) != 2 || ids[0] != a.ID || ids[1] != c.ID {
		t.Fatalf("removed = %v, want [%s %s]", ids, a.ID, c.ID)
	}
	for _, x := range got {
		if x != a && x != c {
			t.Errorf("removed %p is not one of the original pointers", x)
		}
	}
}
