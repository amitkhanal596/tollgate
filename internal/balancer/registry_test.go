package balancer

import (
	"sync"
	"testing"
	"time"
)

func TestRegistryIdentity(t *testing.T) {
	r := NewRegistry()
	a1 := r.Get("users", mustURL(t, "http://10.0.0.1:80"))
	a2 := r.Get("users", mustURL(t, "http://10.0.0.1:80")) // distinct *url.URL, same value

	tests := []struct {
		name     string
		other    *Backend
		wantSame bool
	}{
		{"same upstream and url", a2, true},
		{"different url", r.Get("users", mustURL(t, "http://10.0.0.2:80")), false},
		{"different upstream same url", r.Get("orders", mustURL(t, "http://10.0.0.1:80")), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.other == a1; got != tc.wantSame {
				t.Errorf("pointer equality = %v, want %v", got, tc.wantSame)
			}
		})
	}
	if a1.ID != "users|http://10.0.0.1:80" {
		t.Errorf("ID = %q, want %q", a1.ID, "users|http://10.0.0.1:80")
	}
	if got := r.Len(); got != 3 {
		t.Errorf("Len = %d, want 3", got)
	}
}

func TestRegistryStateSurvivesSwap(t *testing.T) {
	r := NewRegistry()
	u := mustURL(t, "http://10.0.0.1:80")
	b := r.Get("users", u)
	b.SetActiveHealthy(false)
	b.EjectUntil(t0.Add(time.Minute))
	b.RecordFailure()
	b.RecordFailure()

	again := r.Get("users", mustURL(t, "http://10.0.0.1:80"))
	if again != b {
		t.Fatal("expected same pointer")
	}
	if again.ActiveHealthy() {
		t.Error("active-down state lost")
	}
	if !again.EjectedUntil().Equal(t0.Add(time.Minute)) {
		t.Errorf("ejection lost: %v", again.EjectedUntil())
	}
	if got := again.RecordFailure(); got != 3 {
		t.Errorf("failure counter = %d, want 3", got)
	}
}

func TestRegistryPrune(t *testing.T) {
	tests := []struct {
		name    string
		keep    map[string]struct{}
		wantLen int
		kept    []string // upstream|url that must keep identity
		dropped []string
	}{
		{"keep all", map[string]struct{}{"u|http://a:1": {}, "u|http://b:1": {}}, 2, []string{"http://a:1", "http://b:1"}, nil},
		{"keep one", map[string]struct{}{"u|http://a:1": {}}, 1, []string{"http://a:1"}, []string{"http://b:1"}},
		{"keep none", map[string]struct{}{}, 0, nil, []string{"http://a:1", "http://b:1"}},
		{"nil keep", nil, 0, nil, []string{"http://a:1", "http://b:1"}},
		{"keep unknown id", map[string]struct{}{"zzz|http://x:1": {}}, 0, nil, []string{"http://a:1", "http://b:1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRegistry()
			old := map[string]*Backend{}
			for _, s := range []string{"http://a:1", "http://b:1"} {
				b := r.Get("u", mustURL(t, s))
				b.SetActiveHealthy(false)
				b.RecordFailure()
				old[s] = b
			}
			r.Prune(tc.keep)
			if got := r.Len(); got != tc.wantLen {
				t.Errorf("Len after prune = %d, want %d", got, tc.wantLen)
			}
			for _, s := range tc.kept {
				if r.Get("u", mustURL(t, s)) != old[s] {
					t.Errorf("%s: kept backend changed identity", s)
				}
			}
			for _, s := range tc.dropped {
				fresh := r.Get("u", mustURL(t, s))
				if fresh == old[s] {
					t.Errorf("%s: pruned backend returned same pointer", s)
				}
				if !fresh.ActiveHealthy() || fresh.RecordFailure() != 1 {
					t.Errorf("%s: fresh backend should have reset state", s)
				}
			}
		})
	}
}

func TestRegistryConcurrentGetSamePointer(t *testing.T) {
	r := NewRegistry()
	const n = 64
	got := make([]*Backend, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			got[i] = r.Get("users", mustURL(t, "http://10.0.0.1:80"))
		}(i)
	}
	close(start)
	wg.Wait()
	for i := 1; i < n; i++ {
		if got[i] != got[0] {
			t.Fatalf("goroutine %d got a different pointer", i)
		}
	}
	if r.Len() != 1 {
		t.Errorf("Len = %d, want 1", r.Len())
	}
}

func TestRegistryConcurrentGetAndPrune(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if i%4 == 0 {
					r.Prune(map[string]struct{}{})
				} else {
					r.Get("u", mustURL(t, "http://h:1")).RecordFailure()
				}
				_ = r.Len()
			}
		}(i)
	}
	wg.Wait()
}
