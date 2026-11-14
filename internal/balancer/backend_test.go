package balancer

import (
	"net/url"
	"sync"
	"testing"
	"time"
)

func mustURL(t testing.TB, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

var t0 = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

func newTestBackend(t testing.TB) *Backend {
	t.Helper()
	return NewBackend("users|http://10.0.0.7:8080", mustURL(t, "http://10.0.0.7:8080"))
}

func TestNewBackendInitialState(t *testing.T) {
	u := mustURL(t, "http://10.0.0.7:8080")
	b := NewBackend("users|http://10.0.0.7:8080", u)
	if b.ID != "users|http://10.0.0.7:8080" {
		t.Errorf("ID = %q", b.ID)
	}
	if b.URL != u {
		t.Errorf("URL = %v, want same pointer %v", b.URL, u)
	}
	if !b.Available(t0) {
		t.Error("new backend should be available")
	}
	if !b.ActiveHealthy() {
		t.Error("new backend should be actively healthy")
	}
	if got := b.InFlight(); got != 0 {
		t.Errorf("InFlight = %d, want 0", got)
	}
	if !b.EjectedUntil().IsZero() {
		t.Errorf("EjectedUntil = %v, want zero", b.EjectedUntil())
	}
}

func TestSetActiveHealthy(t *testing.T) {
	tests := []struct {
		name        string
		calls       []bool
		wantChanged []bool
		wantAvail   bool
	}{
		{"down", []bool{false}, []bool{true}, false},
		{"down then up", []bool{false, true}, []bool{true, true}, true},
		{"up is no-op", []bool{true}, []bool{false}, true},
		{"down twice reports no change", []bool{false, false}, []bool{true, false}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackend(t)
			for i, up := range tc.calls {
				if got := b.SetActiveHealthy(up); got != tc.wantChanged[i] {
					t.Errorf("call %d SetActiveHealthy(%v) changed = %v, want %v", i, up, got, tc.wantChanged[i])
				}
			}
			if got := b.Available(t0); got != tc.wantAvail {
				t.Errorf("Available = %v, want %v", got, tc.wantAvail)
			}
			if got := b.ActiveHealthy(); got != tc.wantAvail {
				t.Errorf("ActiveHealthy = %v, want %v", got, tc.wantAvail)
			}
		})
	}
}

func TestEjectUntil(t *testing.T) {
	deadline := t0.Add(30 * time.Second)
	tests := []struct {
		name string
		now  time.Time
		want bool
	}{
		{"well before", t0, false},
		{"just before", deadline.Add(-time.Nanosecond), false},
		{"exactly at deadline", deadline, true},
		{"after", deadline.Add(time.Second), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackend(t)
			b.EjectUntil(deadline)
			if got := b.Available(tc.now); got != tc.want {
				t.Errorf("Available(%v) = %v, want %v", tc.now, got, tc.want)
			}
			if got := b.EjectedUntil(); !got.Equal(deadline) {
				t.Errorf("EjectedUntil = %v, want %v", got, deadline)
			}
		})
	}
}

func TestAvailableRequiresBothHealthAndNoEjection(t *testing.T) {
	deadline := t0.Add(time.Minute)
	tests := []struct {
		name  string
		up    bool
		eject bool
		now   time.Time
		avail bool
	}{
		{"healthy not ejected", true, false, t0, true},
		{"healthy ejected", true, true, t0, false},
		{"down not ejected", false, false, t0, false},
		{"down ejected", false, true, t0, false},
		{"down, ejection expired", false, true, deadline.Add(time.Second), false},
		{"healthy, ejection expired", true, true, deadline.Add(time.Second), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackend(t)
			b.SetActiveHealthy(tc.up)
			if tc.eject {
				b.EjectUntil(deadline)
			}
			if got := b.Available(tc.now); got != tc.avail {
				t.Errorf("Available = %v, want %v", got, tc.avail)
			}
		})
	}
}

func TestRecordFailureAndSuccess(t *testing.T) {
	tests := []struct {
		name string
		ops  string // F = failure, S = success
		want []int32
	}{
		{"three failures", "FFF", []int32{1, 2, 3}},
		{"success resets", "FFSF", []int32{1, 2, 0, 1}},
		{"success on fresh", "SF", []int32{0, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBackend(t)
			for i, op := range tc.ops {
				var got int32
				if op == 'F' {
					got = b.RecordFailure()
				} else {
					b.RecordSuccess()
					// observe reset via the next failure below
					continue
				}
				if got != tc.want[i] && op == 'F' {
					t.Errorf("op %d RecordFailure = %d, want %d", i, got, tc.want[i])
				}
			}
		})
	}
	// Explicit reset check: after success the next failure returns 1.
	b := newTestBackend(t)
	b.RecordFailure()
	b.RecordFailure()
	b.RecordSuccess()
	if got := b.RecordFailure(); got != 1 {
		t.Errorf("after reset RecordFailure = %d, want 1", got)
	}
}

func TestRecordFailureDoesNotAffectAvailability(t *testing.T) {
	// Ejection is a separate decision made by the health package.
	b := newTestBackend(t)
	for i := 0; i < 100; i++ {
		b.RecordFailure()
	}
	if !b.Available(t0) {
		t.Error("RecordFailure alone must not make the backend unavailable")
	}
}

func TestBackendConcurrentUse(t *testing.T) {
	b := newTestBackend(t)
	const goroutines = 32
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				now := t0.Add(time.Duration(i) * time.Millisecond)
				switch (g + i) % 7 {
				case 0:
					b.SetActiveHealthy(i%2 == 0)
				case 1:
					b.RecordFailure()
				case 2:
					b.RecordSuccess()
				case 3:
					b.EjectUntil(now.Add(time.Second))
				case 4:
					_ = b.Available(now)
				case 5:
					_ = b.InFlight()
					_ = b.ActiveHealthy()
				case 6:
					_ = b.EjectedUntil()
				}
			}
		}(g)
	}
	wg.Wait()
}
