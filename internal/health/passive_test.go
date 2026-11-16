package health

import (
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
)

var pt0 = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

func newPassiveBackend(t testing.TB) *balancer.Backend {
	t.Helper()
	u, err := url.Parse("http://10.0.0.1:80")
	if err != nil {
		t.Fatal(err)
	}
	return balancer.NewBackend("svc|http://10.0.0.1:80", u)
}

func passiveCfg(n int, cooloff time.Duration) config.PassiveHealth {
	return config.PassiveHealth{ConsecutiveFailures: n, Cooloff: config.Duration(cooloff)}
}

var errDial = errors.New("dial tcp: connection refused")

type outcome struct {
	status int
	err    error
}

var (
	ok200   = outcome{200, nil}
	ok404   = outcome{404, nil}
	fail500 = outcome{500, nil}
	fail503 = outcome{503, nil}
	failErr = outcome{0, errDial}
)

func TestObservePassiveEjectsOnNthConsecutiveFailure(t *testing.T) {
	const cooloff = 30 * time.Second
	tests := []struct {
		name  string
		n     int
		seq   []outcome
		eject int // 1-based index of the call that must return true; 0 = never
	}{
		{"5xx x3 ejects on 3rd", 3, []outcome{fail500, fail500, fail500}, 3},
		{"mixed 5xx and transport errors", 3, []outcome{fail503, failErr, fail500}, 3},
		{"N-1 failures do not eject", 3, []outcome{fail500, fail500}, 0},
		{"success resets the streak", 3, []outcome{fail500, fail500, ok200, fail500, fail500}, 0},
		{"success then N failures ejects on Nth after it", 3, []outcome{fail500, ok200, fail500, fail500, fail500}, 5},
		{"4xx counts as success", 3, []outcome{fail500, fail500, ok404, fail500, fail500}, 0},
		{"error wins over status", 2, []outcome{{200, errDial}, {200, errDial}}, 2},
		{"threshold 1 ejects immediately", 1, []outcome{fail500}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newPassiveBackend(t)
			p := passiveCfg(tc.n, cooloff)
			for i, o := range tc.seq {
				got := ObservePassive(b, p, o.status, o.err, pt0)
				want := tc.eject == i+1
				if got != want {
					t.Fatalf("call %d returned ejected=%v, want %v", i+1, got, want)
				}
			}
			if tc.eject == 0 {
				if !b.Available(pt0) {
					t.Fatal("backend ejected unexpectedly")
				}
				return
			}
			if b.Available(pt0) {
				t.Fatal("backend still available after ejection")
			}
			if b.Available(pt0.Add(cooloff - time.Nanosecond)) {
				t.Fatal("backend available before cooloff elapsed")
			}
			if !b.Available(pt0.Add(cooloff)) {
				t.Fatal("backend not available at now+cooloff")
			}
			if !b.ActiveHealthy() {
				t.Fatal("passive ejection must not touch the active verdict")
			}
			if got := b.EjectedUntil(); !got.Equal(pt0.Add(cooloff)) {
				t.Fatalf("EjectedUntil = %v, want %v", got, pt0.Add(cooloff))
			}
		})
	}
}

// ADR 0006: the failure counter is reset ONLY by a success, not when the
// cooloff ends, so a backend that comes back and fails again is re-ejected on
// its first failure (a natural half-open probe).
func TestObservePassiveReEjectsOnFirstFailureAfterCooloff(t *testing.T) {
	const cooloff = 10 * time.Second
	p := passiveCfg(3, cooloff)
	b := newPassiveBackend(t)
	for i := 0; i < 3; i++ {
		ObservePassive(b, p, 500, nil, pt0)
	}
	later := pt0.Add(cooloff)
	if !b.Available(later) {
		t.Fatal("not available after cooloff")
	}
	if !ObservePassive(b, p, 500, nil, later) {
		t.Fatal("single failure after cooloff must re-eject (counter not reset by cooloff expiry)")
	}
	if b.Available(later.Add(cooloff - time.Nanosecond)) {
		t.Fatal("new ejection must run a full cooloff from the re-ejecting failure")
	}
	if !b.Available(later.Add(cooloff)) {
		t.Fatal("available again after second cooloff")
	}
}

func TestObservePassiveSuccessAfterCooloffRestartsStreak(t *testing.T) {
	const cooloff = 10 * time.Second
	p := passiveCfg(3, cooloff)
	b := newPassiveBackend(t)
	for i := 0; i < 3; i++ {
		ObservePassive(b, p, 500, nil, pt0)
	}
	later := pt0.Add(cooloff)
	if ObservePassive(b, p, 200, nil, later) {
		t.Fatal("success reported ejected")
	}
	if ObservePassive(b, p, 500, nil, later) {
		t.Fatal("ejected before N failures since the success")
	}
	if ObservePassive(b, p, 500, nil, later) {
		t.Fatal("ejected before N failures since the success")
	}
	if !ObservePassive(b, p, 500, nil, later) {
		t.Fatal("3rd failure after success must eject")
	}
}

func TestObservePassiveDisabledNeverEjects(t *testing.T) {
	tests := []struct {
		name string
		p    config.PassiveHealth
	}{
		{"zero value", config.PassiveHealth{}},
		{"zero threshold with cooloff", config.PassiveHealth{Cooloff: config.Duration(time.Minute)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newPassiveBackend(t)
			for i := 0; i < 100; i++ {
				if ObservePassive(b, tc.p, 500, errDial, pt0) {
					t.Fatalf("call %d ejected with passive health disabled", i+1)
				}
			}
			if !b.Available(pt0) {
				t.Fatal("backend unavailable with passive health disabled")
			}
		})
	}
}

func TestActiveAndPassiveCombine(t *testing.T) {
	const cooloff = 10 * time.Second
	p := passiveCfg(2, cooloff)
	later := pt0.Add(cooloff)

	t.Run("active down blocks even without ejection", func(t *testing.T) {
		b := newPassiveBackend(t)
		b.SetActiveHealthy(false)
		if b.Available(pt0) {
			t.Fatal("active-down backend available")
		}
	})
	t.Run("passive ejection leaves active verdict alone", func(t *testing.T) {
		b := newPassiveBackend(t)
		ObservePassive(b, p, 500, nil, pt0)
		ObservePassive(b, p, 500, nil, pt0)
		if !b.ActiveHealthy() {
			t.Fatal("ActiveHealthy flipped by passive ejection")
		}
		if b.Available(pt0) {
			t.Fatal("ejected backend available")
		}
	})
	t.Run("active up verdict does not clear ejection", func(t *testing.T) {
		b := newPassiveBackend(t)
		b.SetActiveHealthy(false)
		ObservePassive(b, p, 500, nil, pt0)
		ObservePassive(b, p, 500, nil, pt0)
		b.SetActiveHealthy(true) // active recovery
		if b.Available(pt0.Add(time.Second)) {
			t.Fatal("active recovery cleared the passive ejection")
		}
		if !b.Available(later) {
			t.Fatal("available after cooloff once active is up")
		}
	})
	t.Run("cooloff expiry does not clear active down", func(t *testing.T) {
		b := newPassiveBackend(t)
		ObservePassive(b, p, 500, nil, pt0)
		ObservePassive(b, p, 500, nil, pt0)
		b.SetActiveHealthy(false)
		if b.Available(later) {
			t.Fatal("ejection expiry made an active-down backend available")
		}
	})
}

func TestObservePassiveConcurrent(t *testing.T) {
	b := newPassiveBackend(t)
	p := passiveCfg(5, time.Minute)

	const goroutines, calls = 32, 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	ejections := 0
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < calls; i++ {
				if ObservePassive(b, p, 500, nil, pt0) {
					mu.Lock()
					ejections++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if b.Available(pt0) {
		t.Fatal("backend not ejected after 1600 concurrent failures")
	}
	if ejections == 0 {
		t.Fatal("no call reported ejected==true")
	}
	if got := b.EjectedUntil(); !got.Equal(pt0.Add(time.Minute)) {
		t.Fatalf("EjectedUntil = %v, want %v", got, pt0.Add(time.Minute))
	}
}
