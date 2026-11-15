package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// burst sends n sequential GETs to path and returns the status codes.
func burst(t *testing.T, c *http.Client, base, path string, n int) []int {
	t.Helper()
	codes := make([]int, 0, n)
	for i := 0; i < n; i++ {
		codes = append(codes, getCode(t, c, base+path))
	}
	return codes
}

// quietFor reports whether b receives no traffic during n requests that the
// gateway answers (a way to observe "b is out of rotation" without peeking
// at internals).
func quietFor(t *testing.T, c *http.Client, base string, b *ctrlBackend, n int) bool {
	t.Helper()
	before := b.hits.Load()
	for _, code := range burst(t, c, base, "/x", n) {
		if code == http.StatusNotFound { // would make "quiet" vacuous
			t.Fatal("test bug: request did not route to the upstream")
		}
	}
	return b.hits.Load() == before
}

// threeUp starts A, B, C and a gateway in front with the given upstream
// settings.
func threeUp(t *testing.T, hc *config.HealthCheck, ph *config.PassiveHealth) (h *harness, srv *httptest.Server, a, b, c *ctrlBackend) {
	t.Helper()
	a, b, c = newCtrlBackend(t, "A"), newCtrlBackend(t, "B"), newCtrlBackend(t, "C")
	h, srv = startGatewayWith(t, Options{}, cfgOf(1,
		[]config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, hc, ph, a.url(), b.url(), c.url())))
	return
}

// L2: active health check takes a failing backend out of rotation, puts it
// back when it recovers, and an all-down upstream answers 503 (never 502).
func TestWiringActiveHealthRoutesAroundFailingBackend(t *testing.T) {
	_, srv, _, b, _ := threeUp(t, hcFast(), nil)
	c := newClient(t)

	for i, code := range burst(t, c, srv.URL, "/x", 9) {
		if code != 200 {
			t.Fatalf("warm-up request %d: %d", i, code)
		}
	}

	b.setDown() // /healthz and traffic both fail
	waitFor(t, 5*time.Second, "B to be marked down by probes", func() bool {
		return quietFor(t, c, srv.URL, b, 9)
	})
	// Once it is marked down, clients must never see its failures.
	for i, code := range burst(t, c, srv.URL, "/x", 60) {
		if code != 200 {
			t.Fatalf("request %d after B was marked down: status %d, want 200", i, code)
		}
	}

	b.setUp()
	before := b.hits.Load()
	waitFor(t, 5*time.Second, "B to receive traffic again after recovery", func() bool {
		burst(t, c, srv.URL, "/x", 6)
		return b.hits.Load() > before
	})
}

func TestWiringAllBackendsDownIs503(t *testing.T) {
	_, srv, a, b, cc := threeUp(t, hcFast(), nil)
	c := newClient(t)
	for _, x := range []*ctrlBackend{a, b, cc} {
		x.healthy.Store(false) // probes fail; traffic itself would still answer 200
	}
	waitFor(t, 5*time.Second, "503 once every backend is down", func() bool {
		return getCode(t, c, srv.URL+"/x") == http.StatusServiceUnavailable
	})
	for i, code := range burst(t, c, srv.URL, "/x", 10) {
		if code != http.StatusServiceUnavailable {
			t.Errorf("request %d with all backends down: %d, want 503 (not 502)", i, code)
		}
	}
}

// L3: a config swap that does not touch the upstream keeps the health
// verdict (state lives on the Registry's *Backend, and the Checker must not
// reset it).
func TestWiringHotReloadKeepsHealthState(t *testing.T) {
	a, b, cc := newCtrlBackend(t, "A"), newCtrlBackend(t, "B"), newCtrlBackend(t, "C")
	other := echoUpstream(t, "other")
	ups := []config.Upstream{
		upx("u", config.AlgorithmRoundRobin, hcFast(), nil, a.url(), b.url(), cc.url()),
		up("o", other.URL),
	}
	h, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, ups...))
	c := newClient(t)

	b.setDown()
	waitFor(t, 5*time.Second, "B to be marked down", func() bool { return quietFor(t, c, srv.URL, b, 9) })

	// Unrelated change: a second route to another upstream.
	h.apply(t, cfgOf(2, []config.Route{rt("r", "", "/", "u"), rt("r2", "", "/other", "o")}, ups...))

	// Immediately after the swap B must still be skipped: no grace window.
	before := b.hits.Load()
	for i, code := range burst(t, c, srv.URL, "/x", 60) {
		if code != 200 {
			t.Fatalf("request %d after reload: %d, want 200", i, code)
		}
	}
	if got := b.hits.Load(); got != before {
		t.Errorf("B received %d requests right after an unrelated reload; its down state was lost", got-before)
	}
}

// L4 (architecture.md section 11 row 7): removing the health_check from the
// upstream must bring a backend that probes had marked down back.
func TestWiringRemovingHealthCheckRevivesBackend(t *testing.T) {
	a, b, cc := newCtrlBackend(t, "A"), newCtrlBackend(t, "B"), newCtrlBackend(t, "C")
	h, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, hcFast(), nil, a.url(), b.url(), cc.url())))
	c := newClient(t)

	b.healthy.Store(false) // probes fail, traffic would be fine
	waitFor(t, 5*time.Second, "B to be marked down", func() bool { return quietFor(t, c, srv.URL, b, 9) })

	h.apply(t, cfgOf(2, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, nil, nil, a.url(), b.url(), cc.url())))

	before := b.hits.Load()
	waitFor(t, 3*time.Second, "B to get traffic after health_check was removed", func() bool {
		burst(t, c, srv.URL, "/x", 6)
		return b.hits.Load() > before
	})
}

// L5 (architecture.md section 11 row 3): a backend removed and later re-added
// gets a fresh *Backend; the Checker must probe the new pointer.
func TestWiringReAddedBackendIsProbedAgain(t *testing.T) {
	a, b, cc := newCtrlBackend(t, "A"), newCtrlBackend(t, "B"), newCtrlBackend(t, "C")
	h, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, hcFast(), nil, a.url(), b.url(), cc.url())))
	c := newClient(t)

	b.setDown()
	waitFor(t, 5*time.Second, "B to be marked down", func() bool { return quietFor(t, c, srv.URL, b, 9) })

	h.apply(t, cfgOf(2, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, hcFast(), nil, a.url(), cc.url())))
	b.setDown() // still unhealthy while it is out of the config

	probesBefore := b.probes.Load()
	h.apply(t, cfgOf(3, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, hcFast(), nil, a.url(), b.url(), cc.url())))

	waitFor(t, 5*time.Second, "the re-added B to be probed", func() bool { return b.probes.Load() > probesBefore })
	waitFor(t, 5*time.Second, "the re-added B to be marked down again", func() bool {
		return quietFor(t, c, srv.URL, b, 9)
	})
	for i, code := range burst(t, c, srv.URL, "/x", 40) {
		if code != 200 {
			t.Fatalf("request %d after B was marked down again: %d", i, code)
		}
	}
}

// L6a: passive ejection on consecutive 5xx, for roughly the cooloff.
func TestWiringPassiveEjectsOn5xx(t *testing.T) {
	const cooloff = 300 * time.Millisecond
	for _, status := range []int{500, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
			b.status.Store(int32(status))
			_, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
				upx("u", config.AlgorithmRoundRobin, nil,
					&config.PassiveHealth{ConsecutiveFailures: 3, Cooloff: config.Duration(cooloff)},
					a.url(), b.url())))
			c := newClient(t)

			start := time.Now()
			bad := 0
			for _, code := range burst(t, c, srv.URL, "/x", 20) {
				if code == status {
					bad++
				} else if code != 200 {
					t.Fatalf("unexpected status %d", code)
				}
			}
			if time.Since(start) > cooloff*2/3 {
				t.Skipf("machine too slow for a %v cooloff window (took %v)", cooloff, time.Since(start))
			}
			if bad != 3 || b.hits.Load() != 3 {
				t.Fatalf("clients saw %d x %d and B was hit %d times; want exactly 3 of each (ejected after 3 consecutive failures)",
					bad, status, b.hits.Load())
			}

			// After the cooloff B is tried again.
			third := b.times()[2]
			waitFor(t, 3*time.Second, "B to be retried after the cooloff", func() bool {
				getCode(t, c, srv.URL+"/x")
				return b.hits.Load() > 3
			})
			if gap := b.times()[3].Sub(third); gap < cooloff-50*time.Millisecond {
				t.Errorf("B was retried %v after its ejection, want about the %v cooloff", gap, cooloff)
			}
		})
	}
}

// L6b: a refused connection is a passive failure too.
func TestWiringPassiveEjectsOnConnectionRefused(t *testing.T) {
	a := newCtrlBackend(t, "A")
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // nothing listens there any more

	_, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, nil,
			&config.PassiveHealth{ConsecutiveFailures: 3, Cooloff: config.Duration(10 * time.Second)},
			a.url(), deadURL)))
	c := newClient(t)

	n502, n200 := 0, 0
	for _, code := range burst(t, c, srv.URL, "/x", 20) {
		switch code {
		case 502:
			n502++
		case 200:
			n200++
		default:
			t.Fatalf("unexpected status %d", code)
		}
	}
	if n502 < 1 || n502 > 3 || n200 != 20-n502 {
		t.Errorf("502s=%d 200s=%d, want 1..3 502s then only 200s (dead backend ejected after 3 failures)", n502, n200)
	}
}

// L6c: client errors never eject.
func TestWiringPassiveIgnores4xx(t *testing.T) {
	for _, status := range []int{400, 404, 429} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
			b.status.Store(int32(status))
			_, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
				upx("u", config.AlgorithmRoundRobin, nil,
					&config.PassiveHealth{ConsecutiveFailures: 3, Cooloff: config.Duration(10 * time.Second)},
					a.url(), b.url())))
			c := newClient(t)
			for i, code := range burst(t, c, srv.URL, "/x", 30) {
				if code != 200 && code != status {
					t.Fatalf("request %d: %d", i, code)
				}
			}
			if got := b.hits.Load(); got < 14 {
				t.Errorf("B served only %d of 30 requests: a %d response must not eject it", got, status)
			}
		})
	}
}

// L7 (architecture.md section 11 row 11): clients that hang up must not
// eject a healthy backend.
func TestWiringClientCancelIsNotAPassiveFailure(t *testing.T) {
	b := newCtrlBackend(t, "B")
	b.delay.Store(int64(5 * time.Second))
	h, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, nil,
			&config.PassiveHealth{ConsecutiveFailures: 2, Cooloff: config.Duration(30 * time.Second)},
			b.url())))

	const n = 10
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/x", nil)
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < n; i++ {
		<-done
	}
	waitFor(t, 3*time.Second, "all canceled requests to be reaped by the gateway", func() bool {
		return inFlight(t, h.g, "u", b.url()) == 0
	})
	if got := b.hits.Load(); got != n {
		t.Fatalf("backend saw %d requests, want %d (test did not exercise the path)", got, n)
	}

	b.delay.Store(0)
	if code := getCode(t, newClient(t), srv.URL+"/x"); code != http.StatusOK {
		t.Fatalf("after %d canceled requests the only backend answers %d, want 200 (it was ejected)", n, code)
	}
}

// L8: the gateway's own timeouts (504) DO count against the backend.
func TestWiringTimeoutsCountAsPassiveFailures(t *testing.T) {
	tests := []struct {
		name string
		tm   config.Timeouts
	}{
		{"response header timeout", config.Timeouts{ResponseHeader: ms(50)}},
		{"request timeout", config.Timeouts{Request: ms(50)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
			b.delay.Store(int64(2 * time.Second))
			r := rt("r", "", "/", "u")
			r.Timeouts = tc.tm
			_, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{r},
				upx("u", config.AlgorithmRoundRobin, nil,
					&config.PassiveHealth{ConsecutiveFailures: 3, Cooloff: config.Duration(30 * time.Second)},
					a.url(), b.url())))
			c := newClient(t)

			n504 := 0
			for _, code := range burst(t, c, srv.URL, "/x", 20) {
				switch code {
				case 504:
					n504++
				case 200:
				default:
					t.Fatalf("unexpected status %d", code)
				}
			}
			if n504 != 3 {
				t.Errorf("clients saw %d gateway timeouts, want exactly 3 (backend ejected after 3 consecutive)", n504)
			}
		})
	}
}

// L9: Run owns the health checker and the limiter sweepers; when it returns,
// probes have stopped and no goroutine is left behind.
func TestWiringRunShutdownStopsProbesAndGoroutines(t *testing.T) {
	b := newCtrlBackend(t, "A")
	baseline := runtime.NumGoroutine()

	h := startRunWith(t, Options{})
	h.apply(t, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, hcFast(), nil, b.url())))
	waitFor(t, 3*time.Second, "probes to start", func() bool { return b.probes.Load() >= 3 })
	// Open a pooled upstream connection too.
	if rec := serveAs(h.g, defaultRemote, "/x"); rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}

	h.cancel()
	select {
	case err := <-h.done:
		h.done <- err // the cleanup in startRunWith reads it again
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	atReturn := b.probes.Load()
	time.Sleep(150 * time.Millisecond) // several probe intervals
	if got := b.probes.Load(); got != atReturn {
		t.Errorf("backend saw %d health probes after Run returned", got-atReturn)
	}

	// Goroutines drain asynchronously (closed connections, timers): retry.
	const tolerance = 2
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > baseline+tolerance {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			buf = buf[:runtime.Stack(buf, true)]
			t.Fatalf("goroutines: %d, baseline %d (+%d tolerance)\n%s", runtime.NumGoroutine(), baseline, tolerance, buf)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
