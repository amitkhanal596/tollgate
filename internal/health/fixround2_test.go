package health

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
)

// D8: probes must never go through an environment HTTP proxy. A probe that
// is silently proxied reports the proxy's health, not the backend's. Go
// never proxies loopback, so a behavioural test is impossible in a unit
// test; pin the structure instead: the transport the Checker builds for
// itself has no Proxy func.
func TestDefaultProbeTransportIgnoresEnvironmentProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("NO_PROXY", "")

	c := NewChecker(nil, nil, nil)
	tr, ok := c.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("default probe transport is %T, want *http.Transport", c.client.Transport)
	}
	if tr.Proxy != nil {
		t.Error("default probe transport has a non-nil Proxy (http.ProxyFromEnvironment): probes would go through HTTP(S)_PROXY")
	}
	if c.ownsTran == nil || c.ownsTran != tr {
		t.Error("default probe transport is not the one the Checker owns and closes")
	}
}

// D9 (race hunt): the probe hand-off chain. A backend is DOWN by its probe;
// then, in quick succession, Sync changes its settings (restart, keeps the
// verdict) and Sync drops it (removed -> must be reset to up). The restarted
// probe may be stopped before it has even started, while its predecessor is
// still winding down; the final state must still be "up" and no probe may
// touch the backend afterwards.
//
// This is a regression lock for a race: it passes most iterations on the
// current code, and only fails when the scheduler interleaves the
// predecessor's last verdict write after the successor's reset. Run with
// -race and -count to hunt it.
func TestProbeHandoffChainEndsUp(t *testing.T) {
	const iterations = 200
	f := newFakeBackend(t)
	h := newHarness(t, nil)
	h.start(t)

	fast := func(path string, fall int) config.HealthCheck {
		c := check(path, 1, fall)
		c.Interval = config.Duration(2 * time.Millisecond)
		return c
	}

	backends := make([]*balancer.Backend, 0, iterations)
	paths := make([]string, 0, iterations)
	for i := 0; i < iterations; i++ {
		path := fmt.Sprintf("/hc%d", i)
		f.setPath(path, http.StatusInternalServerError)
		b := f.backend(t, fmt.Sprintf("svc%d", i))
		backends = append(backends, b)
		paths = append(paths, path)

		h.c.Sync([]Target{{b, fast(path, 1)}})
		eventually(t, wait, "backend marked down by its probe", func() bool { return !b.ActiveHealthy() })

		// Settings change (restart, keeps verdict), then immediately removal.
		h.c.Sync([]Target{{b, fast(path, 2)}})
		h.c.Sync(nil)
		eventually(t, wait, "removed backend reset to up", b.ActiveHealthy)
	}

	// Let every straggling goroutine finish, then require silence.
	time.Sleep(100 * time.Millisecond)
	before := make([]int, len(paths))
	for i, p := range paths {
		before[i] = f.hitsOn(p)
	}
	time.Sleep(100 * time.Millisecond)
	for i, p := range paths {
		if got := f.hitsOn(p); got != before[i] {
			t.Errorf("%s: %d probe requests arrived after the backend was removed", p, got-before[i])
		}
		if !backends[i].ActiveHealthy() {
			t.Errorf("backend %d ended DOWN: a late verdict overwrote the reset to up", i)
		}
	}
}
