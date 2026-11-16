package proxy

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/metrics"
)

// Metrics and access-log wiring (Phase 6, architecture.md section 9 and
// section 11 row 19). Gathered through the registry, not testutil.ToFloat64,
// because the vectors are not reachable from outside package metrics.

// ---- registry reading -------------------------------------------------------

type igSeries struct {
	labels map[string]string
	val    float64 // counter or gauge value
	count  uint64  // histogram sample count
}

func igGather(t *testing.T, reg *prometheus.Registry, name string) []igSeries {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var out []igSeries
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			s := igSeries{labels: map[string]string{}}
			for _, lp := range m.GetLabel() {
				s.labels[lp.GetName()] = lp.GetValue()
			}
			switch {
			case m.GetHistogram() != nil:
				s.count = m.GetHistogram().GetSampleCount()
			case m.GetCounter() != nil:
				s.val = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				s.val = m.GetGauge().GetValue()
			}
			out = append(out, s)
		}
	}
	return out
}

// igVal returns the value of the series of name whose labels equal want
// exactly (0, false if there is none).
func igVal(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) (float64, bool) {
	t.Helper()
	for _, s := range igGather(t, reg, name) {
		if len(s.labels) != len(want) {
			continue
		}
		ok := true
		for k, v := range want {
			if s.labels[k] != v {
				ok = false
			}
		}
		if ok {
			return s.val, true
		}
	}
	return 0, false
}

// igNum is igVal with "absent" read as 0 (a counter that never fired).
func igNum(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) float64 {
	v, _ := igVal(t, reg, name, want)
	return v
}

func igHistCount(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) uint64 {
	t.Helper()
	for _, s := range igGather(t, reg, name) {
		match := len(s.labels) == len(want)
		for k, v := range want {
			if s.labels[k] != v {
				match = false
			}
		}
		if match {
			return s.count
		}
	}
	return 0
}

func igSum(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	var sum float64
	for _, s := range igGather(t, reg, name) {
		sum += s.val
	}
	return sum
}

func igHistTotal(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	var n uint64
	for _, s := range igGather(t, reg, name) {
		n += s.count
	}
	return n
}

// igBackendSeries returns the series of name for upstream whose backend label
// mentions hp ("host:port"). The docs do not fix whether the label is the
// Registry ID ("up|http://host:port") or the bare URL, so match by substring.
func igBackendSeries(t *testing.T, reg *prometheus.Registry, name, upstream, hp string) []igSeries {
	t.Helper()
	var out []igSeries
	for _, s := range igGather(t, reg, name) {
		if s.labels["upstream"] == upstream && strings.Contains(s.labels["backend"], hp) {
			out = append(out, s)
		}
	}
	return out
}

func igBackendVal(t *testing.T, reg *prometheus.Registry, name, upstream, hp string) (float64, bool) {
	t.Helper()
	ss := igBackendSeries(t, reg, name, upstream, hp)
	if len(ss) == 0 {
		return 0, false
	}
	return ss[0].val, true
}

// igAnyBackendSeries counts every tollgate_backend_* series of a backend.
func igAnyBackendSeries(t *testing.T, reg *prometheus.Registry, upstream, hp string) int {
	n := 0
	for _, name := range []string{"tollgate_backend_up", "tollgate_backend_inflight", "tollgate_backend_ejections_total"} {
		n += len(igBackendSeries(t, reg, name, upstream, hp))
	}
	return n
}

func newMetrics(t *testing.T) (*prometheus.Registry, *metrics.Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return reg, metrics.New(reg)
}

func lbl(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// closedURL is a base URL nothing listens on (connection refused).
func closedURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return "http://" + addr
}

// ---- per-request counters ---------------------------------------------------

type metricsFixture struct {
	reg *prometheus.Registry
	h   *harness
	srv *httptest.Server
}

// metricsGateway serves one route per outcome we want to count.
func metricsGateway(t *testing.T, opts Options) (*metricsFixture, map[string]*ctrlBackend) {
	t.Helper()
	reg, m := newMetrics(t)
	good := newCtrlBackend(t, "good")
	bad := newCtrlBackend(t, "bad")
	bad.status.Store(500)
	slow := newCtrlBackend(t, "slow")
	slow.delay.Store(int64(3 * time.Second))
	sick := newCtrlBackend(t, "sick")
	sick.healthy.Store(false)

	slowRoute := rt("slow", "", "/slow", "slow")
	slowRoute.Timeouts = config.Timeouts{ResponseHeader: ms(100)}
	keyed := authRoute("keyed", "/keyed", "good", config.AuthAPIKey, nil)
	limited := authRoute("limited", "/limited", "good", config.AuthNone, rl(0.001, 1))

	cfg := withAuth(cfgOf(1,
		[]config.Route{
			rt("ok", "", "/ok", "good"), rt("err", "", "/err", "bad"), rt("dead", "", "/dead", "dead"),
			slowRoute, keyed, limited, rt("sick", "", "/sick", "sick"),
		},
		upx("good", config.AlgorithmRoundRobin, nil, nil, good.url()),
		upx("bad", config.AlgorithmRoundRobin, nil, nil, bad.url()),
		upx("dead", config.AlgorithmRoundRobin, nil, nil, closedURL(t)),
		upx("slow", config.AlgorithmRoundRobin, nil, nil, slow.url()),
		upx("sick", config.AlgorithmRoundRobin, hcFast(), nil, sick.url()),
	), config.AuthSettings{APIKeys: keysFor("alice", "alice-key")})

	opts.Metrics = m
	h, srv := startGatewayWith(t, opts, cfg)
	return &metricsFixture{reg: reg, h: h, srv: srv},
		map[string]*ctrlBackend{"good": good, "bad": bad, "slow": slow, "sick": sick}
}

func TestMetricsRequestsCountedOncePerRequest(t *testing.T) {
	f, _ := metricsGateway(t, Options{Getenv: testGetenv})
	cl := newClient(t)
	// The sick upstream's only backend must be marked down before its 503 case.
	waitFor(t, 5*time.Second, "503 from the upstream whose only backend is down", func() bool {
		return getCode(t, cl, f.srv.URL+"/sick") == http.StatusServiceUnavailable
	})

	tests := []struct {
		name            string
		path            string
		hdr             map[string]string
		code            int
		route, upstream string
		rateLimited     string // expected tollgate_ratelimited_total reason, "" = none
		authFail        string // expected tollgate_auth_failures_total reason, "" = none
	}{
		{name: "200", path: "/ok", code: 200, route: "ok", upstream: "good"},
		{name: "upstream 500 is passed through", path: "/err", code: 500, route: "err", upstream: "bad"},
		{name: "502 dead backend", path: "/dead", code: 502, route: "dead", upstream: "dead"},
		{name: "504 response header timeout", path: "/slow", code: 504, route: "slow", upstream: "slow"},
		{name: "401 missing key", path: "/keyed", code: 401, route: "keyed", upstream: "good", authFail: "missing"},
		{name: "401 invalid key", path: "/keyed", hdr: map[string]string{"X-API-Key": "nope"}, code: 401, route: "keyed", upstream: "good", authFail: "invalid"},
		{name: "200 with a valid key", path: "/keyed", hdr: map[string]string{"X-API-Key": "alice-key"}, code: 200, route: "keyed", upstream: "good"},
		{name: "limited route: first request is admitted", path: "/limited", code: 200, route: "limited", upstream: "good"},
		{name: "limited route: second request gets 429", path: "/limited", code: 429, route: "limited", upstream: "good", rateLimited: "limit"},
		{name: "404 no route", path: "/nowhere", code: 404, route: "none", upstream: "none"},
		{name: "400 unsafe path", path: "/ok/../x", code: 400, route: "none", upstream: "none"},
		{name: "503 no healthy backend", path: "/sick", code: 503, route: "sick", upstream: "sick"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			series := lbl("route", tc.route, "upstream", tc.upstream, "code", itoa(tc.code))
			dur := lbl("route", tc.route, "upstream", tc.upstream)
			totalBefore := igSum(t, f.reg, "tollgate_requests_total")
			serBefore := igNum(t, f.reg, "tollgate_requests_total", series)
			durBefore := igHistCount(t, f.reg, "tollgate_request_duration_seconds", dur)
			durTotalBefore := igHistTotal(t, f.reg, "tollgate_request_duration_seconds")
			rlBefore := igSum(t, f.reg, "tollgate_ratelimited_total")
			afBefore := igSum(t, f.reg, "tollgate_auth_failures_total")

			req, _ := http.NewRequest(http.MethodGet, f.srv.URL+tc.path, nil)
			for k, v := range tc.hdr {
				req.Header.Set(k, v)
			}
			resp, err := cl.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != tc.code {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.code)
			}

			// The metric may be recorded just after the response is written.
			waitFor(t, 2*time.Second, "requests_total to move", func() bool {
				return igNum(t, f.reg, "tollgate_requests_total", series) > serBefore
			})
			consistently(t, 50*time.Millisecond, "exactly one count", func() bool {
				return igNum(t, f.reg, "tollgate_requests_total", series) == serBefore+1 &&
					igSum(t, f.reg, "tollgate_requests_total") == totalBefore+1
			})
			waitFor(t, time.Second, "duration histogram count to move by one", func() bool {
				return igHistCount(t, f.reg, "tollgate_request_duration_seconds", dur) == durBefore+1 &&
					igHistTotal(t, f.reg, "tollgate_request_duration_seconds") == durTotalBefore+1
			})

			wantRL, wantAF := 0.0, 0.0
			if tc.rateLimited != "" {
				wantRL = 1
				if got := igNum(t, f.reg, "tollgate_ratelimited_total", lbl("route", tc.route, "reason", tc.rateLimited)); got != 1 {
					t.Errorf("ratelimited_total{route=%s,reason=%s} = %v, want 1", tc.route, tc.rateLimited, got)
				}
			}
			if tc.authFail != "" {
				wantAF = 1
				v := igNum(t, f.reg, "tollgate_auth_failures_total", lbl("route", tc.route, "reason", tc.authFail))
				if v < 1 {
					t.Errorf("auth_failures_total{route=%s,reason=%s} = %v, want >= 1", tc.route, tc.authFail, v)
				}
			}
			if d := igSum(t, f.reg, "tollgate_ratelimited_total") - rlBefore; d != wantRL {
				t.Errorf("ratelimited_total moved by %v, want %v", d, wantRL)
			}
			if d := igSum(t, f.reg, "tollgate_auth_failures_total") - afBefore; d != wantAF {
				t.Errorf("auth_failures_total moved by %v, want %v", d, wantAF)
			}
		})
	}
}

// Reasons of tollgate_ratelimited_total: limit / capacity / auth_throttle.
func TestMetricsRateLimitReasons(t *testing.T) {
	t.Run("capacity vs limit", func(t *testing.T) {
		reg, m := newMetrics(t)
		clk := newFakeClock() // frozen: no bucket ever becomes idle, so none is evicted
		u := newRecUp(t)
		h := startRunWith(t, Options{Now: clk.Now, LimiterMaxKeys: 64, Metrics: m})
		h.apply(t, cfgOf(1, []config.Route{authRoute("pub", "/pub", "u", config.AuthNone, rl(0.01, 1))}, up("u", u.srv.URL)))

		denied := 0
		for i := 0; i < 300; i++ {
			if serveAs(h.g, ipN(i), "/pub").Code == http.StatusTooManyRequests {
				denied++
			}
		}
		if denied == 0 {
			t.Fatal("test bug: no request hit the key-table capacity")
		}
		waitFor(t, 2*time.Second, "capacity denials to be counted", func() bool {
			return igNum(t, reg, "tollgate_ratelimited_total", lbl("route", "pub", "reason", "capacity")) == float64(denied)
		})
		if v, ok := igVal(t, reg, "tollgate_ratelimited_total", lbl("route", "pub", "reason", "limit")); ok && v != 0 {
			t.Errorf("reason=limit = %v after only first-use keys were denied, want 0", v)
		}

		// A key that is in the table and has no tokens left: an ordinary limit.
		if code := serveAs(h.g, ipN(0), "/pub").Code; code != http.StatusTooManyRequests {
			t.Fatalf("second request from a known key: %d, want 429", code)
		}
		waitFor(t, 2*time.Second, "limit denial to be counted", func() bool {
			return igNum(t, reg, "tollgate_ratelimited_total", lbl("route", "pub", "reason", "limit")) == 1
		})
		if got := igNum(t, reg, "tollgate_ratelimited_total", lbl("route", "pub", "reason", "capacity")); got != float64(denied) {
			t.Errorf("reason=capacity = %v, want %d unchanged", got, denied)
		}
	})

	t.Run("failed-auth throttle and auth failure reasons", func(t *testing.T) {
		reg, m := newMetrics(t)
		clk := newFakeClock()
		u := newRecUp(t)
		h := startRunWith(t, Options{Now: clk.Now, Getenv: testGetenv, Metrics: m})
		h.apply(t, withAuth(
			cfgOf(1, []config.Route{authRoute("keyed", "/", "u", config.AuthAPIKey, nil)}, up("u", u.srv.URL)),
			config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))

		ip := "203.0.113.5:1111"
		for i := 0; i < 10; i++ {
			if code := serveAs(h.g, ip, "/x", "X-API-Key", "wrong").Code; code != 401 {
				t.Fatalf("bad key #%d: %d, want 401", i+1, code)
			}
		}
		if code := serveAs(h.g, ip, "/x", "X-API-Key", "wrong").Code; code != 429 {
			t.Fatalf("11th bad key: %d, want 429", code)
		}
		if code := serveAs(h.g, "203.0.113.6:1", "/x").Code; code != 401 {
			t.Fatalf("missing key: %d, want 401", code)
		}
		waitFor(t, 2*time.Second, "reasons to be counted", func() bool {
			return igNum(t, reg, "tollgate_ratelimited_total", lbl("route", "keyed", "reason", "auth_throttle")) == 1 &&
				igNum(t, reg, "tollgate_auth_failures_total", lbl("route", "keyed", "reason", "missing")) == 1
		})
		if got := igNum(t, reg, "tollgate_auth_failures_total", lbl("route", "keyed", "reason", "invalid")); got != 10 {
			t.Errorf("auth_failures_total{reason=invalid} = %v, want 10 (a throttled request is never verified)", got)
		}
		if got := igNum(t, reg, "tollgate_ratelimited_total", lbl("route", "keyed", "reason", "limit")); got != 0 {
			t.Errorf("reason=limit = %v, want 0", got)
		}
		if got := igNum(t, reg, "tollgate_requests_total", lbl("route", "keyed", "upstream", "u", "code", "429")); got != 1 {
			t.Errorf("requests_total code 429 = %v, want 1", got)
		}
	})
}

// ---- backend gauges and counters --------------------------------------------

func TestMetricsBackendUpFollowsActiveChecks(t *testing.T) {
	reg, m := newMetrics(t)
	a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
	startGatewayWith(t, Options{Metrics: m}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, hcFast(), nil, a.url(), b.url())))

	upVal := func(x *ctrlBackend) (float64, bool) {
		return igBackendVal(t, reg, "tollgate_backend_up", "u", hostport(x.srv))
	}
	waitUp := func(x *ctrlBackend, want float64) {
		t.Helper()
		waitFor(t, 5*time.Second, "backend_up="+itoa(int(want)), func() bool {
			v, ok := upVal(x)
			return ok && v == want
		})
	}
	waitUp(a, 1)
	waitUp(b, 1)
	b.setDown()
	waitUp(b, 0)
	waitUp(a, 1)
	b.setUp()
	waitUp(b, 1)
}

func TestMetricsEjectionsCountedOncePerEjection(t *testing.T) {
	reg, m := newMetrics(t)
	a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
	a.status.Store(500)
	passive := &config.PassiveHealth{ConsecutiveFailures: 3, Cooloff: config.Duration(time.Minute)}
	_, srv := startGatewayWith(t, Options{Metrics: m}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
		upx("u", config.AlgorithmRoundRobin, nil, passive, a.url(), b.url())))
	cl := newClient(t)

	burst(t, cl, srv.URL, "/x", 30)
	waitFor(t, 2*time.Second, "one ejection of A", func() bool {
		v, _ := igBackendVal(t, reg, "tollgate_backend_ejections_total", "u", hostport(a.srv))
		return v == 1
	})
	burst(t, cl, srv.URL, "/x", 30) // A is out for a minute: nothing more to eject
	consistently(t, 100*time.Millisecond, "still exactly one ejection", func() bool {
		v, _ := igBackendVal(t, reg, "tollgate_backend_ejections_total", "u", hostport(a.srv))
		return v == 1
	})
	if v, ok := igBackendVal(t, reg, "tollgate_backend_ejections_total", "u", hostport(b.srv)); ok && v != 0 {
		t.Errorf("healthy B has ejections_total = %v", v)
	}
}

func TestMetricsInFlightGaugeReflectsHeldRequests(t *testing.T) {
	reg, m := newMetrics(t)
	b := newCtrlBackend(t, "B")
	hold := make(chan struct{})
	b.hold.Store(&hold)
	var once sync.Once
	release := func() { once.Do(func() { close(hold) }) }
	defer release()
	_, srv := startGatewayWith(t, Options{Metrics: m}, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", b.url())))

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(srv.URL + "/hold")
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
	}
	inflight := func() float64 {
		v, _ := igBackendVal(t, reg, "tollgate_backend_inflight", "u", hostport(b.srv))
		return v
	}
	// The gauge may be sampled periodically, so allow a couple of seconds.
	waitFor(t, 2*time.Second, "inflight gauge = 3 with three parked requests", func() bool { return inflight() == 3 })
	release()
	wg.Wait()
	waitFor(t, 2*time.Second, "inflight gauge back to 0", func() bool { return inflight() == 0 })
}

// Section 11 row 19 and review note P2-6: pruned backends lose ALL their
// series and the checker's reset-to-up callback must not bring them back.
func TestMetricsBackendSeriesRemovedWithBackend(t *testing.T) {
	tests := []struct {
		name    string
		hc      *config.HealthCheck
		passive *config.PassiveHealth
		// makeDown puts B in the state the case is about (B is the one removed).
		makeDown func(t *testing.T, b *ctrlBackend, srv *httptest.Server)
		// settled reports that B's series exist in the state we want to remove.
		settled func(t *testing.T, reg *prometheus.Registry, hp string) bool
	}{
		{
			name: "backend that was DOWN by active checks",
			hc:   hcFast(),
			makeDown: func(t *testing.T, b *ctrlBackend, srv *httptest.Server) {
				b.healthy.Store(false)
			},
			settled: func(t *testing.T, reg *prometheus.Registry, hp string) bool {
				v, ok := igBackendVal(t, reg, "tollgate_backend_up", "u", hp)
				return ok && v == 0
			},
		},
		{
			name:    "backend that was ejected passively",
			passive: &config.PassiveHealth{ConsecutiveFailures: 2, Cooloff: config.Duration(time.Minute)},
			makeDown: func(t *testing.T, b *ctrlBackend, srv *httptest.Server) {
				b.status.Store(500)
				burst(t, newClient(t), srv.URL, "/x", 20)
			},
			settled: func(t *testing.T, reg *prometheus.Registry, hp string) bool {
				e, _ := igBackendVal(t, reg, "tollgate_backend_ejections_total", "u", hp)
				_, up := igBackendVal(t, reg, "tollgate_backend_up", "u", hp)
				return e == 1 && up
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg, m := newMetrics(t)
			a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
			mk := func(version int64, backends ...string) *config.Config {
				return cfgOf(version, []config.Route{rt("r", "", "/", "u")},
					upx("u", config.AlgorithmRoundRobin, tc.hc, tc.passive, backends...))
			}
			h, srv := startGatewayWith(t, Options{Metrics: m}, mk(1, a.url(), b.url()))
			hpA, hpB := hostport(a.srv), hostport(b.srv)

			tc.makeDown(t, b, srv)
			waitFor(t, 5*time.Second, "B's series to reach the state under test", func() bool { return tc.settled(t, reg, hpB) })
			if n := len(igBackendSeries(t, reg, "tollgate_backend_up", "u", hpA)); n != 1 {
				t.Fatalf("A has %d backend_up series before the swap, want 1", n)
			}

			h.apply(t, mk(2, a.url())) // B removed by a config change

			waitFor(t, 2*time.Second, "every tollgate_backend_* series of B to disappear", func() bool {
				return igAnyBackendSeries(t, reg, "u", hpB) == 0
			})
			consistently(t, 500*time.Millisecond, "B's series must not come back", func() bool {
				return igAnyBackendSeries(t, reg, "u", hpB) == 0
			})
			if n := len(igBackendSeries(t, reg, "tollgate_backend_up", "u", hpA)); n != 1 {
				t.Errorf("A has %d backend_up series after the swap, want 1 (only B is forgotten)", n)
			}
		})
	}
}

// ---- config gauges ----------------------------------------------------------

func TestMetricsConfigVersionAndApplyFailures(t *testing.T) {
	reg, m := newMetrics(t)
	a := echoUpstream(t, "A")
	h := startRunWith(t, Options{Metrics: m})
	good := func(v int64) *config.Config {
		return cfgOf(v, []config.Route{rt("r", "", "/", "u")}, up("u", a.URL))
	}
	version := func() float64 { return igNum(t, reg, "tollgate_config_version", lbl()) }
	failures := func() float64 { return igNum(t, reg, "tollgate_config_apply_failures_total", lbl()) }

	for _, v := range []int64{1, 2} {
		h.apply(t, good(v))
		waitFor(t, 2*time.Second, "config_version == Version()", func() bool { return version() == float64(h.g.Version()) && version() == float64(v) })
	}

	h.src.push(cfgOf(3, []config.Route{rt("r", "", "/", "ghost")}, up("u", a.URL)))
	waitFor(t, 2*time.Second, "apply failure to be counted", func() bool { return failures() == 1 })
	consistently(t, 100*time.Millisecond, "one failure, version unchanged", func() bool {
		return failures() == 1 && version() == 2 && h.g.Version() == 2
	})

	h.apply(t, good(4))
	waitFor(t, 2*time.Second, "config_version == 4", func() bool { return version() == 4 })
	if failures() != 1 {
		t.Errorf("apply failures = %v after a good config, want still 1", failures())
	}
}

// ---- label hygiene -----------------------------------------------------------

func TestMetricsLabelHygiene(t *testing.T) {
	reg, m := newMetrics(t)
	u := newRecUp(t)
	opts := Options{Metrics: m, Getenv: testGetenv}
	_, srv := startGatewayWith(t, opts, withAuth(
		cfgOf(1, []config.Route{authRoute("api", "/p", "u", config.AuthAPIKey, nil)}, up("u", u.srv.URL)),
		config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))
	cl := newClient(t)
	do := func(path string) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		req.Header.Set("X-API-Key", "alice-key")
		resp, err := cl.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
	}
	count := func(name string) int { return len(igGather(t, reg, name)) }

	do("/p/warmup")
	waitFor(t, 2*time.Second, "warm-up to be counted", func() bool { return igSum(t, reg, "tollgate_requests_total") == 1 })
	reqSeries, durSeries := count("tollgate_requests_total"), count("tollgate_request_duration_seconds")

	for i := 0; i < 200; i++ {
		do("/p/item-" + itoa(i) + "/detail?q=secretquery" + itoa(i))
	}
	waitFor(t, 3*time.Second, "all requests to be counted", func() bool { return igSum(t, reg, "tollgate_requests_total") == 201 })
	if got := count("tollgate_requests_total"); got != reqSeries {
		t.Errorf("requests_total has %d series after 200 distinct paths, want %d: paths must not be labels", got, reqSeries)
	}
	if got := count("tollgate_request_duration_seconds"); got != durSeries {
		t.Errorf("duration histogram has %d series, want %d", got, durSeries)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		for _, mt := range mf.GetMetric() {
			for _, lp := range mt.GetLabel() {
				v := lp.GetValue()
				for _, bad := range []string{"alice", "item-", "secretquery", "/p/", "?"} {
					if strings.Contains(v, bad) {
						t.Errorf("metric %s label %s=%q contains %q (client IDs, paths and queries are not label values)",
							mf.GetName(), lp.GetName(), v, bad)
					}
				}
			}
		}
	}
}

func TestMetricsNilStillWorks(t *testing.T) {
	var logs igBuf
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	for name, opts := range map[string]Options{
		"no metrics, no access log": {},
		"no metrics, access log":    {AccessLogger: logger},
	} {
		t.Run(name, func(t *testing.T) {
			opts.Getenv = testGetenv
			u := newRecUp(t)
			h := startRunWith(t, opts)
			h.apply(t, withAuth(cfgOf(1, []config.Route{authRoute("r", "/", "u", config.AuthAPIKey, rl(100, 100))}, up("u", u.srv.URL)),
				config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))
			if code := serveAs(h.g, defaultRemote, "/x", "X-API-Key", "alice-key").Code; code != 200 {
				t.Errorf("200 case: %d", code)
			}
			if code := serveAs(h.g, defaultRemote, "/x").Code; code != 401 {
				t.Errorf("401 case: %d", code)
			}
			if code := serveAs(h.g, defaultRemote, "/a/../b").Code; code != 400 {
				t.Errorf("400 case: %d", code)
			}
		})
	}
}

// ---- access log ----------------------------------------------------------------

type accessFixture struct {
	logs *igBuf
	h    *harness
	srv  *httptest.Server
	up   *recUp
}

// accessGateway has an api_key route /api (limited to 2 requests, no
// refill worth mentioning), a public route /pub, and a dead upstream route.
func accessGateway(t *testing.T) *accessFixture {
	t.Helper()
	logs := &igBuf{}
	all := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	u := newRecUp(t)
	cfg := withAuth(cfgOf(7, []config.Route{
		authRoute("api", "/api", "u", config.AuthAPIKey, rl(0.001, 2)),
		authRoute("pub", "/pub", "u", config.AuthNone, nil),
		authRoute("dead", "/dead", "dead", config.AuthNone, nil),
	}, up("u", u.srv.URL), up("dead", closedURL(t))), config.AuthSettings{APIKeys: keysFor("alice", "alice-key-SECRET789")})
	// Both loggers write to the same buffer, so "never in the log output"
	// covers the gateway's own logs as well as the access lines.
	h, srv := startGatewayWith(t, Options{Logger: all, AccessLogger: all, Getenv: testGetenv}, cfg)
	return &accessFixture{logs: logs, h: h, srv: srv, up: u}
}

func (f *accessFixture) waitLines(t *testing.T, n int) []map[string]any {
	t.Helper()
	waitFor(t, 2*time.Second, "access log lines", func() bool { return len(f.logs.requestLines(t)) >= n })
	return f.logs.requestLines(t)
}

func TestAccessLogOneLinePerRequestWithRightFields(t *testing.T) {
	f := accessGateway(t)
	cl := newClient(t)
	host := f.srv.Listener.Addr().String()
	upHP := hostport(f.up.srv)

	tests := []struct {
		name     string
		path     string // with query
		hdr      map[string]string
		status   int
		route    string
		upstream string
		proxied  bool
		clientID string
	}{
		{"authenticated and proxied", "/api/items?token=SECRET123", map[string]string{
			"X-API-Key": "alice-key-SECRET789", "Authorization": "Bearer SECRETBEARER456", "Cookie": "session=SECRETCOOKIE"},
			200, "api", "u", true, "alice"},
		{"wrong key", "/api/items?token=SECRET123", map[string]string{"X-API-Key": "SECRETWRONGKEY"}, 401, "api", "u", false, ""},
		{"public route", "/pub/a", nil, 200, "pub", "u", true, ""},
		{"no route", "/nowhere?token=SECRET123", nil, 404, "", "", false, ""},
		{"unsafe path", "/pub/../x", nil, 400, "", "", false, ""},
		{"dead backend", "/dead/x", nil, 502, "dead", "dead", true, ""},
		{"second valid request", "/api/items", map[string]string{"X-API-Key": "alice-key-SECRET789"}, 200, "api", "u", true, "alice"},
		{"third valid request is limited", "/api/items", map[string]string{"X-API-Key": "alice-key-SECRET789"}, 429, "api", "u", false, ""},
	}
	// Note the table is ordered: the api route admits two valid requests.
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, f.srv.URL+tc.path, nil)
			for k, v := range tc.hdr {
				req.Header.Set(k, v)
			}
			resp, err := cl.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
			}

			lines := f.waitLines(t, i+1)
			if len(lines) != i+1 {
				t.Fatalf("%d access lines after %d requests, want exactly one per request", len(lines), i+1)
			}
			ln := lines[i]
			wantPath := tc.path
			if q := strings.IndexByte(wantPath, '?'); q >= 0 {
				wantPath = wantPath[:q]
			}
			str := func(k string) string { s, _ := ln[k].(string); return s }
			num := func(k string) float64 { v, _ := ln[k].(float64); return v }

			if rid := resp.Header.Get("X-Request-ID"); rid == "" || str("request_id") != rid {
				t.Errorf("request_id = %q, response header %q: want equal and non-empty", str("request_id"), rid)
			}
			checks := []struct{ field, got, want string }{
				{"method", str("method"), "GET"},
				{"host", str("host"), host},
				{"path", str("path"), wantPath},
				{"client_id", str("client_id"), tc.clientID},
				{"remote_ip", str("remote_ip"), "127.0.0.1"},
			}
			for _, c := range checks {
				if c.got != c.want {
					t.Errorf("%s = %q, want %q", c.field, c.got, c.want)
				}
			}
			// Unmatched requests may log the route as "" or "none".
			if tc.route == "" {
				if r := str("route"); r != "" && r != "none" {
					t.Errorf("route = %q, want \"\" or \"none\"", r)
				}
				if u := str("upstream"); u != "" && u != "none" {
					t.Errorf("upstream = %q, want \"\" or \"none\"", u)
				}
			} else if str("route") != tc.route || str("upstream") != tc.upstream {
				t.Errorf("route/upstream = %q/%q, want %q/%q", str("route"), str("upstream"), tc.route, tc.upstream)
			}
			if tc.proxied {
				if b := str("backend"); b == "" {
					t.Error("backend is empty for a request that was sent to a backend")
				} else if tc.upstream == "u" && !strings.Contains(b, upHP) {
					t.Errorf("backend = %q, want it to identify %s", b, upHP)
				}
			} else if b := str("backend"); b != "" {
				t.Errorf("backend = %q for a request rejected before any backend, want empty", b)
			}
			if int(num("status")) != tc.status {
				t.Errorf("status = %v, want %d", num("status"), tc.status)
			}
			if num("duration_ms") <= 0 {
				t.Errorf("duration_ms = %v, want > 0", num("duration_ms"))
			}
			if int(num("bytes_out")) != len(body) {
				t.Errorf("bytes_out = %v, want %d (the body length)", num("bytes_out"), len(body))
			}
			if int64(num("config_version")) != f.h.g.Version() {
				t.Errorf("config_version = %v, want %d", num("config_version"), f.h.g.Version())
			}
		})
	}

	consistently(t, 100*time.Millisecond, "no extra lines", func() bool { return len(f.logs.requestLines(t)) == len(tests) })

	out := f.logs.String()
	for _, secret := range []string{"SECRET123", "SECRETBEARER456", "SECRETCOOKIE", "SECRET789", "SECRETWRONGKEY"} {
		if strings.Contains(out, secret) {
			t.Errorf("log output contains %q: credentials and query strings must never be logged", secret)
		}
	}
}

// 499 (client went away) and aborted responses still produce one line each.
func TestAccessLogCanceledAndAbortedRequests(t *testing.T) {
	t.Run("client cancels: 499", func(t *testing.T) {
		logs := &igBuf{}
		entered := make(chan struct{}, 1)
		upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- struct{}{}
			<-r.Context().Done()
		}))
		defer upSrv.Close()
		l := slog.New(slog.NewJSONHandler(logs, nil))
		_, srv := startGatewayWith(t, Options{AccessLogger: l}, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upSrv.URL)))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/x", nil)
		errc := make(chan error, 1)
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
			errc <- err
		}()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("request never reached the upstream")
		}
		cancel()
		if err := <-errc; err != nil {
			t.Logf("server returned: %v", err)
		}

		waitFor(t, 3*time.Second, "one access line", func() bool { return len(logs.requestLines(t)) >= 1 })
		consistently(t, 100*time.Millisecond, "exactly one line", func() bool { return len(logs.requestLines(t)) == 1 })
		if st, _ := logs.requestLines(t)[0]["status"].(float64); int(st) != 499 {
			t.Errorf("status = %v, want 499", st)
		}
	})

	t.Run("upstream dies mid-body: aborted response", func(t *testing.T) {
		logs := &igBuf{}
		upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(200)
			_, _ = w.Write([]byte("partial"))
			w.(http.Flusher).Flush()
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}))
		defer upSrv.Close()
		l := slog.New(slog.NewJSONHandler(logs, nil))
		_, srv := startGatewayWith(t, Options{AccessLogger: l}, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upSrv.URL)))

		resp, err := newClient(t).Get(srv.URL + "/x")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body) // fails: the response was cut short
			_ = resp.Body.Close()
		}
		waitFor(t, 3*time.Second, "one access line", func() bool { return len(logs.requestLines(t)) >= 1 })
		consistently(t, 100*time.Millisecond, "exactly one line", func() bool { return len(logs.requestLines(t)) == 1 })
	})
}

func TestAccessLogNilLoggerWritesNoRequestLines(t *testing.T) {
	var logs igBuf
	l := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	a := echoUpstream(t, "A")
	_, srv := startGatewayWith(t, Options{Logger: l /* AccessLogger nil */}, cfgOf(1, []config.Route{rt("r", "", "/ok", "u")}, up("u", a.URL)))
	cl := newClient(t)
	for _, p := range []string{"/ok", "/missing", "/ok/../x"} {
		_ = getCode(t, cl, srv.URL+p)
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(logs.requestLines(t)); n != 0 {
		t.Errorf("%d \"request\" lines with AccessLogger nil, want 0", n)
	}
}
