package metrics

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// exercise touches every series so vec families appear in Gather.
func exercise(m *Metrics) {
	m.ObserveRequest("r", "u", 200, time.Millisecond)
	m.RateLimited("r", "limit")
	m.AuthFailed("r", "missing")
	m.SetBackendUp("u", "b", true)
	m.SetBackendInFlight("u", "b", 1)
	m.BackendEjected("u", "b")
	m.SetConfigVersion(1)
	m.ConfigApplyFailed()
	m.SetConfigStreamConnected(true)
}

func TestMetricCatalog(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := New(reg)
	exercise(m)

	tests := []struct {
		name, typ string
		labels    []string
	}{
		{"tollgate_requests_total", "COUNTER", []string{"code", "route", "upstream"}},
		{"tollgate_request_duration_seconds", "HISTOGRAM", []string{"route", "upstream"}},
		{"tollgate_ratelimited_total", "COUNTER", []string{"reason", "route"}},
		{"tollgate_auth_failures_total", "COUNTER", []string{"reason", "route"}},
		{"tollgate_backend_up", "GAUGE", []string{"backend", "upstream"}},
		{"tollgate_backend_inflight", "GAUGE", []string{"backend", "upstream"}},
		{"tollgate_backend_ejections_total", "COUNTER", []string{"backend", "upstream"}},
		{"tollgate_config_version", "GAUGE", nil},
		{"tollgate_config_apply_failures_total", "COUNTER", nil},
		{"tollgate_config_stream_connected", "GAUGE", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer guard(t)
			typ, help, ls, ok := familyType(t, reg, tc.name)
			if !ok {
				t.Fatalf("family %s missing", tc.name)
			}
			if typ != tc.typ {
				t.Errorf("type = %s, want %s", typ, tc.typ)
			}
			if strings.TrimSpace(help) == "" {
				t.Errorf("empty HELP")
			}
			sort.Strings(ls)
			if fmt.Sprint(ls) != fmt.Sprint(tc.labels) {
				t.Errorf("labels = %v, want %v", ls, tc.labels)
			}
		})
	}
}

func TestNewRegistration(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	New(reg) // must not panic on a fresh registry

	func() {
		defer func() {
			if recover() == nil {
				t.Error("second New on the same registry must panic (duplicate registration)")
			}
		}()
		New(reg)
	}()

	// A different registry is fine.
	New(prometheus.NewRegistry())

	// Never the global default registry.
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), "tollgate_") {
			t.Errorf("default registry has %s", mf.GetName())
		}
	}
}

func TestObserveRequest(t *testing.T) {
	tests := []struct {
		name            string
		route, upstream string
		status          int
		wantCode        string
		wantRoute       string
		wantUpstream    string
	}{
		{"ok", "api", "backend", 200, "200", "api", "backend"},
		{"rate limited", "api", "backend", 429, "429", "api", "backend"},
		{"bad gateway", "api", "backend", 502, "502", "api", "backend"},
		{"client closed", "api", "backend", 499, "499", "api", "backend"},
		{"unmatched route", RouteNone, RouteNone, 404, "404", RouteNone, RouteNone},
		{"empty route and upstream become none", "", "", 400, "400", RouteNone, RouteNone},
		{"empty upstream only", "api", "", 401, "401", "api", RouteNone},
		{"status zero recorded as given", "api", "backend", 0, "0", "api", "backend"},
		{"status 99", "api", "backend", 99, "99", "api", "backend"},
		{"status 1000", "api", "backend", 1000, "1000", "api", "backend"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer guard(t)
			reg := prometheus.NewRegistry()
			m := New(reg)
			m.ObserveRequest(tc.route, tc.upstream, tc.status, 250*time.Millisecond)
			m.ObserveRequest(tc.route, tc.upstream, tc.status, 250*time.Millisecond)

			cl := labels{"route": tc.wantRoute, "upstream": tc.wantUpstream, "code": tc.wantCode}
			if got := value(t, reg, "tollgate_requests_total", cl); got != 2 {
				t.Errorf("requests_total = %v, want 2", got)
			}
			ok, sum, count := find(t, reg, "tollgate_request_duration_seconds",
				labels{"route": tc.wantRoute, "upstream": tc.wantUpstream})
			if !ok || count != 2 {
				t.Fatalf("histogram found=%v count=%d, want 2", ok, count)
			}
			// 2 x 250ms recorded in SECONDS, not ms or ns.
			if sum < 0.499 || sum > 0.501 {
				t.Errorf("histogram sum = %v, want 0.5 seconds", sum)
			}
		})
	}
}

func TestRequestLabelsAreIndependent(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.ObserveRequest("a", "u", 200, time.Millisecond)
	m.ObserveRequest("a", "u", 500, time.Millisecond)
	m.ObserveRequest("b", "u", 200, time.Millisecond)
	if v := value(t, reg, "tollgate_requests_total", labels{"route": "a", "upstream": "u", "code": "200"}); v != 1 {
		t.Errorf("a/200 = %v", v)
	}
	if v := value(t, reg, "tollgate_requests_total", labels{"route": "a", "upstream": "u", "code": "500"}); v != 1 {
		t.Errorf("a/500 = %v", v)
	}
	if n := testutil.CollectAndCount(mustCollector(t, reg), "tollgate_requests_total"); n != 3 {
		t.Errorf("series count = %d, want 3", n)
	}
}

// mustCollector adapts a registry to a Collector for testutil.CollectAndCount.
func mustCollector(t *testing.T, reg *prometheus.Registry) prometheus.Collector {
	t.Helper()
	return registryCollector{reg}
}

type registryCollector struct{ g prometheus.Gatherer }

func (rc registryCollector) Describe(chan<- *prometheus.Desc) {}
func (rc registryCollector) Collect(ch chan<- prometheus.Metric) {
	mfs, _ := rc.g.Gather()
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			var names, vals []string
			for _, lp := range m.GetLabel() {
				names = append(names, lp.GetName())
				vals = append(vals, lp.GetValue())
			}
			d := prometheus.NewDesc(mf.GetName(), "x", names, nil)
			ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, m.GetCounter().GetValue(), vals...)
		}
	}
}

func TestRequestCounterExposition(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.ObserveRequest("api", "svc", 200, time.Millisecond)
	want := `
# HELP tollgate_requests_total x
# TYPE tollgate_requests_total counter
tollgate_requests_total{code="200",route="api",upstream="svc"} 1
`
	// HELP text is implementation-defined, so compare values only via a HELP-agnostic gather.
	mfs, _ := reg.Gather()
	var help string
	for _, mf := range mfs {
		if mf.GetName() == "tollgate_requests_total" {
			help = mf.GetHelp()
		}
	}
	want = strings.Replace(want, "# HELP tollgate_requests_total x", "# HELP tollgate_requests_total "+help, 1)
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "tollgate_requests_total"); err != nil {
		t.Error(err)
	}
}

func TestHistogramBuckets(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.ObserveRequest("r", "u", 200, time.Millisecond)
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() != "tollgate_request_duration_seconds" {
			continue
		}
		bs := mf.GetMetric()[0].GetHistogram().GetBucket()
		if len(bs) < 5 {
			t.Fatalf("only %d buckets", len(bs))
		}
		smallest, largest := bs[0].GetUpperBound(), bs[0].GetUpperBound()
		for _, b := range bs {
			ub := b.GetUpperBound()
			if ub < smallest {
				smallest = ub
			}
			if ub > largest && ub < 1e300 { // skip +Inf if present
				largest = ub
			}
		}
		if smallest > 0.001 {
			t.Errorf("smallest bucket %v > 1ms", smallest)
		}
		if largest < 5 {
			t.Errorf("largest finite bucket %v < 5s", largest)
		}
		return
	}
	t.Fatal("histogram family missing")
}

// Unknown reasons are recorded as given: the caller bounds cardinality, and
// dropping data silently would hide a wiring bug.
func TestReasonCounters(t *testing.T) {
	type call func(m *Metrics, route, reason string)
	tests := []struct {
		name    string
		metric  string
		do      call
		reasons []string
	}{
		{"ratelimited", "tollgate_ratelimited_total", func(m *Metrics, r, re string) { m.RateLimited(r, re) },
			[]string{"limit", "capacity", "auth_throttle", "something_new"}},
		{"auth failures", "tollgate_auth_failures_total", func(m *Metrics, r, re string) { m.AuthFailed(r, re) },
			[]string{"missing", "invalid", "something_new"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer guard(t)
			reg := prometheus.NewRegistry()
			m := New(reg)
			for i, re := range tc.reasons {
				for j := 0; j <= i; j++ { // reason i is counted i+1 times
					tc.do(m, "api", re)
				}
			}
			tc.do(m, "other", tc.reasons[0])
			for i, re := range tc.reasons {
				if got := value(t, reg, tc.metric, labels{"route": "api", "reason": re}); got != float64(i+1) {
					t.Errorf("%s reason=%s = %v, want %d", tc.metric, re, got, i+1)
				}
			}
			if got := value(t, reg, tc.metric, labels{"route": "other", "reason": tc.reasons[0]}); got != 1 {
				t.Errorf("other route = %v, want 1", got)
			}
		})
	}
}

func TestBackendGauges(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := New(reg)
	bl := labels{"upstream": "u", "backend": "b1"}

	m.SetBackendUp("u", "b1", true)
	if v := value(t, reg, "tollgate_backend_up", bl); v != 1 {
		t.Errorf("up = %v", v)
	}
	m.SetBackendUp("u", "b1", false)
	if v := value(t, reg, "tollgate_backend_up", bl); v != 0 {
		t.Errorf("down = %v", v)
	}
	m.SetBackendInFlight("u", "b1", 7)
	m.SetBackendInFlight("u", "b1", 3)
	if v := value(t, reg, "tollgate_backend_inflight", bl); v != 3 {
		t.Errorf("inflight = %v", v)
	}
	m.BackendEjected("u", "b1")
	m.BackendEjected("u", "b1")
	if v := value(t, reg, "tollgate_backend_ejections_total", bl); v != 2 {
		t.Errorf("ejections = %v", v)
	}
}

func TestForgetBackend(t *testing.T) {
	defer guard(t)
	names := []string{"tollgate_backend_up", "tollgate_backend_inflight", "tollgate_backend_ejections_total"}
	seed := func(m *Metrics, up, be string) {
		m.SetBackendUp(up, be, true)
		m.SetBackendInFlight(up, be, 2)
		m.BackendEjected(up, be)
	}

	t.Run("removes exactly that backend", func(t *testing.T) {
		defer guard(t)
		reg := prometheus.NewRegistry()
		m := New(reg)
		seed(m, "u", "b1")
		seed(m, "u", "b2")
		seed(m, "other", "b1") // same backend name, different upstream
		m.ForgetBackend("u", "b1")
		for _, n := range names {
			if has(t, reg, n, labels{"upstream": "u", "backend": "b1"}) {
				t.Errorf("%s{u,b1} still present", n)
			}
			if !has(t, reg, n, labels{"upstream": "u", "backend": "b2"}) {
				t.Errorf("%s{u,b2} wrongly removed", n)
			}
			if !has(t, reg, n, labels{"upstream": "other", "backend": "b1"}) {
				t.Errorf("%s{other,b1} wrongly removed", n)
			}
		}
	})

	t.Run("unknown backend is a no-op", func(t *testing.T) {
		defer guard(t)
		reg := prometheus.NewRegistry()
		m := New(reg)
		seed(m, "u", "b1")
		m.ForgetBackend("u", "nope")
		m.ForgetBackend("nope", "b1")
		for _, n := range names {
			if !has(t, reg, n, labels{"upstream": "u", "backend": "b1"}) {
				t.Errorf("%s lost", n)
			}
		}
	})

	t.Run("set after forget recreates the series", func(t *testing.T) {
		defer guard(t)
		reg := prometheus.NewRegistry()
		m := New(reg)
		seed(m, "u", "b1")
		m.ForgetBackend("u", "b1")
		m.SetBackendUp("u", "b1", false)
		if v := value(t, reg, "tollgate_backend_up", labels{"upstream": "u", "backend": "b1"}); v != 0 {
			t.Errorf("up = %v", v)
		}
		// Counter restarts from zero after a forget.
		m.BackendEjected("u", "b1")
		if v := value(t, reg, "tollgate_backend_ejections_total", labels{"upstream": "u", "backend": "b1"}); v != 1 {
			t.Errorf("ejections = %v, want 1", v)
		}
	})
}

func TestConfigMetrics(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.SetConfigVersion(41)
	m.SetConfigVersion(42)
	if v := value(t, reg, "tollgate_config_version", labels{}); v != 42 {
		t.Errorf("version = %v", v)
	}
	m.ConfigApplyFailed()
	m.ConfigApplyFailed()
	m.ConfigApplyFailed()
	if v := value(t, reg, "tollgate_config_apply_failures_total", labels{}); v != 3 {
		t.Errorf("failures = %v", v)
	}
	m.SetConfigStreamConnected(true)
	if v := value(t, reg, "tollgate_config_stream_connected", labels{}); v != 1 {
		t.Errorf("connected = %v", v)
	}
	m.SetConfigStreamConnected(false)
	if v := value(t, reg, "tollgate_config_stream_connected", labels{}); v != 0 {
		t.Errorf("disconnected = %v", v)
	}
}

func TestHandler(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	other := prometheus.NewRegistry()
	other.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "foreign_metric_total", Help: "h"}))
	m := New(reg)
	New(prometheus.NewRegistry()) // unrelated gateway metrics elsewhere

	scrape := func(h http.Handler) (int, string, string) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		b, _ := io.ReadAll(rec.Result().Body)
		return rec.Code, rec.Header().Get("Content-Type"), string(b)
	}

	h := Handler(reg)
	m.ObserveRequest("api", "svc", 200, time.Millisecond)
	code, ct, body := scrape(h)
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type = %q", ct)
	}
	if !strings.Contains(body, `tollgate_requests_total{code="200",route="api",upstream="svc"} 1`) {
		t.Errorf("body missing request series:\n%s", body)
	}
	if strings.Contains(body, "foreign_metric_total") {
		t.Error("handler leaked another registry")
	}

	m.ObserveRequest("api", "svc", 200, time.Millisecond)
	_, _, body = scrape(h)
	if !strings.Contains(body, `tollgate_requests_total{code="200",route="api",upstream="svc"} 2`) {
		t.Errorf("later observation not reflected:\n%s", body)
	}
}

func TestConcurrency(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := New(reg)
	const goroutines, iters = 32, 1000
	backends := []string{"b0", "b1", "b2"}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				be := backends[(g+i)%len(backends)]
				switch i % 8 {
				case 0, 4:
					m.ObserveRequest("r", "u", 200+(i%3), time.Microsecond)
				case 1:
					m.SetBackendUp("u", be, i%2 == 0)
				case 2:
					m.SetBackendInFlight("u", be, int64(i))
				case 3:
					m.ForgetBackend("u", be)
				case 5:
					m.BackendEjected("u", be)
				case 6:
					m.RateLimited("r", "limit")
					m.AuthFailed("r", "invalid")
				case 7:
					m.SetConfigVersion(int64(i))
					m.ConfigApplyFailed()
					m.SetConfigStreamConnected(i%2 == 0)
				}
			}
		}(g)
	}
	// Scrape concurrently too.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := reg.Gather(); err != nil {
					t.Errorf("gather: %v", err)
					return
				}
			}
		}
	}()
	wg.Wait()
	close(stop)
	<-done

	var total float64
	for _, code := range []string{"200", "201", "202"} {
		if ok, v, _ := find(t, reg, "tollgate_requests_total", labels{"route": "r", "upstream": "u", "code": code}); ok {
			total += v
		}
	}
	if want := float64(goroutines * iters / 4); total != want {
		t.Errorf("request total = %v, want %v", total, want)
	}
	if v := value(t, reg, "tollgate_config_apply_failures_total", labels{}); v != float64(goroutines*iters/8) {
		t.Errorf("apply failures = %v", v)
	}
}

func TestControlPlaneMetrics(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	cp := NewControlPlane(reg)
	cp.ObserveAPIRequest("GET", "/v1/config", 200, 100*time.Millisecond)
	cp.ObserveAPIRequest("GET", "/v1/config", 200, 100*time.Millisecond)
	cp.ObserveAPIRequest("POST", "/v1/config", 422, time.Millisecond)
	cp.SetHeadVersion(9)
	cp.SetConnectedDataPlanes(3)

	if v := value(t, reg, "tollgate_cp_api_requests_total", labels{"method": "GET", "route": "/v1/config", "code": "200"}); v != 2 {
		t.Errorf("GET 200 = %v", v)
	}
	if v := value(t, reg, "tollgate_cp_api_requests_total", labels{"method": "POST", "route": "/v1/config", "code": "422"}); v != 1 {
		t.Errorf("POST 422 = %v", v)
	}
	ok, sum, count := find(t, reg, "tollgate_cp_api_request_duration_seconds", labels{"method": "GET", "route": "/v1/config"})
	if !ok || count != 2 || sum < 0.199 || sum > 0.201 {
		t.Errorf("duration found=%v count=%d sum=%v", ok, count, sum)
	}
	if v := value(t, reg, "tollgate_cp_config_head_version", labels{}); v != 9 {
		t.Errorf("head = %v", v)
	}
	cp.SetConnectedDataPlanes(0)
	if v := value(t, reg, "tollgate_cp_dataplanes_connected", labels{}); v != 0 {
		t.Errorf("connected = %v", v)
	}

	for name, typ := range map[string]string{
		"tollgate_cp_api_requests_total":           "COUNTER",
		"tollgate_cp_api_request_duration_seconds": "HISTOGRAM",
		"tollgate_cp_config_head_version":          "GAUGE",
		"tollgate_cp_dataplanes_connected":         "GAUGE",
	} {
		gt, help, _, ok := familyType(t, reg, name)
		if !ok || gt != typ || strings.TrimSpace(help) == "" {
			t.Errorf("%s: ok=%v type=%s help=%q", name, ok, gt, help)
		}
	}

	// Gateway and control plane can share a registry only if names do not collide.
	New(reg)

	// The gateway registry must not contain control-plane series.
	gw := prometheus.NewRegistry()
	New(gw)
	mfs, _ := gw.Gather()
	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), "tollgate_cp_") {
			t.Errorf("gateway exports %s", mf.GetName())
		}
	}
}

func TestNilReceiversAreNoOps(t *testing.T) {
	t.Run("gateway", func(t *testing.T) {
		defer guard(t)
		var m *Metrics
		m.ObserveRequest("r", "u", 200, time.Second)
		m.RateLimited("r", "limit")
		m.AuthFailed("r", "missing")
		m.SetBackendUp("u", "b", true)
		m.SetBackendInFlight("u", "b", 1)
		m.BackendEjected("u", "b")
		m.ForgetBackend("u", "b")
		m.SetConfigVersion(1)
		m.ConfigApplyFailed()
		m.SetConfigStreamConnected(true)
	})
	t.Run("control plane", func(t *testing.T) {
		defer guard(t)
		var m *ControlPlaneMetrics
		m.ObserveAPIRequest("GET", "/x", 200, time.Second)
		m.SetHeadVersion(1)
		m.SetConnectedDataPlanes(1)
	})
}

func BenchmarkObserveRequest(b *testing.B) {
	m := New(prometheus.NewRegistry())
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.ObserveRequest("api", "svc", 200, 3*time.Millisecond)
		}
	})
}
