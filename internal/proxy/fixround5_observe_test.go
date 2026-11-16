package proxy

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/metrics"
)

// Fix round 5, metrics / access-log items O1-O3 and A2 (proxy level).

// ---- O1: upgraded connections --------------------------------------------------

// echoUpgradeServer answers "Upgrade: echo" with 101 and then echoes lines
// until the peer goes away; anything else gets a plain 200.
func echoUpgradeServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "echo") {
			_, _ = io.WriteString(w, "plain")
			return
		}
		conn, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
		_ = brw.Flush()
		for {
			line, err := brw.ReadString('\n')
			if err != nil {
				return
			}
			_, _ = brw.WriteString(line)
			_ = brw.Flush()
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func TestFixRound5_UpgradeIsLoggedAs101AndNotObservedAsLatency(t *testing.T) {
	reg, m := newMetrics(t)
	var logs igBuf
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	upstream := echoUpgradeServer(t)
	_, srv := startGatewayWith(t, Options{Metrics: m, AccessLogger: logger},
		cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upstream.URL)))

	// A normal request first: the histogram baseline.
	if code, body := getBody(t, newClient(t), srv.URL+"/plain"); code != 200 || body != "plain" {
		t.Fatalf("plain request = %d %q", code, body)
	}
	series := map[string]string{"route": "r", "upstream": "u"}
	if n := igHistCount(t, reg, "tollgate_request_duration_seconds", series); n != 1 {
		t.Fatalf("fixture: duration count after one plain request = %d, want 1", n)
	}

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	req, _ := http.NewRequest(http.MethodGet, "http://gw/ws", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "echo")
	if err := req.Write(conn); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatalf("reading the upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade response = %d, want 101", resp.StatusCode)
	}
	if _, err := io.WriteString(conn, "ping\n"); err != nil {
		t.Fatal(err)
	}
	if line, err := br.ReadString('\n'); err != nil || line != "ping\n" {
		t.Fatalf("echo through the gateway = %q, %v, want ping", line, err)
	}
	conn.Close() // the upgraded connection ends: only now is the request recorded

	waitFor(t, 3*time.Second, "the upgraded request to be recorded", func() bool {
		return igSum(t, reg, "tollgate_requests_total") >= 2
	})
	if v := igNum(t, reg, "tollgate_requests_total", lbl("route", "r", "upstream", "u", "code", "101")); v != 1 {
		t.Errorf("tollgate_requests_total{code=101} = %v, want 1", v)
	}
	if v := igNum(t, reg, "tollgate_requests_total", lbl("route", "r", "upstream", "u", "code", "200")); v != 1 {
		t.Errorf("tollgate_requests_total{code=200} = %v, want 1 (only the plain request)", v)
	}
	if n := igHistCount(t, reg, "tollgate_request_duration_seconds", series); n != 1 {
		t.Errorf("duration histogram count = %d, want 1: a socket's lifetime is not a request latency", n)
	}

	waitFor(t, 2*time.Second, "the access log line of /ws", func() bool {
		for _, ln := range logs.requestLines(t) {
			if ln["path"] == "/ws" {
				return true
			}
		}
		return false
	})
	for _, ln := range logs.requestLines(t) {
		if ln["path"] == "/ws" && ln["status"] != float64(101) {
			t.Errorf("access log status for the upgraded request = %v, want 101", ln["status"])
		}
	}
}

// ---- O2: backend_up means "in rotation" ------------------------------------------

func TestFixRound5_BackendUpGoesToZeroWhilePassivelyEjected(t *testing.T) {
	tests := []struct {
		name string
		hc   *config.HealthCheck
	}{
		{"no active health check", nil},
		{"with an active health check", hcFast()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg, m := newMetrics(t)
			a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
			b.status.Store(500) // traffic fails, /healthz stays fine
			passive := &config.PassiveHealth{ConsecutiveFailures: 2, Cooloff: config.Duration(time.Second)}
			_, srv := startGatewayWith(t, Options{Metrics: m}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
				upx("u", config.AlgorithmRoundRobin, tc.hc, passive, a.url(), b.url())))
			upOf := func(x *ctrlBackend) (float64, bool) {
				return igBackendVal(t, reg, "tollgate_backend_up", "u", hostport(x.srv))
			}
			waitFor(t, 2*time.Second, "B up=1 at start", func() bool { v, ok := upOf(b); return ok && v == 1 })

			burst(t, newClient(t), srv.URL, "/x", 20) // ejects B after two 500s
			waitFor(t, 2*time.Second, "backend_up{B}=0 while B is ejected", func() bool { v, ok := upOf(b); return ok && v == 0 })
			if v, _ := upOf(a); v != 1 {
				t.Errorf("backend_up{A} = %v while only B is ejected, want 1", v)
			}
			waitFor(t, 3*time.Second, "backend_up{B}=1 after the cool-off", func() bool { v, ok := upOf(b); return ok && v == 1 })
		})
	}
}

// ---- O3: route series are forgotten -------------------------------------------------

func fr5RouteLabelCount(t *testing.T, m map[string][]igSeries, route string) int {
	t.Helper()
	n := 0
	for _, ss := range m {
		for _, s := range ss {
			if s.labels["route"] == route {
				n++
			}
		}
	}
	return n
}

func TestFixRound5_RemovedOrRenamedRouteSeriesDisappear(t *testing.T) {
	families := []string{
		"tollgate_requests_total", "tollgate_request_duration_seconds",
		"tollgate_ratelimited_total", "tollgate_auth_failures_total",
	}
	tests := []struct {
		name      string
		v2Routes  func(oldR config.Route, keep config.Route) []config.Route
		wantAfter string // route that must appear after v2 traffic ("" = none)
	}{
		{"removed", func(_, keep config.Route) []config.Route { return []config.Route{keep} }, ""},
		{"renamed", func(oldR, keep config.Route) []config.Route {
			oldR.Name = "old-renamed"
			return []config.Route{oldR, keep}
		}, "old-renamed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg, m := newMetrics(t)
			u := newRecUp(t)
			oldR := authRoute("old", "/old", "u", config.AuthAPIKey, rl(0.001, 1))
			keep := authRoute("keep", "/keep", "u", config.AuthNone, nil)
			auth := config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}
			h, srv := startGatewayWith(t, Options{Metrics: m}, withAuth(
				cfgOf(1, []config.Route{oldR, keep}, up("u", u.srv.URL)), auth))

			do := func(path, key string) int {
				req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
				if key != "" {
					req.Header.Set("X-API-Key", key)
				}
				resp, err := newClient(t).Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				return resp.StatusCode
			}
			if c := do("/old", ""); c != 401 {
				t.Fatalf("fixture: missing key = %d, want 401", c)
			}
			if c := do("/old", "alice-key"); c != 200 {
				t.Fatalf("fixture: first keyed request = %d, want 200", c)
			}
			if c := do("/old", "alice-key"); c != 429 {
				t.Fatalf("fixture: second keyed request = %d, want 429", c)
			}
			if c := do("/keep", ""); c != 200 {
				t.Fatalf("fixture: /keep = %d, want 200", c)
			}
			gather := func() map[string][]igSeries {
				out := map[string][]igSeries{}
				for _, f := range families {
					out[f] = igGather(t, reg, f)
				}
				return out
			}
			for _, f := range families {
				if fr5RouteLabelCount(t, map[string][]igSeries{f: igGather(t, reg, f)}, "old") == 0 {
					t.Fatalf("fixture: %s has no series for route old", f)
				}
			}

			h.apply(t, withAuth(cfgOf(2, tc.v2Routes(oldR, keep), up("u", u.srv.URL)), auth))
			waitFor(t, 2*time.Second, "series of the old route name to be forgotten", func() bool {
				return fr5RouteLabelCount(t, gather(), "old") == 0
			})
			if tc.wantAfter != "" {
				// The new name starts clean and counts normally.
				if c := do("/old", "alice-key"); c != 200 {
					t.Fatalf("renamed route first keyed request = %d, want 200", c)
				}
				if v := igNum(t, reg, "tollgate_requests_total", lbl("route", tc.wantAfter, "upstream", "u", "code", "200")); v != 1 {
					t.Errorf("requests_total for %s = %v, want 1", tc.wantAfter, v)
				}
				if n := fr5RouteLabelCount(t, gather(), "old"); n != 0 {
					t.Errorf("%d series with the old route name reappeared", n)
				}
			}
			if tc.name == "removed" {
				if v := igNum(t, reg, "tollgate_requests_total", lbl("route", "keep", "upstream", "u", "code", "200")); v != 1 {
					t.Errorf("series of the route that stayed: requests_total{keep} = %v, want 1 (untouched)", v)
				}
			}
		})
	}
}

// ---- A2 (proxy level): a stuck log sink must not stall requests ----------------------

// fr5BlockedSink blocks every Write until released.
type fr5BlockedSink struct {
	gate    chan struct{}
	once    sync.Once
	entered atomic.Int64
}

func newFr5BlockedSink() *fr5BlockedSink { return &fr5BlockedSink{gate: make(chan struct{})} }

func (s *fr5BlockedSink) Write(p []byte) (int, error) {
	s.entered.Add(1)
	<-s.gate
	return len(p), nil
}

func (s *fr5BlockedSink) release() { s.once.Do(func() { close(s.gate) }) }

func TestFixRound5_BlockedAccessLogSinkDoesNotStallRequests(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	sink := newFr5BlockedSink()
	defer sink.release()
	var drops atomic.Int64
	aw := metrics.NewAsyncWriter(sink, 64, func() { drops.Add(1) })
	defer func() {
		sink.release()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = aw.Close(ctx)
	}()
	logger := slog.New(slog.NewJSONHandler(aw, nil))

	a := echoUpstream(t, "A")
	_, srv := startGatewayWith(t, Options{AccessLogger: logger},
		cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", a.URL)))

	cl := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 8}}
	defer cl.CloseIdleConnections()
	start := time.Now()
	for i := 0; i < 500; i++ {
		resp, err := cl.Get(srv.URL + "/x")
		if err != nil {
			t.Fatalf("request %d failed with the log sink blocked: %v", i, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("request %d = %d, want 200", i, resp.StatusCode)
		}
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Errorf("500 requests took %v with a blocked log sink", el)
	}
	if sink.entered.Load() == 0 {
		t.Error("the sink was never written to: the test did not exercise a blocked sink")
	}
	if drops.Load() == 0 {
		t.Error("onDrop never called although 500 lines went into a 64-line buffer behind a blocked sink")
	}
}
