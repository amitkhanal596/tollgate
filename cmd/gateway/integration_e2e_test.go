package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/controlplane"
)

// End-to-end tests of the gateway process wired to a REAL control-plane
// handler running in-process (Phases 4-6). They are written against
//
//	func runEnv(ctx context.Context, args []string, logOut io.Writer, getenv func(string) string) int
//
// (stubs_integration.go), a new entry point next to the existing run, so the
// existing tests keep compiling. Flags pinned here: -control-plane (mutually
// exclusive with -config), -node-id (default: hostname), -lkg-path,
// -consul-addr; TOLLGATE_DATAPLANE_TOKEN comes from getenv.

const (
	e2eAdminTok = "admin-token-0123456789abcdef0123456789abcdef"
	e2eDPTok    = "dplane-token-0123456789abcdef0123456789abcdef"
	e2eUnsetEnv = "TOLLGATE_E2E_UNSET_SECRET"
)

// ---- small utilities --------------------------------------------------------

type e2eBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *e2eBuf) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *e2eBuf) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

func e2eFreeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func e2eWait(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

var e2eClient = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}

func e2eGet(url string) (int, string, error) {
	resp, err := e2eClient.Get(url)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}

// e2eUpstream answers every request with its name.
func e2eUpstream(t *testing.T, name string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, name)
	}))
	t.Cleanup(s.Close)
	return s
}

// ---- config documents --------------------------------------------------------

func e2eRoute(name, prefix, upstream string) string {
	return fmt.Sprintf(`{"name":%q,"path_prefix":%q,"upstream":%q}`, name, prefix, upstream)
}

func e2eUp(name, url string) string {
	return fmt.Sprintf(`{"name":%q,"backends":[%q]}`, name, url)
}

func e2eDoc(routes, ups []string, extra string) string {
	return fmt.Sprintf(`{"routes":[%s],"upstreams":[%s]%s}`, strings.Join(routes, ","), strings.Join(ups, ","), extra)
}

// e2eSingle routes /api to one upstream "u" at url.
func e2eSingle(url string) string {
	return e2eDoc([]string{e2eRoute("api", "/api", "u")}, []string{e2eUp("u", url)}, "")
}

// ---- in-process control plane ---------------------------------------------------

type e2eCP struct {
	t    *testing.T
	hub  *controlplane.Hub
	srv  *httptest.Server
	once sync.Once
}

func newE2ECP(t *testing.T) *e2eCP {
	t.Helper()
	hub := controlplane.NewHub()
	h, err := controlplane.NewHandler(controlplane.Options{
		Store: controlplane.NewMemStore(), Hub: hub, Nodes: controlplane.NewNodes(nil, 0),
		AdminToken: e2eAdminTok, DataPlaneToken: e2eDPTok,
		Heartbeat: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	cp := &e2eCP{t: t, hub: hub, srv: httptest.NewServer(h)}
	t.Cleanup(cp.stop)
	return cp
}

// stop takes the control plane down for good (idempotent): open streams are
// ended first, otherwise httptest's Close would wait for them forever.
func (c *e2eCP) stop() {
	c.once.Do(func() {
		c.hub.Close()
		c.srv.CloseClientConnections()
		c.srv.Close()
	})
}

func (c *e2eCP) admin(method, path, body string) (int, []byte) {
	c.t.Helper()
	req, err := http.NewRequest(method, c.srv.URL+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+e2eAdminTok)
	resp, err := e2eClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// write performs an admin write that must succeed and returns the new head version.
func (c *e2eCP) write(method, path, body string) int64 {
	c.t.Helper()
	code, b := c.admin(method, path, body)
	if code != 200 && code != 201 {
		c.t.Fatalf("%s %s: %d %s", method, path, code, b)
	}
	// PUT /v1/config and rollback return the config; PUT /v1/routes returns
	// the route, so read the version from the head instead.
	code, b = c.admin(http.MethodGet, "/v1/config", "")
	var cfg config.Config
	if code != 200 || json.Unmarshal(b, &cfg) != nil {
		c.t.Fatalf("GET /v1/config: %d %s", code, b)
	}
	return cfg.Version
}

func (c *e2eCP) node(id string) (controlplane.NodeStatus, bool) {
	code, b := c.admin(http.MethodGet, "/v1/dataplanes", "")
	var list controlplane.DataPlaneList
	if code != 200 || json.Unmarshal(b, &list) != nil {
		return controlplane.NodeStatus{}, false
	}
	for _, n := range list.DataPlanes {
		if n.Node == id {
			return n, true
		}
	}
	return controlplane.NodeStatus{}, false
}

// ---- gateway process ---------------------------------------------------------------

type e2eGW struct {
	traffic, admin string
	cancel         context.CancelFunc
	done           chan int
	logs           *e2eBuf
}

// callRunEnv runs runEnv, converting a panic (the stub) into a test failure
// and exit code -1, so one panic does not abort the whole test binary.
func callRunEnv(t *testing.T, ctx context.Context, args []string, logOut io.Writer, getenv func(string) string) chan int {
	t.Helper()
	done := make(chan int, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("runEnv panicked: %v", r)
				done <- -1
			}
		}()
		done <- runEnv(ctx, args, logOut, getenv)
	}()
	return done
}

func e2eEnv(kv ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return func(k string) string { return m[k] }
}

// startE2EGW starts the gateway with the given extra flags and token. On
// cleanup it stops the gateway, requires a clean exit and checks that no
// secret ever reached the logs.
func startE2EGW(t *testing.T, token string, extra ...string) *e2eGW {
	t.Helper()
	g := &e2eGW{traffic: e2eFreeAddr(t), admin: e2eFreeAddr(t), logs: &e2eBuf{}}
	args := append([]string{"-listen", g.traffic, "-admin-listen", g.admin}, extra...)
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	getenv := e2eEnv("TOLLGATE_DATAPLANE_TOKEN", token)
	g.done = callRunEnv(t, ctx, args, g.logs, getenv)
	t.Cleanup(func() {
		cancel()
		select {
		case code := <-g.done:
			g.done <- code
			if code != 0 && code != -1 {
				t.Errorf("gateway exit code %d at cleanup, want 0", code)
			}
		case <-time.After(10 * time.Second):
			t.Error("gateway did not stop within 10s of cancel")
		}
		if token != "" && strings.Contains(g.logs.String(), token) {
			t.Error("the data-plane token appears in the gateway logs")
		}
	})
	return g
}

// exited reports a prematurely finished gateway so waits fail fast.
func (g *e2eGW) exited() (int, bool) {
	select {
	case code := <-g.done:
		g.done <- code
		return code, true
	default:
		return 0, false
	}
}

func (g *e2eGW) waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	e2eWait(t, d, what, func() bool {
		if code, gone := g.exited(); gone {
			t.Fatalf("gateway exited with code %d while waiting for %s; logs:\n%s", code, what, g.logs.String())
		}
		return cond()
	})
}

func (g *e2eGW) readyz() int {
	code, _, err := e2eGet("http://" + g.admin + "/readyz")
	if err != nil {
		return 0
	}
	return code
}

func (g *e2eGW) waitReady(t *testing.T) {
	t.Helper()
	g.waitFor(t, 8*time.Second, "/readyz 200", func() bool { return g.readyz() == 200 })
}

// waitAdmin waits until the admin listener answers at all.
func (g *e2eGW) waitAdmin(t *testing.T) {
	t.Helper()
	g.waitFor(t, 5*time.Second, "admin listener", func() bool { return g.readyz() != 0 })
}

// body returns the status and body of GET /api/x on the traffic port ("" body on error).
func (g *e2eGW) api() (int, string) {
	code, body, err := e2eGet("http://" + g.traffic + "/api/x")
	if err != nil {
		return 0, ""
	}
	return code, body
}

func (g *e2eGW) waitAPI(t *testing.T, want string) {
	t.Helper()
	g.waitFor(t, 8*time.Second, "traffic to reach "+want, func() bool {
		code, body := g.api()
		return code == 200 && body == want
	})
}

// metric scrapes the admin listener and returns the value of an unlabeled series.
func (g *e2eGW) metric(name string) (float64, bool) {
	code, body, err := e2eGet("http://" + g.admin + "/metrics")
	if err != nil || code != 200 {
		return 0, false
	}
	for _, ln := range strings.Split(body, "\n") {
		if strings.HasPrefix(ln, name+" ") {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(ln, name+" ")), 64)
			return v, err == nil
		}
	}
	return 0, false
}

func (g *e2eGW) waitMetric(t *testing.T, name string, want float64) {
	t.Helper()
	g.waitFor(t, 8*time.Second, fmt.Sprintf("%s == %v", name, want), func() bool {
		v, ok := g.metric(name)
		return ok && v == want
	})
}

func cpFlags(cp *e2eCP, node string, extra ...string) []string {
	return append([]string{"-control-plane", cp.srv.URL, "-node-id", node}, extra...)
}

// ---- tests ------------------------------------------------------------------------

// The fixtures must be accepted by the real control plane (this passes today;
// it keeps a later failure from being blamed on a bad fixture).
func TestE2EFixtureDocsAreValid(t *testing.T) {
	cp := newE2ECP(t)
	a := e2eUpstream(t, "A")
	jwtDoc := e2eDoc(
		[]string{e2eRoute("api", "/api", "u"), `{"name":"secure","path_prefix":"/secure","upstream":"u","auth":"jwt"}`},
		[]string{e2eUp("u", a.URL)},
		`,"auth":{"jwt":{"algorithm":"HS256","hmac_secret_env":"`+e2eUnsetEnv+`"}}`)
	for name, doc := range map[string]string{"single": e2eSingle(a.URL), "jwt": jwtDoc} {
		if code, body := cp.admin(http.MethodPut, "/v1/config", doc); code != 200 {
			t.Errorf("%s: PUT /v1/config = %d %s", name, code, body)
		}
	}
	// The helpers the end-to-end tests rely on, against the real handler.
	cp.write(http.MethodPut, "/v1/config", e2eDoc([]string{e2eRoute("api", "/api", "uA")}, []string{e2eUp("uA", a.URL), e2eUp("uB", a.URL)}, ""))
	if v := cp.write(http.MethodPut, "/v1/routes/api", e2eRoute("api", "/api", "uB")); v != 4 {
		t.Errorf("version after route PUT = %d, want 4", v)
	}
	if v := cp.write(http.MethodPost, "/v1/config/rollback", ""); v != 5 {
		t.Errorf("version after rollback = %d, want 5", v)
	}
	if _, ok := cp.node("nobody"); ok {
		t.Error("unknown node reported")
	}
}

func TestE2EFlagValidation(t *testing.T) {
	a := e2eUpstream(t, "A")
	cfgFile := filepath.Join(t.TempDir(), "gw.yaml")
	yaml := fmt.Sprintf("routes:\n  - name: api\n    path_prefix: /api\n    upstream: u\nupstreams:\n  - name: u\n    backends: [%q]\n", a.URL)
	if err := os.WriteFile(cfgFile, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	const cp = "http://127.0.0.1:1" // never contacted: validation fails first or the connection is refused

	tests := []struct {
		name     string
		args     []string
		token    string
		wantExit int // 0 = any non-zero code
	}{
		{"both -config and -control-plane", []string{"-config", cfgFile, "-control-plane", cp}, e2eDPTok, 2},
		{"neither", nil, e2eDPTok, 2},
		{"-control-plane without TOLLGATE_DATAPLANE_TOKEN", []string{"-control-plane", cp, "-node-id", "n1"}, "", 0},
		{"-control-plane that is not a URL", []string{"-control-plane", "not a url", "-node-id", "n1"}, e2eDPTok, 0},
		{"invalid -node-id", []string{"-control-plane", cp, "-node-id", "bad node!"}, e2eDPTok, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var logs e2eBuf
			args := append([]string{"-listen", e2eFreeAddr(t), "-admin-listen", e2eFreeAddr(t)}, tc.args...)
			done := callRunEnv(t, ctx, args, &logs, e2eEnv("TOLLGATE_DATAPLANE_TOKEN", tc.token))
			select {
			case code := <-done:
				switch {
				case code == 0:
					t.Errorf("exit code 0, want non-zero; logs:\n%s", logs.String())
				case tc.wantExit != 0 && code != tc.wantExit:
					t.Errorf("exit code %d, want %d; logs:\n%s", code, tc.wantExit, logs.String())
				}
			case <-time.After(3 * time.Second):
				t.Fatal("run did not fail fast")
			}
			if tc.token != "" && strings.Contains(logs.String(), tc.token) {
				t.Error("the token appears in the logs")
			}
		})
	}
}

func TestE2ENotReadyUntilConfigThenAckRoundTrip(t *testing.T) {
	cp := newE2ECP(t)
	a := e2eUpstream(t, "A")
	gw := startE2EGW(t, e2eDPTok, cpFlags(cp, "gw-1")...)

	gw.waitAdmin(t)
	gw.waitFor(t, 5*time.Second, "node to connect to the control plane", func() bool {
		n, ok := cp.node("gw-1")
		return ok && n.Connected
	})
	// Connected, but the control plane has no config: not ready.
	for i := 0; i < 10; i++ {
		if code := gw.readyz(); code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d with no config yet, want 503", code)
		}
		time.Sleep(15 * time.Millisecond)
	}

	v := cp.write(http.MethodPut, "/v1/config", e2eSingle(a.URL))
	if v != 1 {
		t.Fatalf("first version = %d, want 1", v)
	}
	gw.waitReady(t)
	gw.waitAPI(t, "A")
	gw.waitFor(t, 5*time.Second, "control plane to show applied_version 1", func() bool {
		n, ok := cp.node("gw-1")
		return ok && n.Connected && n.AppliedVersion == 1
	})
	if n, _ := cp.node("gw-1"); n.RejectedVersion != 0 || n.LastError != "" {
		t.Errorf("node reports rejection %d %q after a good config", n.RejectedVersion, n.LastError)
	}
	gw.waitMetric(t, "tollgate_config_version", 1)
	gw.waitMetric(t, "tollgate_config_stream_connected", 1)
}

func TestE2EDefaultNodeIDIsHostname(t *testing.T) {
	host, err := os.Hostname()
	if err != nil || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`).MatchString(host) {
		t.Skipf("hostname %q is not a valid node ID on this machine", host)
	}
	cp := newE2ECP(t)
	gw := startE2EGW(t, e2eDPTok, "-control-plane", cp.srv.URL) // no -node-id
	gw.waitFor(t, 5*time.Second, "node named after the hostname to connect", func() bool {
		n, ok := cp.node(host)
		return ok && n.Connected
	})
}

func TestE2EHotReloadUnderLoad(t *testing.T) {
	cp := newE2ECP(t)
	a, b := e2eUpstream(t, "A"), e2eUpstream(t, "B")
	doc := e2eDoc([]string{e2eRoute("api", "/api", "uA")}, []string{e2eUp("uA", a.URL), e2eUp("uB", b.URL)}, "")
	cp.write(http.MethodPut, "/v1/config", doc)
	gw := startE2EGW(t, e2eDPTok, cpFlags(cp, "gw-1")...)
	gw.waitReady(t)
	gw.waitAPI(t, "A")

	var total, bad atomic.Int64
	var firstBad atomic.Value
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &http.Client{Timeout: 5 * time.Second}
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := c.Get("http://" + gw.traffic + "/api/x")
				total.Add(1)
				if err != nil {
					bad.Add(1)
					firstBad.CompareAndSwap(nil, err.Error())
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 || (string(body) != "A" && string(body) != "B") {
					bad.Add(1)
					firstBad.CompareAndSwap(nil, fmt.Sprintf("status %d body %q", resp.StatusCode, body))
				}
			}
		}()
	}

	var head int64
	target := "uA"
	for i := 0; i < 20; i++ {
		target = map[string]string{"uA": "uB", "uB": "uA"}[target]
		head = cp.write(http.MethodPut, "/v1/routes/api", e2eRoute("api", "/api", target))
		time.Sleep(5 * time.Millisecond)
	}
	want := map[string]string{"uA": "A", "uB": "B"}[target]
	gw.waitMetric(t, "tollgate_config_version", float64(head))
	gw.waitAPI(t, want)
	time.Sleep(50 * time.Millisecond) // a little more load on the final config
	close(stop)
	wg.Wait()

	if bad.Load() != 0 {
		t.Errorf("%d of %d requests failed during 20 route flips, first: %v; want zero dropped", bad.Load(), total.Load(), firstBad.Load())
	}
	if total.Load() < 100 {
		t.Errorf("only %d requests were sent during the reloads; the load was too light to prove anything", total.Load())
	}
}

func TestE2ERollbackReturnsTrafficAndVersionMetric(t *testing.T) {
	cp := newE2ECP(t)
	a, b := e2eUpstream(t, "A"), e2eUpstream(t, "B")
	doc := e2eDoc([]string{e2eRoute("api", "/api", "uA")}, []string{e2eUp("uA", a.URL), e2eUp("uB", b.URL)}, "")
	cp.write(http.MethodPut, "/v1/config", doc)
	gw := startE2EGW(t, e2eDPTok, cpFlags(cp, "gw-1")...)
	gw.waitReady(t)
	gw.waitAPI(t, "A")

	cp.write(http.MethodPut, "/v1/routes/api", e2eRoute("api", "/api", "uB"))
	gw.waitAPI(t, "B")
	gw.waitMetric(t, "tollgate_config_version", 2)

	head := cp.write(http.MethodPost, "/v1/config/rollback", "")
	if head != 3 {
		t.Fatalf("head after rollback = %d, want 3 (rollback publishes a NEW version)", head)
	}
	gw.waitAPI(t, "A")
	gw.waitMetric(t, "tollgate_config_version", float64(head))
	gw.waitFor(t, 5*time.Second, "control plane to show applied_version 3", func() bool {
		n, ok := cp.node("gw-1")
		return ok && n.AppliedVersion == 3
	})
}

func TestE2ERejectedConfigKeepsServingAndIsReported(t *testing.T) {
	cp := newE2ECP(t)
	a, b := e2eUpstream(t, "A"), e2eUpstream(t, "B")
	cp.write(http.MethodPut, "/v1/config", e2eSingle(a.URL))
	gw := startE2EGW(t, e2eDPTok, cpFlags(cp, "gw-1")...)
	gw.waitReady(t)
	gw.waitAPI(t, "A")

	// Valid for the control plane, unbuildable for the gateway: the JWT
	// secret variable is not set in its environment.
	badDoc := e2eDoc(
		[]string{e2eRoute("api", "/api", "u"), `{"name":"secure","path_prefix":"/secure","upstream":"u","auth":"jwt"}`},
		[]string{e2eUp("u", a.URL)},
		`,"auth":{"jwt":{"algorithm":"HS256","hmac_secret_env":"`+e2eUnsetEnv+`"}}`)
	if v := cp.write(http.MethodPut, "/v1/config", badDoc); v != 2 {
		t.Fatalf("bad config got version %d, want 2", v)
	}

	gw.waitFor(t, 5*time.Second, "control plane to show the rejection", func() bool {
		n, ok := cp.node("gw-1")
		return ok && n.RejectedVersion == 2 && n.LastError != ""
	})
	if n, _ := cp.node("gw-1"); n.AppliedVersion != 1 {
		t.Errorf("applied_version = %d after a rejection, want 1", n.AppliedVersion)
	}
	// Still serving version 1, and the metrics say so.
	for i := 0; i < 5; i++ {
		if code, body := gw.api(); code != 200 || body != "A" {
			t.Fatalf("request after rejection: %d %q, want 200 A", code, body)
		}
	}
	gw.waitMetric(t, "tollgate_config_version", 1)
	gw.waitMetric(t, "tollgate_config_apply_failures_total", 1)
	if code := gw.readyz(); code != 200 {
		t.Errorf("/readyz = %d after a rejected config, want 200 (the old config is live)", code)
	}

	// A later valid config is applied.
	cp.write(http.MethodPut, "/v1/config", e2eSingle(b.URL))
	gw.waitAPI(t, "B")
	gw.waitMetric(t, "tollgate_config_version", 3)
	gw.waitFor(t, 5*time.Second, "applied_version 3", func() bool {
		n, ok := cp.node("gw-1")
		return ok && n.AppliedVersion == 3
	})
}

func TestE2EControlPlaneDownKeepsServing(t *testing.T) {
	cp := newE2ECP(t)
	a := e2eUpstream(t, "A")
	cp.write(http.MethodPut, "/v1/config", e2eSingle(a.URL))
	gw := startE2EGW(t, e2eDPTok, cpFlags(cp, "gw-1")...)
	gw.waitReady(t)
	gw.waitAPI(t, "A")
	gw.waitMetric(t, "tollgate_config_stream_connected", 1)

	cp.stop()

	gw.waitMetric(t, "tollgate_config_stream_connected", 0)
	for i := 0; i < 20; i++ {
		if code, body := gw.api(); code != 200 || body != "A" {
			t.Fatalf("request %d with the control plane down: %d %q, want 200 A", i, code, body)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if code := gw.readyz(); code != 200 {
		t.Errorf("/readyz = %d with the control plane down, want 200 (static stability)", code)
	}
}

func TestE2ELastKnownGoodServesWithControlPlaneDown(t *testing.T) {
	cp := newE2ECP(t)
	a, b := e2eUpstream(t, "A"), e2eUpstream(t, "B")
	lkg := filepath.Join(t.TempDir(), "lkg.json")

	cp.write(http.MethodPut, "/v1/config", e2eSingle(a.URL))
	gw1 := startE2EGW(t, e2eDPTok, cpFlags(cp, "gw-1", "-lkg-path", lkg)...)
	gw1.waitReady(t)
	gw1.waitAPI(t, "A")
	// The last applied config is B: the LKG must hold B, not the first one.
	cp.write(http.MethodPut, "/v1/config", e2eSingle(b.URL))
	gw1.waitAPI(t, "B")
	e2eWait(t, 5*time.Second, "the LKG file to hold the applied config", func() bool {
		raw, err := os.ReadFile(lkg)
		return err == nil && strings.Contains(string(raw), b.URL)
	})

	gw1.cancel()
	select {
	case code := <-gw1.done:
		gw1.done <- code
		if code != 0 {
			t.Fatalf("first gateway exit code %d, want 0", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first gateway did not stop")
	}
	cp.stop() // stays down for the rest of the test

	gw2 := startE2EGW(t, e2eDPTok, cpFlags(cp, "gw-1", "-lkg-path", lkg)...)
	gw2.waitReady(t)
	gw2.waitAPI(t, "B")
	gw2.waitMetric(t, "tollgate_config_stream_connected", 0)
}

// /metrics is an admin-listener endpoint only (section 11 row 19).
func TestE2EMetricsOnAdminListenerOnly(t *testing.T) {
	up := e2eUpstream(t, "UP")
	tests := []struct {
		name        string
		prefix      string
		wantCode    int
		wantBody    string // exact body on the traffic port, "" = only check it is not metrics
		trafficPath string
	}{
		{name: "no route for /metrics", prefix: "/api", wantCode: 404, trafficPath: "/metrics"},
		{name: "catch-all route forwards /metrics to the upstream", prefix: "/", wantCode: 200, wantBody: "UP", trafficPath: "/metrics"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfgFile := filepath.Join(t.TempDir(), "gw.yaml")
			yaml := fmt.Sprintf("routes:\n  - name: r\n    path_prefix: %s\n    upstream: u\nupstreams:\n  - name: u\n    backends: [%q]\n", tc.prefix, up.URL)
			if err := os.WriteFile(cfgFile, []byte(yaml), 0o644); err != nil {
				t.Fatal(err)
			}
			gw := startE2EGW(t, "", "-config", cfgFile, "-config-poll", "50ms")
			gw.waitReady(t)

			code, body, err := e2eGet("http://" + gw.traffic + tc.trafficPath)
			if err != nil {
				t.Fatal(err)
			}
			if code != tc.wantCode || (tc.wantBody != "" && body != tc.wantBody) || strings.Contains(body, "tollgate_") {
				t.Errorf("traffic port GET /metrics = %d %q, want %d %q and no metrics", code, body, tc.wantCode, tc.wantBody)
			}

			// A proxied request, then the admin listener must expose it.
			if tc.prefix == "/" {
				_, _, _ = e2eGet("http://" + gw.traffic + "/hello")
			} else {
				_, _, _ = e2eGet("http://" + gw.traffic + "/api/hello")
			}
			gw.waitFor(t, 5*time.Second, "tollgate_requests_total on the admin listener", func() bool {
				c, b, err := e2eGet("http://" + gw.admin + "/metrics")
				return err == nil && c == 200 && strings.Contains(b, "tollgate_requests_total")
			})
		})
	}
}

// -consul-addr turns discovery on: a `consul:` upstream is filled from the
// (fake) Consul agent.
func TestE2EConsulAddrFlagEnablesDiscovery(t *testing.T) {
	up := e2eUpstream(t, "DISCOVERED")
	host, port, _ := net.SplitHostPort(strings.TrimPrefix(up.URL, "http://"))
	portNum, _ := strconv.Atoi(port)

	consul := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health/service/users" {
			http.NotFound(w, r)
			return
		}
		// Blocking-query semantics, minimally: a request that carries the
		// index we already served waits a little, then repeats the answer.
		if r.URL.Query().Get("index") != "" {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(300 * time.Millisecond):
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Consul-Index", "5")
		w.Header().Set("X-Consul-KnownLeader", "true")
		w.Header().Set("X-Consul-LastContact", "0")
		fmt.Fprintf(w, `[{"Node":{"Node":"n1","Address":%q},"Service":{"ID":"users-1","Service":"users","Address":%q,"Port":%d,"Tags":[]},`+
			`"Checks":[{"Node":"n1","CheckID":"service:users-1","Name":"svc","Status":"passing","ServiceID":"users-1"}]}]`, host, host, portNum)
	}))
	defer consul.Close()

	cfgFile := filepath.Join(t.TempDir(), "gw.yaml")
	yaml := "routes:\n  - name: api\n    path_prefix: /api\n    upstream: users\nupstreams:\n  - name: users\n    consul:\n      service: users\n"
	if err := os.WriteFile(cfgFile, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	gw := startE2EGW(t, "", "-config", cfgFile, "-config-poll", "50ms", "-consul-addr", strings.TrimPrefix(consul.URL, "http://"))
	gw.waitAPI(t, "DISCOVERED")
}
