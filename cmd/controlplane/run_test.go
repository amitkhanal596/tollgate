package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/amitkhanal596/tollgate/internal/controlplane"
)

// Tests are written against
//   func run(ctx context.Context, args []string, logOut io.Writer, getenv func(string) string) int
// Exit codes: 0 clean shutdown, 2 flag/usage error, 1 startup or runtime failure.

const (
	adminTok = "ADMINTOKEN-0123456789abcdef0123456789abcdef"
	dpTok    = "DPLANETOKEN-0123456789abcdef0123456789abcdef"

	putBody = `{"routes":[{"name":"api","path_prefix":"/api","upstream":"web"}],"upstreams":[{"name":"web","backends":["http://127.0.0.1:9001"]}]}`
)

// panicCode is what runSafe returns when run panics (the stub does).
const panicCode = -999

// syncBuf is a goroutine-safe log sink.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func goodEnv() map[string]string {
	return map[string]string{
		controlplane.EnvAdminToken:     adminTok,
		controlplane.EnvDataPlaneToken: dpTok,
	}
}

// runSafe calls run and converts a panic into panicCode so each test fails
// on its own instead of aborting the whole test binary.
func runSafe(ctx context.Context, args []string, out io.Writer, env map[string]string) (code int) {
	defer func() {
		if r := recover(); r != nil {
			code = panicCode
		}
	}()
	return run(ctx, args, out, envOf(env))
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// runOnce runs to completion (for startup-failure cases) with a deadline.
func runOnce(t *testing.T, args []string, env map[string]string, limit time.Duration) (int, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := &syncBuf{}
	done := make(chan int, 1)
	go func() { done <- runSafe(ctx, args, logs, env) }()
	select {
	case code := <-done:
		if code == panicCode {
			t.Fatalf("run panicked (stub not implemented?)")
		}
		return code, logs.String()
	case <-time.After(limit):
		cancel()
		t.Fatalf("run did not return within %v (logs: %s)", limit, logs.String())
		return 0, ""
	}
}

type cp struct {
	addr   string
	cancel context.CancelFunc
	done   chan int
	logs   *syncBuf
	drain  time.Duration
}

// startCP starts the control plane on a free port and waits for /healthz.
// extra args are appended; callers wanting a store must pass one.
func startCP(t *testing.T, env map[string]string, extra ...string) *cp {
	t.Helper()
	c := &cp{addr: freeAddr(t), done: make(chan int, 1), logs: &syncBuf{}, drain: 3 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	args := append([]string{"-listen", c.addr, "-drain-timeout", c.drain.String(), "-heartbeat", "20ms"}, extra...)
	go func() { c.done <- runSafe(ctx, args, c.logs, env) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-c.done:
		case <-time.After(10 * time.Second):
			t.Error("run did not return at cleanup")
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-c.done:
			c.done <- code
			t.Fatalf("run exited early with code %d (panic=%d); logs: %s", code, panicCode, c.logs.String())
		default:
		}
		resp, err := http.Get("http://" + c.addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return c
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("control plane did not become healthy; logs: %s", c.logs.String())
	return nil
}

// stop cancels the context and returns run's exit code and how long it took.
func (c *cp) stop(t *testing.T) (int, time.Duration) {
	t.Helper()
	start := time.Now()
	c.cancel()
	select {
	case code := <-c.done:
		c.done <- code // keep cleanup working
		return code, time.Since(start)
	case <-time.After(c.drain + 5*time.Second):
		t.Fatal("run did not return after cancel")
		return 0, 0
	}
}

func (c *cp) do(t *testing.T, method, path, token, body string) (*http.Response, []byte) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://"+c.addr+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (c *cp) headVersion(t *testing.T) (int64, []byte) {
	t.Helper()
	resp, b := c.do(t, "GET", "/v1/config", adminTok, "")
	if resp.StatusCode != 200 {
		t.Fatalf("GET /v1/config = %d: %s", resp.StatusCode, b)
	}
	var v struct {
		Version int64 `json:"version"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("decode config: %v: %s", err, b)
	}
	return v.Version, b
}

func eventually(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (c *cp) metrics(t *testing.T) string {
	t.Helper()
	resp, b := c.do(t, "GET", "/metrics", "", "")
	if resp.StatusCode != 200 {
		t.Fatalf("GET /metrics = %d", resp.StatusCode)
	}
	return string(b)
}

func writeSeed(t *testing.T, yaml string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "seed.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const validSeedYAML = "routes:\n  - name: seeded\n    path_prefix: /seeded\n    upstream: u\nupstreams:\n  - name: u\n    backends: [\"http://127.0.0.1:9002\"]\n"

// openStream opens the SSE stream with the data-plane token and returns a
// channel of non-comment lines plus the response body.
func (c *cp) openStream(t *testing.T, node string, since int) (<-chan string, io.Closer) {
	t.Helper()
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/v1/config/stream?node=%s&since=%d", c.addr, node, since), nil)
	req.Header.Set("Authorization", "Bearer "+dpTok)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("stream status %d", resp.StatusCode)
	}
	lines := make(chan string, 256)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	return lines, resp.Body
}

// ---- 1. happy path --------------------------------------------------------

func TestHappyPath(t *testing.T) {
	c := startCP(t, goodEnv(), "-memory-store")

	for _, p := range []string{"/healthz", "/readyz"} {
		if resp, _ := c.do(t, "GET", p, "", ""); resp.StatusCode != 200 {
			t.Errorf("%s = %d, want 200", p, resp.StatusCode)
		}
	}
	if v, _ := c.headVersion(t); v != 0 {
		t.Errorf("empty store head version = %d, want 0", v)
	}
	// Admin endpoints must reject a missing and the data-plane token.
	if resp, _ := c.do(t, "GET", "/v1/config", "", ""); resp.StatusCode != 401 {
		t.Errorf("no token = %d, want 401", resp.StatusCode)
	}
	if resp, _ := c.do(t, "GET", "/v1/config", dpTok, ""); resp.StatusCode != 401 {
		t.Errorf("data-plane token on admin endpoint = %d, want 401", resp.StatusCode)
	}
	resp, b := c.do(t, "PUT", "/v1/config", adminTok, putBody)
	if resp.StatusCode != 200 {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("ETag"); got != `"1"` {
		t.Errorf("ETag = %q, want \"1\"", got)
	}
	if v, _ := c.headVersion(t); v != 1 {
		t.Errorf("head after PUT = %d, want 1", v)
	}
	code, took := c.stop(t)
	if code != 0 {
		t.Errorf("exit code = %d, want 0; logs: %s", code, c.logs.String())
	}
	if took > c.drain {
		t.Errorf("shutdown took %v, longer than drain timeout %v", took, c.drain)
	}
}

// ---- 2. stream end to end -------------------------------------------------

func TestStreamEndToEndAndShutdown(t *testing.T) {
	c := startCP(t, goodEnv(), "-memory-store")
	lines, body := c.openStream(t, "n1", 0)
	defer func() { _ = body.Close() }()

	// Wait until the server has registered the stream before writing, so the
	// event is delivered live (not only via head-on-connect).
	eventually(t, "dataplane n1 connected", func() bool {
		_, b := c.do(t, "GET", "/v1/dataplanes", adminTok, "")
		return strings.Contains(string(b), `"n1"`)
	})
	if resp, b := c.do(t, "PUT", "/v1/config", adminTok, putBody); resp.StatusCode != 200 {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, b)
	}

	var sawID, sawEvent, sawData bool
	timeout := time.After(5 * time.Second)
read:
	for !sawID || !sawEvent || !sawData {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatal("stream ended before the config event")
			}
			switch {
			case l == "id: 1":
				sawID = true
			case l == "event: config":
				sawEvent = true
			case strings.HasPrefix(l, "data: ") && strings.Contains(l, `"version":1`):
				sawData = true
			}
		case <-timeout:
			break read
		}
	}
	if !sawID || !sawEvent || !sawData {
		t.Fatalf("config frame incomplete: id=%v event=%v data=%v", sawID, sawEvent, sawData)
	}

	// Cancel: the open stream must not hold shutdown past the drain timeout.
	code, took := c.stop(t)
	if code != 0 {
		t.Errorf("exit code = %d, want 0; logs: %s", code, c.logs.String())
	}
	if took >= c.drain {
		t.Errorf("shutdown took %v with an open stream; want < drain %v", took, c.drain)
	}
	// The client side must see the stream end.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-lines:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("stream did not end after shutdown")
		}
	}
}

// ---- 3. metrics -----------------------------------------------------------

func TestMetricsOnSameListener(t *testing.T) {
	c := startCP(t, goodEnv(), "-memory-store")
	if m := c.metrics(t); !strings.Contains(m, "tollgate_cp_") {
		t.Fatalf("/metrics has no tollgate_cp_ metrics:\n%s", m)
	}
	if resp, b := c.do(t, "PUT", "/v1/config", adminTok, putBody); resp.StatusCode != 200 {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, b)
	}
	head := regexp.MustCompile(`(?m)^tollgate_cp_config_head_version 1$`)
	reqs := regexp.MustCompile(`(?m)^tollgate_cp_api_requests_total\{[^}]*method="PUT"[^}]*\} [1-9]`)
	eventually(t, "head version and api request metrics", func() bool {
		m := c.metrics(t)
		return head.MatchString(m) && reqs.MatchString(m)
	})

	_, body := c.openStream(t, "n1", 0)
	connected := func(n int) func() bool {
		re := regexp.MustCompile(fmt.Sprintf(`(?m)^tollgate_cp_dataplanes_connected %d$`, n))
		return func() bool { return re.MatchString(c.metrics(t)) }
	}
	eventually(t, "dataplanes_connected 1", connected(1))
	_ = body.Close()
	eventually(t, "dataplanes_connected 0", connected(0))
}

// ---- 4. seed --------------------------------------------------------------

func TestSeed(t *testing.T) {
	t.Run("empty store is seeded as version 1", func(t *testing.T) {
		c := startCP(t, goodEnv(), "-memory-store", "-seed", writeSeed(t, validSeedYAML))
		v, b := c.headVersion(t)
		if v != 1 {
			t.Fatalf("head = %d, want 1", v)
		}
		if !strings.Contains(string(b), `"seeded"`) {
			t.Errorf("head does not contain seeded route: %s", b)
		}
		resp, vb := c.do(t, "GET", "/v1/config/versions", adminTok, "")
		if resp.StatusCode != 200 || !strings.Contains(string(vb), `"seed"`) {
			t.Errorf("versions = %d, want author \"seed\": %s", resp.StatusCode, vb)
		}
	})
	t.Run("invalid seed fails startup", func(t *testing.T) {
		bad := "routes:\n  - name: r\n    path_prefix: /r\n    upstream: missing\n"
		code, _ := runOnce(t, []string{"-listen", freeAddr(t), "-memory-store", "-seed", writeSeed(t, bad)}, goodEnv(), 5*time.Second)
		if code != 1 {
			t.Errorf("exit = %d, want 1", code)
		}
	})
	t.Run("unparsable seed fails startup", func(t *testing.T) {
		code, _ := runOnce(t, []string{"-listen", freeAddr(t), "-memory-store", "-seed", writeSeed(t, "routes: [[[")}, goodEnv(), 5*time.Second)
		if code != 1 {
			t.Errorf("exit = %d, want 1", code)
		}
	})
	t.Run("missing seed file fails startup", func(t *testing.T) {
		code, _ := runOnce(t, []string{"-listen", freeAddr(t), "-memory-store", "-seed", filepath.Join(t.TempDir(), "nope.yaml")}, goodEnv(), 5*time.Second)
		if code != 1 {
			t.Errorf("exit = %d, want 1", code)
		}
	})
}

// ---- 5. startup failures --------------------------------------------------

func TestStartupFailures(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busy.Close() }()

	without := func(key string) map[string]string {
		e := goodEnv()
		delete(e, key)
		return e
	}
	withTokens := func(a, d string) map[string]string {
		return map[string]string{controlplane.EnvAdminToken: a, controlplane.EnvDataPlaneToken: d}
	}
	mem := func() []string { return []string{"-listen", freeAddr(t), "-memory-store"} }

	tests := []struct {
		name     string
		args     func() []string
		env      map[string]string
		wantCode int
		wantLog  string // must appear in logs, if non-empty
		limit    time.Duration
	}{
		{"missing admin token", mem, without(controlplane.EnvAdminToken), 1, controlplane.EnvAdminToken, 5 * time.Second},
		{"missing data-plane token", mem, without(controlplane.EnvDataPlaneToken), 1, controlplane.EnvDataPlaneToken, 5 * time.Second},
		{"short admin token", mem, withTokens("short", dpTok), 1, "", 5 * time.Second},
		{"short data-plane token", mem, withTokens(adminTok, "short"), 1, "", 5 * time.Second},
		{"equal tokens", mem, withTokens(adminTok, adminTok), 1, "", 5 * time.Second},
		{"no store configured", func() []string { return []string{"-listen", freeAddr(t)} }, goodEnv(), 2, "", 5 * time.Second},
		{"both memory store and database url", func() []string {
			return []string{"-listen", freeAddr(t), "-memory-store", "-database-url", "postgres://127.0.0.1:1/x"}
		}, goodEnv(), 2, "", 5 * time.Second},
		{"unknown flag", func() []string { return append(mem(), "-bogus") }, goodEnv(), 2, "", 5 * time.Second},
		{"listen address in use", func() []string { return []string{"-listen", busy.Addr().String(), "-memory-store"} }, goodEnv(), 1, "", 5 * time.Second},
		{"unreachable database", func() []string {
			return []string{"-listen", freeAddr(t), "-database-url", "postgres://127.0.0.1:1/x?connect_timeout=1"}
		}, goodEnv(), 1, "", 15 * time.Second},
		{"unreachable database via env", func() []string { return []string{"-listen", freeAddr(t)} }, func() map[string]string {
			e := goodEnv()
			e[controlplane.EnvDatabaseURL] = "postgres://127.0.0.1:1/x?connect_timeout=1"
			return e
		}(), 1, "", 15 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, logs := runOnce(t, tt.args(), tt.env, tt.limit)
			if code != tt.wantCode {
				t.Errorf("exit = %d, want %d; logs: %s", code, tt.wantCode, logs)
			}
			if tt.wantLog != "" && !strings.Contains(logs, tt.wantLog) {
				t.Errorf("logs do not mention %q: %s", tt.wantLog, logs)
			}
			for _, tok := range []string{adminTok, dpTok} {
				if strings.Contains(logs, tok) {
					t.Errorf("logs leak a token value: %s", logs)
				}
			}
		})
	}
}

func TestMemoryStoreWarns(t *testing.T) {
	c := startCP(t, goodEnv(), "-memory-store")
	logs := c.logs.String()
	found := false
	for _, l := range strings.Split(logs, "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["level"] == "WARN" && strings.Contains(strings.ToLower(fmt.Sprint(m["msg"])), "persist") {
			found = true
		}
	}
	if !found {
		t.Errorf("no WARN log mentioning persistence: %s", logs)
	}
}

// ---- 6 + 7. logs: no tokens, JSON lines -----------------------------------

func TestLogsAreJSONAndNeverContainTokens(t *testing.T) {
	c := startCP(t, goodEnv(), "-memory-store", "-seed", writeSeed(t, validSeedYAML))
	c.do(t, "PUT", "/v1/config", adminTok, putBody)
	c.do(t, "GET", "/v1/config", "wrong-"+adminTok, "") // a failed auth must not log the presented token either
	_, body := c.openStream(t, "n1", 0)
	time.Sleep(60 * time.Millisecond)
	_ = body.Close()
	if code, _ := c.stop(t); code != 0 {
		t.Fatalf("exit = %d", code)
	}
	logs := c.logs.String()
	if strings.TrimSpace(logs) == "" {
		t.Fatal("no logs written")
	}
	for _, tok := range []string{adminTok, dpTok, "wrong-" + adminTok} {
		if strings.Contains(logs, tok) {
			t.Errorf("logs contain token %q", tok)
		}
	}
	for _, l := range strings.Split(logs, "\n") {
		if strings.TrimSpace(l) == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Errorf("log line is not JSON: %q", l)
			continue
		}
		if _, ok := m["level"]; !ok {
			t.Errorf("log line has no level: %q", l)
		}
		if _, ok := m["msg"]; !ok {
			t.Errorf("log line has no msg: %q", l)
		}
	}
}

// ---- 8. postgres persistence ----------------------------------------------

func TestPostgresPersistence(t *testing.T) {
	base := os.Getenv("TOLLGATE_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TOLLGATE_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, base)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("tgcmd_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	dbURL := u.String()

	c1 := startCP(t, goodEnv(), "-database-url", dbURL)
	if resp, b := c1.do(t, "PUT", "/v1/config", adminTok, putBody); resp.StatusCode != 200 {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, b)
	}
	if code, _ := c1.stop(t); code != 0 {
		t.Fatalf("first run exit = %d; logs: %s", code, c1.logs.String())
	}

	// Second run, same schema, with a seed that must be ignored.
	c2 := startCP(t, goodEnv(), "-database-url", dbURL, "-seed", writeSeed(t, validSeedYAML))
	v, b := c2.headVersion(t)
	if v != 1 {
		t.Fatalf("head after restart = %d, want 1 (persisted, seed ignored)", v)
	}
	if strings.Contains(string(b), `"seeded"`) || !strings.Contains(string(b), `"api"`) {
		t.Errorf("seed overwrote persisted config: %s", b)
	}
	if code, _ := c2.stop(t); code != 0 {
		t.Errorf("second run exit = %d", code)
	}
}
