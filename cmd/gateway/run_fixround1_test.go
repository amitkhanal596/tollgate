package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// NOTE: these tests are written against
//   func run(ctx context.Context, args []string, logOut io.Writer) int
// main.go currently has run(args []string) int; the builder changes it.

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

func writeCfg(t *testing.T, upstreamURL string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gw.yaml")
	y := fmt.Sprintf("routes:\n  - name: api\n    path_prefix: /api\n    upstream: u\nupstreams:\n  - name: u\n    backends: [%q]\n", upstreamURL)
	if err := os.WriteFile(p, []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

type running struct {
	traffic, admin string
	cancel         context.CancelFunc
	done           chan int
}

func startRun(t *testing.T, cfg string, extra ...string) *running {
	t.Helper()
	r := &running{traffic: freeAddr(t), admin: freeAddr(t), done: make(chan int, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	args := append([]string{"-config", cfg, "-listen", r.traffic, "-admin-listen", r.admin, "-config-poll", "50ms"}, extra...)
	go func() { r.done <- run(ctx, args, io.Discard) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(10 * time.Second):
			t.Error("run did not return at cleanup")
		}
	})
	return r
}

func (r *running) wait(t *testing.T, d time.Duration) int {
	t.Helper()
	select {
	case code := <-r.done:
		r.done <- code // keep for cleanup
		return code
	case <-time.After(d):
		t.Fatalf("run did not return within %v", d)
		return -1
	}
}

func get(url string) (int, string, error) {
	c := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := c.Get(url)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	dl := time.Now().Add(d)
	for !cond() {
		if time.Now().After(dl) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *running) waitReady(t *testing.T) {
	t.Helper()
	eventually(t, 5*time.Second, "/readyz 200", func() bool {
		c, _, err := get("http://" + r.admin + "/readyz")
		return err == nil && c == 200
	})
}

func okUpstream(t *testing.T, delay time.Duration) *httptest.Server {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		_, _ = io.WriteString(w, "hello from upstream")
	}))
	t.Cleanup(s.Close)
	return s
}

func TestRunHappyPath(t *testing.T) {
	up := okUpstream(t, 0)
	r := startRun(t, writeCfg(t, up.URL))
	r.waitReady(t)
	code, body, err := get("http://" + r.traffic + "/api/x")
	if err != nil || code != 200 || body != "hello from upstream" {
		t.Fatalf("proxied = %d %q %v", code, body, err)
	}
	start := time.Now()
	r.cancel()
	if c := r.wait(t, 5*time.Second); c != 0 {
		t.Errorf("exit code = %d, want 0", c)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("default shutdown took %v, want prompt", d)
	}
}

func TestRunStartupFailures(t *testing.T) {
	tests := []struct {
		name string
		args func(t *testing.T) []string
	}{
		{"missing -config", func(t *testing.T) []string {
			return []string{"-listen", freeAddr(t), "-admin-listen", freeAddr(t)}
		}},
		{"nonexistent config file", func(t *testing.T) []string {
			return []string{"-config", filepath.Join(t.TempDir(), "nope.yaml"), "-listen", freeAddr(t), "-admin-listen", freeAddr(t)}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan int, 1)
			args := tc.args(t)
			go func() { done <- run(ctx, args, io.Discard) }()
			select {
			case code := <-done:
				if code == 0 {
					t.Error("exit code 0, want non-zero")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("run did not exit quickly")
			}
		})
	}
}

// Section 11 row 13.
func TestRunShutdownDelay(t *testing.T) {
	const delay = 600 * time.Millisecond
	up := okUpstream(t, 0)
	r := startRun(t, writeCfg(t, up.URL), "-shutdown-delay", delay.String())
	r.waitReady(t)

	cancelAt := time.Now()
	r.cancel()
	eventually(t, delay/2, "/readyz 503 after cancel", func() bool {
		c, _, err := get("http://" + r.admin + "/readyz")
		return err == nil && c == 503
	})
	// Still within the delay: traffic must keep being served.
	if time.Since(cancelAt) < delay-150*time.Millisecond {
		code, body, err := get("http://" + r.traffic + "/api/x")
		if err != nil || code != 200 || body != "hello from upstream" {
			t.Fatalf("traffic during shutdown delay = %d %q %v", code, body, err)
		}
	}
	select {
	case c := <-r.done:
		t.Fatalf("run returned (%d) before the delay elapsed (%v)", c, time.Since(cancelAt))
	default:
	}
	if c := r.wait(t, 5*time.Second); c != 0 {
		t.Errorf("exit code = %d", c)
	}
	if d := time.Since(cancelAt); d < delay-50*time.Millisecond {
		t.Errorf("run returned after %v, want >= %v", d, delay)
	}
	if _, _, err := get("http://" + r.traffic + "/api/x"); err == nil {
		t.Error("traffic listener still open after run returned")
	}
}

func TestRunGracefulDrain(t *testing.T) {
	entered := make(chan struct{}, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, "slow done")
	}))
	defer up.Close()
	r := startRun(t, writeCfg(t, up.URL))
	r.waitReady(t)

	type res struct {
		code int
		body string
		err  error
	}
	out := make(chan res, 1)
	go func() {
		c, b, err := get("http://" + r.traffic + "/api/slow")
		out <- res{c, b, err}
	}()
	<-entered
	r.cancel()
	got := <-out
	if got.err != nil || got.code != 200 || got.body != "slow done" {
		t.Fatalf("in-flight request = %+v", got)
	}
	if c := r.wait(t, 5*time.Second); c != 0 {
		t.Errorf("exit code = %d", c)
	}
}

// Section 11 row 19: ops paths are never answered by the traffic listener.
func TestTrafficPortDoesNotServeOpsEndpoints(t *testing.T) {
	up := okUpstream(t, 0)
	r := startRun(t, writeCfg(t, up.URL))
	r.waitReady(t)
	for _, p := range []string{"/healthz", "/readyz", "/metrics"} {
		code, body, err := get("http://" + r.traffic + p)
		if err != nil || code != 404 {
			t.Errorf("traffic %s = %d %q %v, want 404 from the router", p, code, body, err)
		}
	}
	if code, _, err := get("http://" + r.admin + "/healthz"); err != nil || code != 200 {
		t.Errorf("admin /healthz = %d %v", code, err)
	}
}
