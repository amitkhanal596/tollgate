package main

import (
	"context"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Fix round 5, gateway process items G1-G3 and A2.

// fr5Run starts runEnv and returns the exit-code channel; cancel runs at cleanup.
func fr5Run(t *testing.T, args []string, logs interface{ Write([]byte) (int, error) }, getenv func(string) string) (chan int, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := callRunEnv(t, ctx, args, logs, getenv)
	t.Cleanup(func() {
		cancel()
		select {
		case code := <-done:
			done <- code
		case <-time.After(10 * time.Second):
			t.Error("gateway did not stop within 10s of cancel")
		}
	})
	return done, cancel
}

// ---- G1: no credentials in -control-plane URLs --------------------------------------

func TestFixRound5_ControlPlaneURLWithUserinfoIsRejected(t *testing.T) {
	tests := []struct{ name, url string }{
		{"user and password", "http://ops:hunter2pw@127.0.0.1:1"},
		{"password only", "http://:hunter2pw@127.0.0.1:1"},
		{"user only", "http://hunter2pw@127.0.0.1:1"},
		{"https", "https://ops:hunter2pw@cp.example.com:9000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs e2eBuf
			args := []string{"-listen", e2eFreeAddr(t), "-admin-listen", e2eFreeAddr(t),
				"-control-plane", tc.url, "-node-id", "n1"}
			done, cancel := fr5Run(t, args, &logs, e2eEnv("TOLLGATE_DATAPLANE_TOKEN", e2eDPTok))
			select {
			case code := <-done:
				done <- code // put the exit code back for fr5Run's cleanup, which waits on the same channel
				if code != 2 {
					t.Errorf("exit code %d, want 2 (bad flag value); logs:\n%s", code, logs.String())
				}
			case <-time.After(2 * time.Second):
				cancel()
				t.Error("the gateway accepted a -control-plane URL with credentials and kept running")
			}
			if strings.Contains(logs.String(), "hunter2pw") {
				t.Errorf("the URL password appears in the logs:\n%s", logs.String())
			}
		})
	}
}

// ---- G2: the hostname default is read through getenv("HOSTNAME") ----------------------

func TestFixRound5_InvalidHostnameAsNodeIDExitsOneAndNamesTheFlag(t *testing.T) {
	tests := []struct{ name, host string }{
		{"space and bang", "bad host!"},
		{"leading underscore", "_pod"},
		{"too long", strings.Repeat("a", 200)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs e2eBuf
			args := []string{"-listen", e2eFreeAddr(t), "-admin-listen", e2eFreeAddr(t), "-control-plane", "http://127.0.0.1:1"}
			done, cancel := fr5Run(t, args, &logs, e2eEnv("TOLLGATE_DATAPLANE_TOKEN", e2eDPTok, "HOSTNAME", tc.host))
			select {
			case code := <-done:
				done <- code // put the exit code back for fr5Run's cleanup, which waits on the same channel
				if code != 1 {
					t.Errorf("exit code %d, want 1 (environment problem, not a flag typo); logs:\n%s", code, logs.String())
				}
			case <-time.After(2 * time.Second):
				cancel()
				t.Fatal("the gateway started with an invalid hostname as its node ID")
			}
			if !strings.Contains(logs.String(), "-node-id") {
				t.Errorf("the message does not tell the operator to pass -node-id; logs:\n%s", logs.String())
			}
		})
	}
}

func TestFixRound5_HostnameFromGetenvIsTheDefaultNodeID(t *testing.T) {
	cp := newE2ECP(t)
	var logs e2eBuf
	args := []string{"-listen", e2eFreeAddr(t), "-admin-listen", e2eFreeAddr(t), "-control-plane", cp.srv.URL}
	fr5Run(t, args, &logs, e2eEnv("TOLLGATE_DATAPLANE_TOKEN", e2eDPTok, "HOSTNAME", "fr5-host-1"))
	e2eWait(t, 5*time.Second, "node fr5-host-1 (from $HOSTNAME) to connect", func() bool {
		n, ok := cp.node("fr5-host-1")
		return ok && n.Connected
	})
}

// ---- G3: runtime and process collectors on /metrics -----------------------------------

func TestFixRound5_MetricsIncludeGoRuntimeAndProcessCollectors(t *testing.T) {
	up := okUpstream(t, 0)
	r := startRun(t, writeCfg(t, up.URL))
	r.waitReady(t)
	code, body, err := get("http://" + r.admin + "/metrics")
	if err != nil || code != 200 {
		t.Fatalf("GET /metrics = %d, %v", code, err)
	}
	wants := []string{"go_goroutines", "go_info", "tollgate_config_version"}
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		wants = append(wants, "process_cpu_seconds_total")
	}
	for _, w := range wants {
		if !strings.Contains(body, "\n"+w+" ") && !strings.Contains(body, "\n"+w+"{") {
			t.Errorf("/metrics has no %s series", w)
		}
	}
}

// ---- A2: a blocked log sink does not stall the gateway ----------------------------------

// fr5LimitSink accepts the first limit bytes, then blocks every Write until released.
type fr5LimitSink struct {
	limit   int
	gate    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	written int
	blocked atomic.Bool
}

func (s *fr5LimitSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	if s.written < s.limit {
		s.written += len(p)
		s.mu.Unlock()
		return len(p), nil
	}
	s.mu.Unlock()
	s.blocked.Store(true)
	<-s.gate
	return len(p), nil
}

func (s *fr5LimitSink) release() { s.once.Do(func() { close(s.gate) }) }

func TestFixRound5_BlockedLogSinkDoesNotStallRequests(t *testing.T) {
	up := okUpstream(t, 0)
	// Large enough for the startup lines, far smaller than 500 access lines.
	sink := &fr5LimitSink{limit: 16 << 10, gate: make(chan struct{})}
	traffic, admin := e2eFreeAddr(t), e2eFreeAddr(t)
	args := []string{"-config", writeCfg(t, up.URL), "-config-poll", "50ms", "-listen", traffic, "-admin-listen", admin}
	ctx, cancel := context.WithCancel(context.Background())
	done := callRunEnv(t, ctx, args, sink, e2eEnv())
	t.Cleanup(func() {
		sink.release() // a sink that works again lets the shutdown logs and the async flush through
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("gateway did not stop within 10s of cancel")
		}
	})
	e2eWait(t, 5*time.Second, "/readyz 200", func() bool {
		c, _, err := e2eGet("http://" + admin + "/readyz")
		return err == nil && c == 200
	})

	cl := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 4}}
	defer cl.CloseIdleConnections()
	start := time.Now()
	for i := 0; i < 500; i++ {
		resp, err := cl.Get("http://" + traffic + "/api/x")
		if err != nil {
			t.Fatalf("request %d failed while the log sink was blocked (blocked=%v): %v", i, sink.blocked.Load(), err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("request %d = %d, want 200", i, resp.StatusCode)
		}
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Errorf("500 requests took %v with a blocked log sink", el)
	}
	e2eWait(t, 2*time.Second, "the sink to have blocked (otherwise the test proves nothing)", sink.blocked.Load)
}
