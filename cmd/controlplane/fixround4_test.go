package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/amitkhanal596/tollgate/internal/controlplane"
)

// ---- helpers ---------------------------------------------------------------

// freshSchemaURL creates an empty schema in the test database and returns a
// database URL whose search_path points at it. It skips, without contacting
// any database, unless TOLLGATE_TEST_DATABASE_URL is set.
func freshSchemaURL(t *testing.T) string {
	t.Helper()
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
	schema := fmt.Sprintf("tgfix4_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

// launchCP starts run without waiting for it to become healthy, so two
// control planes can start at the same moment. Pair it with waitReady.
func launchCP(t *testing.T, env map[string]string, extra ...string) *cp {
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
	return c
}

// waitReady waits for GET /readyz = 200, failing if run exits first.
func (c *cp) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-c.done:
			c.done <- code
			t.Fatalf("run exited with code %d (panic=%d) before becoming ready; logs: %s", code, panicCode, c.logs.String())
		default:
		}
		resp, err := http.Get("http://" + c.addr + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("control plane %s not ready; logs: %s", c.addr, c.logs.String())
}

// alive fails the test if run has already exited.
func (c *cp) alive(t *testing.T, what string) {
	t.Helper()
	select {
	case code := <-c.done:
		c.done <- code
		t.Fatalf("%s: run exited with code %d; logs: %s", what, code, c.logs.String())
	default:
	}
}

// waitLine returns the first line from lines satisfying pred within d.
func waitLine(t *testing.T, lines <-chan string, d time.Duration, what string, pred func(string) bool) string {
	t.Helper()
	timeout := time.After(d)
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("stream ended while waiting for %s", what)
			}
			if pred(l) {
				return l
			}
		case <-timeout:
			t.Fatalf("no %s within %v", what, d)
		}
	}
}

func isHeartbeat(l string) bool { return strings.HasPrefix(l, ":") }

func drainLines(lines <-chan string) {
	for {
		select {
		case <-lines:
		default:
			return
		}
	}
}

// ---- P1: seed race ----------------------------------------------------------

// Two control planes starting together with -seed against one fresh database:
// exactly one wins the IfMatch=0 write, and the loser must treat the version
// conflict as "already seeded", not exit.
func TestFixRound4_ConcurrentSeedBothKeepRunning(t *testing.T) {
	seed := writeSeed(t, validSeedYAML)
	for round := 1; round <= 5; round++ {
		t.Run(fmt.Sprintf("round %d", round), func(t *testing.T) {
			dbURL := freshSchemaURL(t)
			a := launchCP(t, goodEnv(), "-database-url", dbURL, "-seed", seed)
			b := launchCP(t, goodEnv(), "-database-url", dbURL, "-seed", seed)
			a.waitReady(t)
			b.waitReady(t)
			for name, c := range map[string]*cp{"a": a, "b": b} {
				if v, _ := c.headVersion(t); v != 1 {
					t.Errorf("replica %s head = %d, want 1", name, v)
				}
				c.alive(t, "replica "+name)
			}
		})
	}
}

// ---- P2: a stream outlives ReadTimeout (known issue K1, refuted) -------------

func TestFixRound4_StreamOutlivesReadTimeout(t *testing.T) {
	c := startCP(t, goodEnv(), "-memory-store", "-read-timeout", "200ms", "-heartbeat", "50ms")
	lines, body := c.openStream(t, "gw-1", 0)
	defer func() { _ = body.Close() }()
	waitLine(t, lines, 2*time.Second, "first heartbeat", isHeartbeat)

	time.Sleep(1500 * time.Millisecond) // 7x the read timeout
	drainLines(lines)
	waitLine(t, lines, time.Second, "heartbeat after the read timeout elapsed", isHeartbeat)

	resp, b := c.do(t, "PUT", "/v1/config", adminTok, putBody)
	_ = resp.Body.Close() // do already closed it; keeps bodyclose quiet
	if resp.StatusCode != 200 {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, b)
	}
	waitLine(t, lines, 2*time.Second, "config event after the read timeout elapsed", func(l string) bool { return l == "id: 1" })
}

// ---- P3: head poller ----------------------------------------------------------

// Ungated: -head-poll exists and 0 (disabled) and a short interval both run
// cleanly and stop within the drain budget. (Two runs cannot share a MemStore,
// so cross-replica delivery is only testable with Postgres, below.)
func TestFixRound4_HeadPollFlagAccepted(t *testing.T) {
	for _, v := range []string{"0", "50ms", "2s"} {
		t.Run("head-poll "+v, func(t *testing.T) {
			c := startCP(t, goodEnv(), "-memory-store", "-head-poll", v)
			lines, body := c.openStream(t, "gw-1", 0)
			defer func() { _ = body.Close() }()
			resp, b := c.do(t, "PUT", "/v1/config", adminTok, putBody)
			_ = resp.Body.Close() // do already closed it; keeps bodyclose quiet
			if resp.StatusCode != 200 {
				t.Fatalf("PUT = %d: %s", resp.StatusCode, b)
			}
			// A local write still reaches local streams whatever the poller does.
			waitLine(t, lines, 2*time.Second, "config event", func(l string) bool { return l == "id: 1" })
			_ = body.Close()
			if code, _ := c.stop(t); code != 0 {
				t.Errorf("exit = %d, want 0; logs: %s", code, c.logs.String())
			}
		})
	}
}

// A write made through replica A reaches a stream held on replica B, because B
// notices the new head in the shared database.
func TestFixRound4_HeadPollDeliversCrossReplica(t *testing.T) {
	dbURL := freshSchemaURL(t)
	a := startCP(t, goodEnv(), "-database-url", dbURL, "-head-poll", "50ms")
	b := startCP(t, goodEnv(), "-database-url", dbURL, "-head-poll", "50ms")
	lines, body := b.openStream(t, "gw-b", 0)
	defer func() { _ = body.Close() }()

	resp, out := a.do(t, "PUT", "/v1/config", adminTok, putBody)
	_ = resp.Body.Close() // do already closed it; keeps bodyclose quiet
	if resp.StatusCode != 200 {
		t.Fatalf("PUT via replica A = %d: %s", resp.StatusCode, out)
	}
	waitLine(t, lines, 2*time.Second, "config v1 on replica B's stream", func(l string) bool { return l == "id: 1" })

	// And a second write, to show it is not a one-off.
	if resp, out := a.do(t, "PUT", "/v1/upstreams/extra", adminTok, `{"name":"extra","backends":["http://127.0.0.1:9003"]}`); resp.StatusCode != 201 {
		t.Fatalf("second PUT = %d: %s", resp.StatusCode, out)
	}
	waitLine(t, lines, 2*time.Second, "config v2 on replica B's stream", func(l string) bool { return l == "id: 2" })
}

// ---- P4: junk HTTP methods cannot blow up /metrics -----------------------------

func TestFixRound4_JunkMethodsKeepMetricsBounded(t *testing.T) {
	c := startCP(t, goodEnv(), "-memory-store")
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	errs := make(chan error, 300)
	for i := 0; i < 300; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			conn, err := net.DialTimeout("tcp", c.addr, 2*time.Second)
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = conn.Close() }()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			method := fmt.Sprintf("JUNK%dX%s", i, strings.Repeat("Z", 200))
			if _, err := fmt.Fprintf(conn, "%s /v1/config HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", method); err != nil {
				errs <- err
				return
			}
			_, _ = io.Copy(io.Discard, conn) // the status does not matter; the request must be served
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("raw request: %v", err)
	}
	m := c.metrics(t)
	if len(m) > 64<<10 {
		t.Errorf("/metrics is %d bytes after 300 junk-method requests, want < 64 KiB", len(m))
	}
	if !strings.Contains(m, `method="other"`) {
		t.Errorf(`/metrics has no method="other" series`)
	}
	if strings.Contains(m, "JUNK") {
		t.Errorf("junk method names leaked into /metrics labels")
	}
}

// ---- P5: drain timeout vs stream write timeout ---------------------------------

func TestFixRound4_DrainTimeoutWarning(t *testing.T) {
	w := controlplane.DefaultStreamWriteTimeout
	tests := []struct {
		name     string
		drain    time.Duration
		wantWarn bool
	}{
		{"half the write timeout", w / 2, true},
		{"equal to the write timeout", w, true},
		{"just above the write timeout", w + time.Second, false},
		{"default 10s", 10 * time.Second, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := startCP(t, goodEnv(), "-memory-store", "-drain-timeout", tc.drain.String())
			if code, _ := c.stop(t); code != 0 {
				t.Fatalf("exit = %d; logs: %s", code, c.logs.String())
			}
			warned := false
			for _, line := range strings.Split(c.logs.String(), "\n") {
				var rec struct {
					Level string `json:"level"`
					Msg   string `json:"msg"`
				}
				if json.Unmarshal([]byte(line), &rec) != nil {
					continue
				}
				if rec.Level == "WARN" && strings.Contains(strings.ToLower(rec.Msg), "drain") {
					warned = true
				}
			}
			if warned != tc.wantWarn {
				t.Errorf("WARN mentioning drain = %v, want %v; logs: %s", warned, tc.wantWarn, c.logs.String())
			}
		})
	}
}
