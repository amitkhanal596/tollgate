package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"net/http"
	"net/http/httptest"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/discovery"
)

// Shared helpers of the integration_*_test.go files (Phases 4-6 wiring).

// ---- log capture -----------------------------------------------------------

// igBuf is a goroutine-safe buffer for slog output.
type igBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *igBuf) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *igBuf) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// jsonLines decodes every complete JSON line written so far.
func (w *igBuf) jsonLines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, ln := range strings.Split(w.String(), "\n") {
		if strings.TrimSpace(ln) == "" {
			continue
		}
		m := map[string]any{}
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", ln, err)
		}
		out = append(out, m)
	}
	return out
}

// requestLines returns only the access-log lines (msg == "request").
func (w *igBuf) requestLines(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, m := range w.jsonLines(t) {
		if m["msg"] == "request" {
			out = append(out, m)
		}
	}
	return out
}

// ---- goroutine identity ------------------------------------------------------

// igGoroutineID returns the current goroutine's ID (parsed from the stack
// header; only used by tests to compare "same goroutine").
func igGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	f := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))
	id, _ := strconv.ParseUint(f[0], 10, 64)
	return id
}

// ---- fake discovery.Resolver --------------------------------------------------

// igWatch is one Watch call: its query, its context and the channel the test
// controls. send follows the Resolver contract: capacity 1, latest wins.
type igWatch struct {
	q   discovery.Query
	ctx context.Context

	mu     sync.Mutex
	ch     chan []discovery.Endpoint
	closed bool
}

func (w *igWatch) send(eps []discovery.Endpoint) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	select {
	case w.ch <- eps:
	default:
		select {
		case <-w.ch:
		default:
		}
		w.ch <- eps // only send (under mu) fills the channel, so this cannot block
	}
}

func (w *igWatch) cancelled() bool { return w.ctx.Err() != nil }

// igResolver records every Watch. With closeOnCancel it behaves like the
// real resolver (closes the channel when ctx is done); without it the channel
// stays open forever, which models a forwarder that must exit on ctx alone
// and lets a test send "late" updates on a dead watch.
type igResolver struct {
	closeOnCancel bool

	mu      sync.Mutex
	watches []*igWatch
}

func (r *igResolver) Watch(ctx context.Context, q discovery.Query) (<-chan []discovery.Endpoint, error) {
	w := &igWatch{q: q, ctx: ctx, ch: make(chan []discovery.Endpoint, 1)}
	r.mu.Lock()
	r.watches = append(r.watches, w)
	r.mu.Unlock()
	if r.closeOnCancel {
		go func() {
			<-ctx.Done()
			w.mu.Lock()
			w.closed = true
			close(w.ch)
			w.mu.Unlock()
		}()
	}
	return w.ch, nil
}

func (r *igResolver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.watches)
}

func (r *igResolver) watch(i int) *igWatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.watches[i]
}

// live returns the watches whose ctx is not cancelled.
func (r *igResolver) live() []*igWatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*igWatch
	for _, w := range r.watches {
		if !w.cancelled() {
			out = append(out, w)
		}
	}
	return out
}

// waitWatches waits until at least n Watch calls were made and returns the
// n-th (1-based) one.
func (r *igResolver) waitWatches(t *testing.T, n int) *igWatch {
	t.Helper()
	waitFor(t, 3*time.Second, "Resolver.Watch call #"+strconv.Itoa(n), func() bool { return r.count() >= n })
	return r.watch(n - 1)
}

// ---- discovery fixtures ---------------------------------------------------------

// hostport is "127.0.0.1:port" of an httptest server.
func hostport(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }

func igEP(id string, s *httptest.Server) discovery.Endpoint {
	return discovery.Endpoint{ID: id, Address: hostport(s)}
}

// discUp is an upstream fed by Consul (no static backends).
func discUp(name, service, tag, scheme string) config.Upstream {
	return config.Upstream{
		Name:      name,
		Algorithm: config.AlgorithmRoundRobin,
		Consul:    &config.ConsulService{Service: service, Tag: tag, Scheme: scheme},
	}
}

// discCfg is one catch-all route "r" to upstream "users".
func discCfg(version int64, u config.Upstream) *config.Config {
	return cfgOf(version, []config.Route{rt("r", "", "/", u.Name)}, u)
}

// igCount sends n GETs and counts the answers by body ("status-NNN" for non-200).
func igCount(t *testing.T, srvURL string, n int) map[string]int {
	t.Helper()
	c := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 10 * time.Second}
	out := map[string]int{}
	for i := 0; i < n; i++ {
		code, body := getBody(t, c, srvURL+"/x")
		if code != 200 {
			body = "status-" + strconv.Itoa(code)
		}
		out[body]++
	}
	return out
}
