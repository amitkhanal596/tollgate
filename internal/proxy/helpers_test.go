package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// fakeSource honours the config.ConfigSource contract: capacity-1 channel with
// latest-wins replacement, closed by the source after ctx is canceled.
type fakeSource struct {
	ch     chan *config.Config
	mu     sync.Mutex
	closed bool
}

func newFakeSource() *fakeSource { return &fakeSource{ch: make(chan *config.Config, 1)} }

func (f *fakeSource) Watch(ctx context.Context) (<-chan *config.Config, error) {
	go func() {
		<-ctx.Done()
		f.closeChan()
	}()
	return f.ch, nil
}

func (f *fakeSource) closeChan() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.ch)
	}
}

// push sends cfg, replacing an unconsumed older value (latest wins).
func (f *fakeSource) push(cfg *config.Config) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	select {
	case f.ch <- cfg:
	default:
		select {
		case <-f.ch:
		default:
		}
		f.ch <- cfg // only push (under mu) sends, so this cannot block
	}
}

// errSource fails Watch synchronously.
type errSource struct{ err error }

func (e errSource) Watch(context.Context) (<-chan *config.Config, error) { return nil, e.err }

// harness runs Gateway.Run in a goroutine against a fakeSource.
type harness struct {
	g      *Gateway
	src    *fakeSource
	cancel context.CancelFunc
	done   chan error
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// startRun starts a gateway with no config applied yet.
func startRun(t *testing.T) *harness {
	t.Helper()
	g := New(Options{})
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{g: g, src: newFakeSource(), cancel: cancel, done: make(chan error, 1)}
	go func() { h.done <- g.Run(ctx, h.src) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-h.done:
			if err != nil {
				t.Logf("Run returned: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cleanup cancel")
		}
	})
	return h
}

// apply pushes cfg and waits until it is live.
func (h *harness) apply(t *testing.T, cfg *config.Config) {
	t.Helper()
	h.src.push(cfg)
	waitFor(t, 3*time.Second, "version to be applied", func() bool { return h.g.Version() == cfg.Version })
}

// startGateway starts Run, applies cfg, and fronts the gateway with a real server.
func startGateway(t *testing.T, cfg *config.Config) (*harness, *httptest.Server) {
	t.Helper()
	h := startRun(t)
	h.apply(t, cfg)
	srv := httptest.NewServer(h.g)
	t.Cleanup(srv.Close)
	return h, srv
}

func newClient(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{MaxIdleConnsPerHost: 16}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

// echoUpstream returns a server that replies with its own name.
func echoUpstream(t *testing.T, name string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(name))
	}))
	t.Cleanup(s.Close)
	return s
}

func rt(name, host, prefix, upstream string) config.Route {
	return config.Route{Name: name, Host: host, PathPrefix: prefix, Upstream: upstream, Auth: config.AuthNone}
}

func up(name string, backends ...string) config.Upstream {
	return config.Upstream{Name: name, Algorithm: config.AlgorithmRoundRobin, Backends: backends}
}

func testDefaults() config.Defaults {
	return config.Defaults{Timeouts: config.Timeouts{
		Dial:           config.Duration(time.Second),
		ResponseHeader: config.Duration(5 * time.Second),
		Request:        config.Duration(10 * time.Second),
	}}
}

func cfgOf(version int64, routes []config.Route, ups ...config.Upstream) *config.Config {
	return &config.Config{Version: version, Defaults: testDefaults(), Routes: routes, Upstreams: ups}
}
