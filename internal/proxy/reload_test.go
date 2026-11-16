package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

func getBody(t *testing.T, c *http.Client, url string) (int, string) {
	t.Helper()
	resp, err := c.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestReloadChangesRouting(t *testing.T) {
	a, b := echoUpstream(t, "A"), echoUpstream(t, "B")
	h, srv := startGateway(t, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", a.URL)))
	c := newClient(t)

	if code, body := getBody(t, c, srv.URL+"/"); code != 200 || body != "A" {
		t.Fatalf("v1: got %d %q, want 200 A", code, body)
	}
	h.apply(t, cfgOf(2, []config.Route{rt("r", "", "/", "u")}, up("u", b.URL)))
	if v := h.g.Version(); v != 2 {
		t.Fatalf("Version() = %d, want 2", v)
	}
	if code, body := getBody(t, c, srv.URL+"/"); code != 200 || body != "B" {
		t.Fatalf("v2: got %d %q, want 200 B", code, body)
	}
}

func TestReloadInFlightRequestFinishesOnOldSnapshot(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-time.After(5 * time.Second):
		}
		_, _ = io.WriteString(w, "old-backend")
	}))
	defer old.Close()
	keep := echoUpstream(t, "keep")

	h, srv := startGateway(t, cfgOf(1, []config.Route{
		rt("old", "", "/old", "oldu"),
		rt("keep", "", "/keep", "keepu"),
	}, up("oldu", old.URL), up("keepu", keep.URL)))

	type result struct {
		code int
		body string
		err  error
	}
	res := make(chan result, 1)
	go func() {
		resp, err := newClient(t).Get(srv.URL + "/old")
		if err != nil {
			res <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		res <- result{resp.StatusCode, string(b), err}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("old upstream never saw the request")
	}

	// v2 removes the /old route and its upstream entirely.
	h.apply(t, cfgOf(2, []config.Route{rt("keep", "", "/keep", "keepu")}, up("keepu", keep.URL)))
	if code, _ := getBody(t, newClient(t), srv.URL+"/old"); code != http.StatusNotFound {
		t.Fatalf("new request to removed route: got %d, want 404", code)
	}

	close(release)
	r := <-res
	if r.err != nil || r.code != 200 || r.body != "old-backend" {
		t.Fatalf("in-flight request: got %d %q err=%v, want 200 old-backend", r.code, r.body, r.err)
	}
}

func TestReloadUnderLoadDropsNothing(t *testing.T) {
	a, b := echoUpstream(t, "A"), echoUpstream(t, "B")
	mk := func(v int64) *config.Config {
		u := a.URL
		if v%2 == 0 {
			u = b.URL
		}
		return cfgOf(v, []config.Route{rt("r", "", "/", "u")}, up("u", u))
	}
	h, srv := startGateway(t, mk(1))
	c := newClient(t)

	const clients = 8
	var total, bad atomic.Int64
	var firstBad atomic.Value
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				total.Add(1)
				resp, err := c.Get(srv.URL + "/")
				if err != nil {
					bad.Add(1)
					firstBad.CompareAndSwap(nil, err.Error())
					continue
				}
				b, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != 200 || (string(b) != "A" && string(b) != "B") {
					bad.Add(1)
					firstBad.CompareAndSwap(nil, fmt.Sprintf("status %d body %q", resp.StatusCode, b))
				}
			}
		}()
	}

	for v := int64(2); v <= 51; v++ {
		h.src.push(mk(v))
		time.Sleep(10 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if n := bad.Load(); n != 0 {
		t.Errorf("%d of %d requests failed during reloads; first: %v", n, total.Load(), firstBad.Load())
	}
	if total.Load() == 0 {
		t.Error("no requests were made")
	}
	waitFor(t, 3*time.Second, "final version 51 (latest wins)", func() bool { return h.g.Version() == 51 })
}

func TestReloadRejectsUnbuildableConfig(t *testing.T) {
	a := echoUpstream(t, "A")
	tests := []struct {
		name string
		cfg  *config.Config
	}{
		{"route references unknown upstream", cfgOf(2, []config.Route{rt("r", "", "/", "nope")}, up("u", a.URL))},
		{"malformed backend URL", cfgOf(2, []config.Route{rt("r", "", "/", "u")}, up("u", "://bad"))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, srv := startGateway(t, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", a.URL)))
			c := newClient(t)
			h.src.push(tc.cfg)

			// The bad config must never become live, and v1 keeps serving throughout.
			deadline := time.Now().Add(200 * time.Millisecond)
			for time.Now().Before(deadline) {
				if v := h.g.Version(); v != 1 {
					t.Fatalf("Version() = %d after bad config, want 1", v)
				}
				if code, body := getBody(t, c, srv.URL+"/"); code != 200 || body != "A" {
					t.Fatalf("traffic broke after bad config: %d %q", code, body)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !h.g.Ready() {
				t.Error("Ready() went false after a rejected config")
			}

			// The reconcile loop is still alive and accepts a later good config.
			b := echoUpstream(t, "B")
			h.apply(t, cfgOf(3, []config.Route{rt("r", "", "/", "u")}, up("u", b.URL)))
			if code, body := getBody(t, c, srv.URL+"/"); code != 200 || body != "B" {
				t.Fatalf("after good v3: %d %q", code, body)
			}
		})
	}
}

func TestRunReturnsNilOnShutdown(t *testing.T) {
	a := echoUpstream(t, "A")
	tests := []struct {
		name     string
		shutdown func(h *harness)
	}{
		{"ctx canceled", func(h *harness) { h.cancel() }},
		{"channel closed", func(h *harness) { h.src.closeChan() }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := startRun(t)
			h.apply(t, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", a.URL)))
			tc.shutdown(h)
			select {
			case err := <-h.done:
				if err != nil {
					t.Fatalf("Run returned %v, want nil", err)
				}
				h.done <- nil // let cleanup observe a finished Run
			case <-time.After(3 * time.Second):
				t.Fatal("Run did not return")
			}
		})
	}
}

func TestRunReturnsWatchError(t *testing.T) {
	sentinel := errors.New("watch failed")
	g := New(Options{})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- g.Run(ctx, errSource{sentinel}) }()
	select {
	case err := <-done:
		if !errors.Is(err, sentinel) {
			t.Fatalf("Run returned %v, want %v", err, sentinel)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return on Watch error")
	}
	if g.Ready() {
		t.Error("Ready() = true though no config was ever applied")
	}
}
