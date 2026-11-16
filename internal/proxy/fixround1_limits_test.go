package proxy

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// P7: listener limits (section 11 row 14).
func TestNewServerLimits(t *testing.T) {
	safely(t, func() {
		srv := NewServer(http.NotFoundHandler())
		if srv.ReadHeaderTimeout != 5*time.Second {
			t.Errorf("ReadHeaderTimeout = %v", srv.ReadHeaderTimeout)
		}
		if srv.IdleTimeout != 120*time.Second {
			t.Errorf("IdleTimeout = %v", srv.IdleTimeout)
		}
		if srv.MaxHeaderBytes != 1<<20 {
			t.Errorf("MaxHeaderBytes = %d", srv.MaxHeaderBytes)
		}
		if srv.ReadTimeout != 0 || srv.WriteTimeout != 0 {
			t.Errorf("Read/WriteTimeout = %v/%v, want 0", srv.ReadTimeout, srv.WriteTimeout)
		}
	})
}

// Serve must enforce the header limit: more than 1 MiB of headers gets 431.
func TestServeRejectsOversizedHeaders(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), time.Second, nil)
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Logf("Serve returned: %v", err)
		}
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	big := strings.Repeat("a", 1<<20+8192)
	go func() { _, _ = io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\nX-Big: "+big+"\r\n\r\n") }()
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Errorf("status = %d, want 431", resp.StatusCode)
	}
}

func poolSize(g *Gateway) int {
	g.transports.mu.Lock()
	defer g.transports.mu.Unlock()
	return len(g.transports.transports)
}

// P7: a reload that changes a route's timeouts mid-request.
func TestTransportPoolAcrossReload(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			entered <- struct{}{}
			<-release
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	mk := func(v int64, rh time.Duration, two bool) *config.Config {
		routes := []config.Route{rt("a", "", "/", "u")}
		routes[0].Timeouts = config.Timeouts{ResponseHeader: config.Duration(rh)}
		if two {
			r2 := rt("b", "", "/b", "u")
			r2.Timeouts = config.Timeouts{Dial: config.Duration(3 * time.Second)}
			routes = append(routes, r2)
		}
		return cfgOf(v, routes, up("u", upstream.URL))
	}
	h, srv := startGateway(t, mk(1, 5*time.Second, false))

	type res struct {
		code int
		err  error
	}
	out := make(chan res, 1)
	go func() {
		resp, err := newClient(t).Get(srv.URL + "/slow")
		if err != nil {
			out <- res{err: err}
			return
		}
		_ = resp.Body.Close()
		out <- res{code: resp.StatusCode}
	}()
	<-entered
	h.apply(t, mk(2, 6*time.Second, false))
	close(release)
	if r := <-out; r.err != nil || r.code != 200 {
		t.Fatalf("in-flight request across reload: %+v", r)
	}
	waitFor(t, 2*time.Second, "pool == 1", func() bool { return poolSize(h.g) == 1 })

	h.apply(t, mk(3, 6*time.Second, true))
	if n := poolSize(h.g); n != 2 {
		t.Errorf("pool size with two distinct timeout sets = %d, want 2", n)
	}
	h.apply(t, mk(4, 7*time.Second, false))
	if n := poolSize(h.g); n != 1 {
		t.Errorf("pool size after collapsing = %d, want 1 (stale transports must be dropped)", n)
	}
}

// P8: aborted and cancelled requests must not leak in-flight counts.
func TestInFlightReleasedOnAbortPaths(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ok" {
			return
		}
		_, _ = io.WriteString(w, "partial")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer upstream.Close()
	cfg := cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upstream.URL))
	cfg.Routes[0].Timeouts = config.Timeouts{Request: config.Duration(250 * time.Millisecond)}
	h, srv := startGateway(t, cfg)

	t.Run("request timeout mid-body", func(t *testing.T) {
		resp, err := newClient(t).Get(srv.URL + "/stall")
		if err == nil {
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		waitFor(t, 3*time.Second, "in-flight 0", func() bool { return inFlight(t, h.g, "u", upstream.URL) == 0 })
	})
	t.Run("client cancel mid-body", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/stall", nil)
		resp, err := newClient(t).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 7)
		_, _ = io.ReadFull(resp.Body, buf)
		cancel()
		_ = resp.Body.Close()
		waitFor(t, 3*time.Second, "in-flight 0", func() bool { return inFlight(t, h.g, "u", upstream.URL) == 0 })
	})
	resp, err := newClient(t).Get(srv.URL + "/ok")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("follow-up failed: %v", err)
	}
	_ = resp.Body.Close()
}
