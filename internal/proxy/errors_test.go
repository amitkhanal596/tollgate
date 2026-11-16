package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// slowHandler waits d (or until the request is canceled) and then replies "slow".
func slowHandler(d time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(d):
			_, _ = io.WriteString(w, "slow")
		case <-r.Context().Done():
		}
	}
}

// assertSafeErrorBody checks an error body is small and does not leak backend addresses.
func assertSafeErrorBody(t *testing.T, body []byte, backendURL string) {
	t.Helper()
	if len(body) > 1024 {
		t.Errorf("error body is %d bytes, want a small body", len(body))
	}
	host := mustHost(t, backendURL)
	_, port, _ := net.SplitHostPort(host)
	if strings.Contains(string(body), host) || (port != "" && strings.Contains(string(body), port)) {
		t.Errorf("error body leaks backend address %q: %q", host, body)
	}
}

func TestErrorBackendDown502(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := "http://" + ln.Addr().String()
	_ = ln.Close() // nothing listens any more

	srv := singleRouteGateway(t, dead)
	resp, err := newClient(t).Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	assertSafeErrorBody(t, body, dead)
}

func TestErrorResponseHeaderTimeout504(t *testing.T) {
	const timeout = 200 * time.Millisecond
	upstream := httptest.NewServer(slowHandler(3 * time.Second))
	defer upstream.Close()

	cfg := cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upstream.URL))
	cfg.Routes[0].Timeouts = config.Timeouts{ResponseHeader: config.Duration(timeout), Request: config.Duration(5 * time.Second)}
	_, srv := startGateway(t, cfg)

	start := time.Now()
	resp, err := newClient(t).Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	elapsed := time.Since(start)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", resp.StatusCode)
	}
	if elapsed < timeout/2 || elapsed > timeout+1500*time.Millisecond {
		t.Errorf("504 after %v, want about %v", elapsed, timeout)
	}
	assertSafeErrorBody(t, body, upstream.URL)
}

func TestErrorRequestTimeoutMidBody(t *testing.T) {
	const timeout = 300 * time.Millisecond
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "partial")
		w.(http.Flusher).Flush()
		select { // stall after headers
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer upstream.Close()

	cfg := cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upstream.URL))
	cfg.Routes[0].Timeouts = config.Timeouts{Request: config.Duration(timeout)}
	_, srv := startGateway(t, cfg)

	start := time.Now()
	resp, err := newClient(t).Get(srv.URL + "/")
	var readErr error
	if err == nil {
		defer func() { _ = resp.Body.Close() }()
		_, readErr = io.ReadAll(resp.Body)
	}
	elapsed := time.Since(start)
	if err == nil && readErr == nil {
		t.Fatal("client read the whole body; the stalled response should have been cut at the request timeout")
	}
	if elapsed > timeout+1500*time.Millisecond {
		t.Errorf("request ended after %v, want about %v", elapsed, timeout)
	}
}

func TestErrorClientCancelPropagatesToUpstream(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan struct{})
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) > 1 {
			return // follow-up request after the cancel answers immediately
		}
		close(entered)
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-time.After(5 * time.Second):
		}
	}))
	defer upstream.Close()
	srv := singleRouteGateway(t, upstream.URL)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/", nil)
	errc := make(chan error, 1)
	go func() {
		resp, err := newClient(t).Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		errc <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream never saw the request")
	}
	cancel()
	if err := <-errc; err == nil {
		t.Error("canceled client request returned no error")
	}
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream request context was not canceled after the client went away")
	}

	// The gateway survives: a fresh request is still accepted and answered.
	resp, err := newClient(t).Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("gateway not serving after client cancel: %v", err)
	}
	_ = resp.Body.Close()
}

func TestErrorPerRouteTimeoutOverridesDefaults(t *testing.T) {
	upstream := httptest.NewServer(slowHandler(500 * time.Millisecond))
	defer upstream.Close()

	cfg := cfgOf(1, []config.Route{
		rt("inherits", "", "/default", "u"),
		rt("overrides", "", "/long", "u"),
	}, up("u", upstream.URL))
	cfg.Defaults.Timeouts.ResponseHeader = config.Duration(100 * time.Millisecond)
	cfg.Routes[1].Timeouts.ResponseHeader = config.Duration(3 * time.Second)
	_, srv := startGateway(t, cfg)

	tests := []struct {
		path string
		want int
	}{
		{"/default", http.StatusGatewayTimeout},
		{"/long", http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := newClient(t).Get(srv.URL + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}
