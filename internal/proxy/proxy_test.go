package proxy

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// seenReq is what an upstream observed about one request.
type seenReq struct {
	method, uri, host, body string
	header                  http.Header
}

func captureUpstream(t *testing.T, status int, respBody string) (*httptest.Server, chan seenReq) {
	t.Helper()
	ch := make(chan seenReq, 16)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ch <- seenReq{r.Method, r.RequestURI, r.Host, string(b), r.Header.Clone()}
		w.Header().Set("X-Upstream", "yes")
		w.Header().Set("X-Request-ID-Echo", r.Header.Get("X-Request-ID"))
		w.WriteHeader(status)
		if respBody != "" {
			_, _ = io.WriteString(w, respBody)
		} else {
			_, _ = w.Write(b)
		}
	}))
	t.Cleanup(s.Close)
	return s, ch
}

func singleRouteGateway(t *testing.T, backends ...string) *httptest.Server {
	t.Helper()
	_, srv := startGateway(t, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", backends...)))
	return srv
}

func recv(t *testing.T, ch chan seenReq) seenReq {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(3 * time.Second):
		t.Fatal("upstream saw no request")
		return seenReq{}
	}
}

func TestProxyForwardsRequestAndResponse(t *testing.T) {
	upstream, seen := captureUpstream(t, http.StatusCreated, "")
	srv := singleRouteGateway(t, upstream.URL)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/echo/p%20q?a=1&b=x%20y", strings.NewReader("hello world"))
	req.Header.Set("X-Custom", "abc")
	resp, err := newClient(t).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	got := recv(t, seen)
	if got.method != http.MethodPost || got.uri != "/echo/p%20q?a=1&b=x%20y" || got.body != "hello world" {
		t.Errorf("upstream saw %s %s body=%q", got.method, got.uri, got.body)
	}
	if got.header.Get("X-Custom") != "abc" {
		t.Errorf("custom request header not forwarded: %v", got.header)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("status = %d, want 201", resp.StatusCode)
	}
	if resp.Header.Get("X-Upstream") != "yes" {
		t.Errorf("upstream response header not relayed: %v", resp.Header)
	}
	if string(body) != "hello world" {
		t.Errorf("body = %q, want echoed request body", body)
	}
}

func TestProxyUpstreamStatusRelayed(t *testing.T) {
	for _, code := range []int{200, 204, 301, 400, 418, 500, 503} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer upstream.Close()
			srv := singleRouteGateway(t, upstream.URL)
			c := newClient(t)
			c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			resp, err := c.Get(srv.URL + "/")
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != code {
				t.Fatalf("status = %d, want %d (upstream status must pass through)", resp.StatusCode, code)
			}
		})
	}
}

func TestProxyForwardedHeadersAndHost(t *testing.T) {
	upstream, seen := captureUpstream(t, 200, "ok")
	srv := singleRouteGateway(t, upstream.URL)
	backendHost := mustHost(t, upstream.URL)
	gatewayHost := mustHost(t, srv.URL)

	tests := []struct {
		name    string
		inXFF   string
		wantXFF string
	}{
		{"no incoming XFF", "", "127.0.0.1"},
		{"incoming XFF kept and client IP appended", "10.1.1.1", "10.1.1.1, 127.0.0.1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/x", nil)
			if tc.inXFF != "" {
				req.Header.Set("X-Forwarded-For", tc.inXFF)
			}
			resp, err := newClient(t).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			got := recv(t, seen)
			if v := got.header.Get("X-Forwarded-For"); v != tc.wantXFF {
				t.Errorf("X-Forwarded-For = %q, want %q", v, tc.wantXFF)
			}
			if v := got.header.Get("X-Forwarded-Host"); v != gatewayHost {
				t.Errorf("X-Forwarded-Host = %q, want original host %q", v, gatewayHost)
			}
			if v := got.header.Get("X-Forwarded-Proto"); v != "http" {
				t.Errorf("X-Forwarded-Proto = %q, want http", v)
			}
			if got.host != backendHost {
				t.Errorf("upstream saw Host %q, want backend host %q", got.host, backendHost)
			}
		})
	}
}

func TestProxyStripsHopByHopHeaders(t *testing.T) {
	upstream, seen := captureUpstream(t, 200, "ok")
	srv := singleRouteGateway(t, upstream.URL)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	req.Header.Set("Connection", "X-Custom-Hop")
	req.Header.Set("X-Custom-Hop", "secret")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("Proxy-Authorization", "Basic abc")
	req.Header.Set("TE", "gzip") // "TE: trailers" is deliberately kept by httputil, so use another value
	req.Header.Set("Upgrade", "foo")
	req.Header.Set("X-End-To-End", "kept")
	resp, err := newClient(t).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	got := recv(t, seen)
	for _, h := range []string{"X-Custom-Hop", "Keep-Alive", "Proxy-Authorization", "Te", "Upgrade"} {
		if v := got.header.Get(h); v != "" {
			t.Errorf("hop-by-hop header %s forwarded with value %q", h, v)
		}
	}
	if got.header.Get("X-End-To-End") != "kept" {
		t.Errorf("end-to-end header dropped: %v", got.header)
	}
}

func TestProxyRequestID(t *testing.T) {
	tests := []struct {
		name, in string
	}{
		{"generated when absent", ""},
		{"propagated when supplied", "client-supplied-123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			upstream, seen := captureUpstream(t, 200, "ok")
			srv := singleRouteGateway(t, upstream.URL)
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
			if tc.in != "" {
				req.Header.Set("X-Request-ID", tc.in)
			}
			resp, err := newClient(t).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			up := recv(t, seen).header.Get("X-Request-ID")
			down := resp.Header.Get("X-Request-ID")
			if up == "" || down == "" {
				t.Fatalf("X-Request-ID missing: upstream=%q client=%q", up, down)
			}
			if up != down {
				t.Errorf("upstream id %q != client id %q", up, down)
			}
			if tc.in != "" && up != tc.in {
				t.Errorf("supplied id changed: got %q want %q", up, tc.in)
			}
		})
	}
}

func TestProxyRoundRobinDistribution(t *testing.T) {
	var counts [3]atomic.Int64
	var backends []string
	for i := range counts {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			counts[i].Add(1)
		}))
		defer s.Close()
		backends = append(backends, s.URL)
	}
	srv := singleRouteGateway(t, backends...)
	c := newClient(t)
	for i := 0; i < 30; i++ {
		resp, err := c.Get(srv.URL + "/")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	for i := range counts {
		if n := counts[i].Load(); n != 10 {
			t.Errorf("backend %d served %d requests, want 10", i, n)
		}
	}
}

func TestProxyStreamsResponseIncrementally(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "first\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-time.After(5 * time.Second):
		}
		_, _ = io.WriteString(w, "second\n")
	}))
	defer upstream.Close()
	defer releaseOnce()
	srv := singleRouteGateway(t, upstream.URL)

	resp, err := newClient(t).Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	type res struct {
		line string
		err  error
	}
	first := make(chan res, 1)
	br := bufio.NewReader(resp.Body)
	go func() {
		l, err := br.ReadString('\n')
		first <- res{l, err}
	}()
	select {
	case r := <-first:
		if r.err != nil || r.line != "first\n" {
			t.Fatalf("first chunk = %q, err %v", r.line, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first chunk not delivered before upstream finished: response is buffered, not streamed")
	}
	releaseOnce()
	rest, err := io.ReadAll(br)
	if err != nil || string(rest) != "second\n" {
		t.Fatalf("rest = %q, err %v", rest, err)
	}
}

func mustHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}
