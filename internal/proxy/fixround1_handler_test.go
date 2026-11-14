package proxy

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// inFlight reads the in-flight count of backend raw on upstream name.
func inFlight(t *testing.T, g *Gateway, upstream, raw string) int64 {
	t.Helper()
	return g.registry.Get(upstream, mustURL(t, raw)).InFlight()
}

// directGateway builds a Gateway with cfg applied synchronously (no Run).
func directGateway(cfg *config.Config) *Gateway {
	g := New(Options{})
	g.apply(cfg)
	return g
}

// P2: a cancelled client must produce 499, not net/http's default 200.
func TestClientCancelWrites499(t *testing.T) {
	entered := make(chan struct{}, 1)
	upCanceled := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ok" {
			return
		}
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
			upCanceled <- struct{}{}
		case <-time.After(5 * time.Second):
		}
	}))
	defer upstream.Close()
	g := directGateway(cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upstream.URL)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/slow", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() { g.ServeHTTP(rec, req); close(done) }()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream never saw the request")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ServeHTTP did not return after cancel")
	}
	if rec.Code != 499 {
		t.Errorf("status = %d, want 499", rec.Code)
	}
	if rec.Header().Get(HeaderRequestID) == "" {
		t.Error("499 response lacks X-Request-ID")
	}
	select {
	case <-upCanceled:
	case <-time.After(3 * time.Second):
		t.Error("upstream context was not cancelled")
	}
	waitFor(t, 3*time.Second, "in-flight to return to 0", func() bool {
		return inFlight(t, g, "u", upstream.URL) == 0
	})
	rec2 := httptest.NewRecorder()
	g.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if rec2.Code != 200 {
		t.Errorf("follow-up status = %d", rec2.Code)
	}
}

// P3: X-Request-ID survives a 103 Early Hints response.
func TestRequestIDSurvives1xx(t *testing.T) {
	gotID := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID <- r.Header.Get(HeaderRequestID)
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.WriteHeader(http.StatusEarlyHints)
		_, _ = io.WriteString(w, "final")
	}))
	defer upstream.Close()
	_, srv := startGateway(t, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upstream.URL)))

	resp, err := newClient(t).Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	up := <-gotID
	if got := resp.Header.Get(HeaderRequestID); got == "" || got != up {
		t.Errorf("final response X-Request-ID = %q, upstream saw %q", got, up)
	}
}

// P3: every gateway-generated error carries X-Request-ID.
func TestGatewayErrorsCarryRequestID(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}))
	defer slow.Close()

	cfg := cfgOf(1,
		[]config.Route{rt("down", "", "/down", "down"), rt("slow", "", "/slow", "slow")},
		up("down", deadURL), up("slow", slow.URL))
	cfg.Routes[1].Timeouts = config.Timeouts{ResponseHeader: config.Duration(100 * time.Millisecond)}
	_, srv := startGateway(t, cfg)

	tests := []struct {
		name, path string
		want       int
	}{
		{"404", "/nomatch", 404},
		{"502", "/down", 502},
		{"504", "/slow", 504},
		{"400", "/down/../x", 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			_, _ = io.WriteString(c, "GET "+tc.path+" HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
			resp, err := http.ReadResponse(bufio.NewReader(c), nil)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
			if resp.Header.Get(HeaderRequestID) == "" {
				t.Error("missing X-Request-ID")
			}
		})
	}

	t.Run("503 not ready", func(t *testing.T) {
		rec := httptest.NewRecorder()
		New(Options{}).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
		if rec.Code != 503 || rec.Header().Get(HeaderRequestID) == "" {
			t.Errorf("code=%d id=%q", rec.Code, rec.Header().Get(HeaderRequestID))
		}
	})
}

// P4: client request IDs are validated.
func TestRequestIDSanitising(t *testing.T) {
	valid := regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	tests := []struct {
		name, in string
		keep     bool
	}{
		{"simple kept", "abc-123_x.y:z", true},
		{"128 chars kept", strings.Repeat("a", 128), true},
		{"129 chars replaced", strings.Repeat("a", 129), false},
		{"space replaced", "abc def", false},
		{"control replaced", "abc\x01def", false},
		{"non-ascii replaced", "café", false},
		{"angle brackets replaced", "a<b>c", false},
		{"comma replaced", "a,b", false},
		{"semicolon replaced", "a;b", false},
		{"quote replaced", `a"b`, false},
		{"slash replaced", "a/b", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan string, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- r.Header.Get(HeaderRequestID)
			}))
			defer upstream.Close()
			g := directGateway(cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upstream.URL)))
			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set(HeaderRequestID, tc.in) // direct map write: no client-side validation
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, req)
			got := <-seen
			if tc.keep && got != tc.in {
				t.Errorf("upstream id = %q, want kept %q", got, tc.in)
			}
			if !tc.keep && (got == tc.in || !valid.MatchString(got)) {
				t.Errorf("upstream id = %q, want a fresh valid id", got)
			}
			if rec.Header().Get(HeaderRequestID) != got {
				t.Errorf("client id %q != upstream id %q", rec.Header().Get(HeaderRequestID), got)
			}
		})
	}
}

// P5: pin the current raw-query behaviour.
func TestRawQueryHandling(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{"order and escapes preserved", "b=x%20y&a=1", "b=x%20y&a=1"},
		{"semicolon param dropped", "a=1;b=2", ""},
		{"malformed escape dropped", "a=%zz", ""},
		{"only unsafe pair dropped", "a=1&b=2;c=3&d=4", "a=1&d=4"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan string, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got <- r.URL.RawQuery
			}))
			defer upstream.Close()
			g := directGateway(cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upstream.URL)))
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, httptest.NewRequest("GET", "/p?"+tc.in, nil))
			if q := <-got; q != tc.want {
				t.Errorf("upstream RawQuery = %q, want %q", q, tc.want)
			}
		})
	}
}

// P6 (section 11 row 12): inbound client-ID header never reaches an upstream.
func TestClientIDHeaderNotForwardedWithoutAuth(t *testing.T) {
	seen := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
	}))
	defer upstream.Close()
	g := directGateway(cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", upstream.URL)))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Tollgate-Client-ID", "admin")
	req.Header["x-tollgate-client-id"] = []string{"admin2"} // non-canonical key
	g.ServeHTTP(httptest.NewRecorder(), req)
	h := <-seen
	for k, v := range h {
		if strings.EqualFold(k, "X-Tollgate-Client-ID") {
			t.Errorf("header %q = %v reached the upstream", k, v)
		}
	}
}
