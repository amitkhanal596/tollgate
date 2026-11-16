package proxy

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// safely runs fn and turns a panic (an unimplemented stub) into a test
// failure so one stub does not abort the whole package run.
func safely(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic (stub not implemented?): %v", r)
		}
	}()
	fn()
}

// pathCases: target is the raw request-target; bad means checkPath must
// reject and the gateway must answer 400. want is the upstream that must
// serve the good ones.
var pathCases = []struct {
	name   string
	target string
	bad    bool
	want   string // "public" or "admin" for good cases
}{
	// positives
	{"root", "/", false, "public"},
	{"admin", "/admin", false, "admin"},
	{"admin trailing slash", "/admin/", false, "admin"},
	{"admin nested", "/admin/users/42", false, "admin"},
	{"dots inside segments", "/a.b/c..d", false, "public"},
	{"file with dots", "/files/report.final.pdf", false, "public"},
	{"utf8", "/caf%C3%A9", false, "public"},
	{"encoded space", "/a%20b", false, "public"},
	{"plus", "/a+b", false, "public"},
	{"tilde", "/~user", false, "public"},
	{"query not checked", "/admin?x=../..//;", false, "admin"},
	// dot segments
	{"dot segment", "/x/./y", true, ""},
	{"dotdot segment", "/x/../admin", true, ""},
	{"trailing dotdot", "/x/..", true, ""},
	{"encoded dotdot", "/x/%2e%2e/admin", true, ""},
	{"encoded dotdot upper", "/x/%2E%2E/admin", true, ""},
	// empty segments
	{"double slash admin", "//admin", true, ""},
	{"double slash middle", "/a//b", true, ""},
	// path parameters
	{"semicolon param", "/admin;jsessionid=1", true, ""},
	{"dotdot semicolon", "/x/..;/admin", true, ""},
	{"encoded dotdot semicolon", "/x/%2e%2e;/admin", true, ""},
	{"semicolon mid", "/a;b/c", true, ""},
	// backslash
	{"literal backslash", `/x/..\admin`, true, ""},
	{"encoded backslash", "/x/..%5cadmin", true, ""},
	{"encoded backslash upper", "/x/%5Cadmin", true, ""},
	// encoded slash / dot
	{"encoded slash", "/a%2fadmin", true, ""},
	{"encoded slash upper", "/a%2Fadmin", true, ""},
	{"encoded dot", "/files/a%2eb", true, ""},
	{"encoded dot upper", "/files/a%2Eb", true, ""},
	// control bytes
	{"nul", "/admin%00", true, ""},
	{"tab", "/admin%09", true, ""},
	{"crlf", "/x%0d%0a", true, ""},
	{"del", "/x%7f", true, ""},
}

func TestCheckPath(t *testing.T) {
	for _, tc := range pathCases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.ParseRequestURI(tc.target)
			if err != nil {
				t.Skipf("target not parseable by net/url (server rejects it itself): %v", err)
			}
			safely(t, func() {
				err := checkPath(u)
				if tc.bad && err == nil {
					t.Errorf("checkPath(%q) = nil, want error", tc.target)
				}
				if !tc.bad && err != nil {
					t.Errorf("checkPath(%q) = %v, want nil", tc.target, err)
				}
			})
		})
	}
}

// rawGet sends target verbatim as the request line (no client-side
// cleaning) and returns the parsed response.
func rawGet(t *testing.T, addr, target string) *http.Response {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: gw.test\r\nConnection: close\r\n\r\n", target)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("reading response for %q: %v", target, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// hitCounter is an upstream that records every request URI it receives.
type hitCounter struct {
	srv  *httptest.Server
	mu   sync.Mutex
	uris []string
}

func newHitCounter(t *testing.T, name string) *hitCounter {
	h := &hitCounter{}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.uris = append(h.uris, r.RequestURI)
		h.mu.Unlock()
		_, _ = w.Write([]byte(name))
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *hitCounter) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.uris)
}

func TestGatewayPathSafety(t *testing.T) {
	pub, adm := newHitCounter(t, "public"), newHitCounter(t, "admin")
	cfg := cfgOf(1,
		[]config.Route{rt("pub", "", "/", "pub"), rt("adm", "", "/admin", "adm")},
		up("pub", pub.srv.URL), up("adm", adm.srv.URL))
	_, srv := startGateway(t, cfg)
	addr := strings.TrimPrefix(srv.URL, "http://")

	for _, tc := range pathCases {
		t.Run(tc.name, func(t *testing.T) {
			pb, ab := pub.count(), adm.count()
			resp := rawGet(t, addr, tc.target)
			body := make([]byte, 64)
			n, _ := resp.Body.Read(body)
			got := string(body[:n])
			if tc.bad {
				if resp.StatusCode != http.StatusBadRequest {
					t.Errorf("status = %d, want 400", resp.StatusCode)
				}
				if pub.count() != pb || adm.count() != ab {
					t.Errorf("an upstream received the request for %q", tc.target)
				}
				if len(got) > 40 || strings.Contains(got, "admin") {
					t.Errorf("400 body should be small and generic, got %q", got)
				}
				return
			}
			if resp.StatusCode != 200 || got != tc.want {
				t.Errorf("got %d %q, want 200 %q", resp.StatusCode, got, tc.want)
			}
		})
	}
}
