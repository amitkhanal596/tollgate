package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/amitkhanal596/tollgate/internal/config"
)

func serveRec(g *Gateway, host, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "http://placeholder"+path, nil)
	r.Host = host
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, r)
	return rec
}

func TestRouting(t *testing.T) {
	names := []string{"root", "api", "apiv2", "hostapi", "wildh"}
	srvs := map[string]string{}
	for _, n := range names {
		srvs[n] = echoUpstream(t, n).URL
	}
	cfg := cfgOf(1, nil)
	for _, n := range names {
		cfg.Upstreams = append(cfg.Upstreams, up(n, srvs[n]))
	}
	cfg.Routes = append(cfg.Routes,
		rt("root", "", "/", "root"),
		rt("api", "", "/api", "api"),
		rt("apiv2", "", "/api/v2", "apiv2"),
		rt("hostapi", "api.example.com", "/h", "hostapi"),
		rt("wildh", "", "/h/deep", "wildh"),
	)
	h := startRun(t)
	h.apply(t, cfg)

	tests := []struct {
		name, host, path, want string
	}{
		{"root", "any.test", "/", "root"},
		{"unmatched path falls to root", "any.test", "/other", "root"},
		{"api exact", "any.test", "/api", "api"},
		{"api subpath", "any.test", "/api/x", "api"},
		{"api trailing slash", "any.test", "/api/", "api"},
		{"segment aware apix", "any.test", "/apix", "root"},
		{"segment aware api2", "any.test", "/api2/v2", "root"},
		{"longest prefix v2", "any.test", "/api/v2", "apiv2"},
		{"longest prefix v2 sub", "any.test", "/api/v2/users", "apiv2"},
		{"v20 is not v2", "any.test", "/api/v20", "api"},
		{"exact host beats longer wildcard prefix", "api.example.com", "/h/deep/x", "hostapi"},
		{"exact host route", "api.example.com", "/h", "hostapi"},
		{"other host uses wildcard", "other.example.com", "/h/deep", "wildh"},
		{"host case-insensitive", "API.Example.COM", "/h/x", "hostapi"},
		{"host port ignored", "api.example.com:8443", "/h/x", "hostapi"},
		{"host case and port", "API.EXAMPLE.com:80", "/h", "hostapi"},
		{"host route not matching path falls back to wildcard", "api.example.com", "/api/x", "api"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := serveRec(h.g, tc.host, tc.path)
			body, _ := io.ReadAll(rec.Body)
			if rec.Code != http.StatusOK || string(body) != tc.want {
				t.Fatalf("host=%q path=%q: got %d %q, want 200 %q", tc.host, tc.path, rec.Code, body, tc.want)
			}
		})
	}
}

func TestRoutingNoMatch404(t *testing.T) {
	a := echoUpstream(t, "a")
	cfg := cfgOf(1, []config.Route{
		rt("api", "", "/api", "a"),
		rt("host", "only.example.com", "/", "a"),
	}, up("a", a.URL))
	h := startRun(t)
	h.apply(t, cfg)

	for _, tc := range []struct{ name, host, path string }{
		{"path outside prefix", "x.test", "/other"},
		{"segment boundary", "x.test", "/apix"},
		{"host-specific route for other host", "x.test", "/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if rec := serveRec(h.g, tc.host, tc.path); rec.Code != http.StatusNotFound {
				t.Fatalf("got %d, want 404", rec.Code)
			}
		})
	}
}

func TestNotReadyBeforeFirstConfig(t *testing.T) {
	g := New(Options{})
	if g.Ready() {
		t.Fatal("Ready() = true before any config")
	}
	if v := g.Version(); v != 0 {
		t.Fatalf("Version() = %d, want 0", v)
	}
	if rec := serveRec(g, "x.test", "/"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", rec.Code)
	}

	// Once the first config is applied the gateway becomes ready.
	h := startRun(t)
	if h.g.Ready() {
		t.Fatal("Ready() = true before first push")
	}
	a := echoUpstream(t, "a")
	h.apply(t, cfgOf(7, []config.Route{rt("r", "", "/", "a")}, up("a", a.URL)))
	if !h.g.Ready() {
		t.Fatal("Ready() = false after first config applied")
	}
	if rec := serveRec(h.g, "x.test", "/"); rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rec.Code)
	}
}
