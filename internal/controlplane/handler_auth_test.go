package controlplane

import (
	"net/http"
	"strings"
	"testing"
)

// adminEndpoint is every /v1 endpoint that takes the ADMIN token.
type endpoint struct {
	method, path, body string
}

var adminEndpoints = []endpoint{
	{"GET", "/v1/config", ""},
	{"PUT", "/v1/config", seedJSON},
	{"GET", "/v1/config/versions", ""},
	{"GET", "/v1/config/versions/1", ""},
	{"POST", "/v1/config/rollback", `{}`},
	{"GET", "/v1/routes", ""},
	{"GET", "/v1/routes/api", ""},
	{"PUT", "/v1/routes/api", `{"path_prefix":"/api","upstream":"web"}`},
	{"DELETE", "/v1/routes/api", ""},
	{"GET", "/v1/upstreams", ""},
	{"GET", "/v1/upstreams/web", ""},
	{"PUT", "/v1/upstreams/web", `{"backends":["http://127.0.0.1:9001"]}`},
	{"DELETE", "/v1/upstreams/web", ""},
	{"GET", "/v1/dataplanes", ""},
}

// dataPlaneEndpoints take the DATA-PLANE token only.
var dataPlaneEndpoints = []endpoint{
	{"GET", "/v1/config/stream?node=gw-1", ""},
	{"POST", "/v1/dataplanes/gw-1/status", `{"applied":1}`},
}

func TestAuth_AdminEndpointsRejectBadCredentials(t *testing.T) {
	defer failOnPanic(t)
	creds := []struct {
		name   string
		header []string // Authorization header (empty = none)
	}{
		{"no header", nil},
		{"wrong token", []string{"Authorization", "Bearer " + strings.Repeat("x", 44)}},
		{"data-plane token", []string{"Authorization", "Bearer " + dpTok}},
		{"admin token as Basic", []string{"Authorization", "Basic " + adminTok}},
		{"bare token no scheme", []string{"Authorization", adminTok}},
		{"empty bearer", []string{"Authorization", "Bearer "}},
		{"two spaces after scheme", []string{"Authorization", "Bearer  " + adminTok}},
		{"token with trailing space", []string{"Authorization", "Bearer " + adminTok + " "}},
		{"prefix of the token", []string{"Authorization", "Bearer " + adminTok[:len(adminTok)-1]}},
		{"token plus suffix", []string{"Authorization", "Bearer " + adminTok + "x"}},
	}
	for _, ep := range adminEndpoints {
		for _, c := range creds {
			run(t, ep.method+" "+ep.path+" / "+c.name, func(t *testing.T) {
				e := newEnv(t)
				rec := do(t, e.h, ep.method, ep.path, "", ep.body, c.header...)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status %d, want 401; body %s", rec.Code, rec.Body)
				}
				if wa := rec.Header().Get("WWW-Authenticate"); !strings.HasPrefix(wa, "Bearer") {
					t.Fatalf("WWW-Authenticate = %q, want Bearer...", wa)
				}
				// Same body for every failure mode (rule 3).
				if got := strings.TrimSpace(rec.Body.String()); got != `{"error":"unauthorized"}` {
					t.Fatalf("body = %s", got)
				}
				if v := e.headVersion(t); v != 0 {
					t.Fatalf("unauthorized request wrote version %d", v)
				}
			})
		}
	}
}

func TestAuth_AdminTokenWorksEverywhere(t *testing.T) {
	defer failOnPanic(t)
	for _, ep := range adminEndpoints {
		run(t, ep.method+" "+ep.path, func(t *testing.T) {
			e := newEnv(t)
			rec := e.admin(t, ep.method, ep.path, ep.body)
			if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden || rec.Code == http.StatusMethodNotAllowed {
				t.Fatalf("admin token refused: status %d body %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestAuth_SchemeIsCaseInsensitive(t *testing.T) {
	defer failOnPanic(t)
	for _, scheme := range []string{"Bearer", "bearer", "BEARER", "bEaReR"} {
		run(t, scheme, func(t *testing.T) {
			e := newEnv(t)
			rec := do(t, e.h, "GET", "/v1/config", "", "", "Authorization", scheme+" "+adminTok)
			if rec.Code != 200 {
				t.Fatalf("status %d", rec.Code)
			}
		})
	}
}

func TestAuth_TokenInQueryIsIgnored(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	for _, q := range []string{"?token=", "?access_token=", "?admin_token="} {
		rec := do(t, e.h, "GET", "/v1/config"+q+adminTok, "", "")
		if rec.Code != 401 {
			t.Fatalf("%s: status %d, want 401", q, rec.Code)
		}
	}
}

func TestAuth_DataPlaneEndpointsRejectAdminToken(t *testing.T) {
	defer failOnPanic(t)
	for _, ep := range dataPlaneEndpoints {
		for name, tok := range map[string]string{"no token": "", "admin token": adminTok, "wrong token": strings.Repeat("z", 44)} {
			run(t, ep.method+" "+ep.path+" / "+name, func(t *testing.T) {
				e := newEnv(t)
				rec := do(t, e.h, ep.method, ep.path, tok, ep.body)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status %d, want 401", rec.Code)
				}
				if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer") {
					t.Fatalf("WWW-Authenticate = %q", rec.Header().Get("WWW-Authenticate"))
				}
				if got := strings.TrimSpace(rec.Body.String()); got != `{"error":"unauthorized"}` {
					t.Fatalf("body = %s", got)
				}
				if len(e.nodes.List()) != 0 {
					t.Fatalf("unauthorized request registered a node")
				}
			})
		}
	}
}

func TestAuth_TokenCheckComesBeforeValidation(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	// Rule 3.2: token -> syntax -> body. A bad node / bad body without a
	// token is 401, not 400.
	cases := []endpoint{
		{"GET", "/v1/config/stream", ""}, // node missing
		{"GET", "/v1/config/stream?node=!!&since=x", ""},
		{"POST", "/v1/dataplanes/-bad/status", `{`},
		{"PUT", "/v1/config", `{`},
		{"GET", "/v1/config/versions?limit=abc", ""},
		{"GET", "/v1/config/versions/abc", ""},
		{"DELETE", "/v1/routes/does-not-exist", ""}, // would be 404
	}
	for _, c := range cases {
		if rec := do(t, e.h, c.method, c.path, "", c.body); rec.Code != 401 {
			t.Errorf("%s %s without token: status %d, want 401", c.method, c.path, rec.Code)
		}
	}
}

func TestRouting_MuxErrorsComeBeforeAuth(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	run(t, "unknown path is 404 even without a token", func(t *testing.T) {
		for _, p := range []string{"/nope", "/v1/nope", "/v1/config/versions/1/extra", "/v1/routes/a/b"} {
			if rec := do(t, e.h, "GET", p, "", ""); rec.Code != 404 {
				t.Errorf("GET %s: status %d, want 404", p, rec.Code)
			}
		}
	})
	run(t, "wrong method on a known path is 405 with Allow, even without a token", func(t *testing.T) {
		for _, c := range []endpoint{
			{"DELETE", "/v1/config", ""},
			{"POST", "/v1/config", ""},
			{"PATCH", "/v1/config", ""},
			{"PUT", "/v1/config/versions", ""},
			{"POST", "/v1/config/versions/1", ""},
			{"GET", "/v1/config/rollback", ""},
			{"PUT", "/v1/config/rollback", ""},
			{"POST", "/v1/routes", ""},
			{"PUT", "/v1/routes", ""},
			{"DELETE", "/v1/routes", ""},
			{"POST", "/v1/routes/x", ""},
			{"PATCH", "/v1/routes/x", ""},
			{"POST", "/v1/upstreams", ""},
			{"POST", "/v1/upstreams/x", ""},
			{"POST", "/v1/dataplanes", ""},
			{"GET", "/v1/dataplanes/gw-1/status", ""},
			{"PUT", "/v1/dataplanes/gw-1/status", ""},
			{"POST", "/v1/config/stream", ""},
			{"PUT", "/v1/config/stream", ""},
			{"POST", "/healthz", ""},
			{"POST", "/readyz", ""},
		} {
			rec := do(t, e.h, c.method, c.path, "", c.body)
			if rec.Code != 405 {
				t.Errorf("%s %s: status %d, want 405", c.method, c.path, rec.Code)
				continue
			}
			if rec.Header().Get("Allow") == "" {
				t.Errorf("%s %s: 405 without Allow header", c.method, c.path)
			}
		}
	})
	run(t, "wrong method with a valid token is still 405", func(t *testing.T) {
		if rec := e.admin(t, "DELETE", "/v1/config", ""); rec.Code != 405 {
			t.Errorf("status %d, want 405", rec.Code)
		}
	})
}

func TestAuth_HealthzNeedsNoToken(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	rec := do(t, e.h, "GET", "/healthz", "", "")
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Fatalf("/healthz = %d %q", rec.Code, rec.Body)
	}
}

func TestAuth_TokensNeverAppearInResponses(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	e.seed(t)
	e.nodes.Report("gw-1", StatusReport{Applied: 1})
	var all []endpoint
	all = append(all, adminEndpoints...)
	all = append(all, endpoint{"POST", "/v1/dataplanes/gw-1/status", `{"applied":1}`})
	all = append(all, endpoint{"GET", "/healthz", ""}, endpoint{"GET", "/readyz", ""})
	for _, ep := range all {
		for _, tok := range []string{"", adminTok, dpTok} {
			rec := do(t, e.h, ep.method, ep.path, tok, ep.body)
			hdrs := rec.Header()
			dump := rec.Body.String()
			for k, vs := range hdrs {
				dump += k + ": " + strings.Join(vs, ",") + "\n"
			}
			if strings.Contains(dump, adminTok) || strings.Contains(dump, dpTok) {
				t.Errorf("%s %s (token %.6q): response contains a token:\n%s", ep.method, ep.path, tok, dump)
			}
		}
	}
}

func TestNewHandler_ValidatesOptions(t *testing.T) {
	defer failOnPanic(t)
	good := func() Options {
		return Options{Store: NewMemStore(), Hub: NewHub(), Nodes: NewNodes(nil, 0), AdminToken: adminTok, DataPlaneToken: dpTok}
	}
	cases := []struct {
		name    string
		mutate  func(*Options)
		wantErr bool
	}{
		{"valid", func(*Options) {}, false},
		{"tokens exactly MinTokenBytes", func(o *Options) {
			o.AdminToken = strings.Repeat("a", MinTokenBytes)
			o.DataPlaneToken = strings.Repeat("b", MinTokenBytes)
		}, false},
		{"admin token one byte short", func(o *Options) { o.AdminToken = strings.Repeat("a", MinTokenBytes-1) }, true},
		{"data-plane token one byte short", func(o *Options) { o.DataPlaneToken = strings.Repeat("b", MinTokenBytes-1) }, true},
		{"admin token empty", func(o *Options) { o.AdminToken = "" }, true},
		{"data-plane token empty", func(o *Options) { o.DataPlaneToken = "" }, true},
		{"tokens equal", func(o *Options) { o.DataPlaneToken = o.AdminToken }, true},
		{"nil store", func(o *Options) { o.Store = nil }, true},
		{"nil hub", func(o *Options) { o.Hub = nil }, true},
		{"nil nodes", func(o *Options) { o.Nodes = nil }, true},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			o := good()
			tc.mutate(&o)
			h, err := NewHandler(o)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewHandler succeeded, want an error")
				}
				if h != nil {
					t.Fatalf("NewHandler returned a handler together with error %v", err)
				}
				for _, tok := range []string{o.AdminToken, o.DataPlaneToken} {
					if tok != "" && strings.Contains(err.Error(), tok) {
						t.Fatalf("error message leaks a token: %v", err)
					}
				}
				return
			}
			if err != nil || h == nil {
				t.Fatalf("NewHandler = %v, %v", h, err)
			}
		})
	}
}
