package controlplane

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// countingStore counts write calls that reach the wrapped Store.
type countingStore struct {
	Store
	writes atomic.Int64
}

func (c *countingStore) Update(ctx context.Context, req UpdateRequest, m func(*config.Config) error) (*config.Config, error) {
	c.writes.Add(1)
	return c.Store.Update(ctx, req, m)
}

func (c *countingStore) Rollback(ctx context.Context, to int64, req UpdateRequest) (*config.Config, error) {
	c.writes.Add(1)
	return c.Store.Rollback(ctx, to, req)
}

// drainPublished returns the highest version pending on sub (0 if none).
func drainPublished(sub <-chan *config.Config) int64 {
	var hi int64
	for {
		c, ok := tryRecv(sub)
		if !ok {
			return hi
		}
		hi = max(hi, c.Version)
	}
}

func bigUpstreamBody(name string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"name":%q,"backends":[`, name)
	for i, be := range backendsOfSize(name, bigUpstreamBytes) {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%q", be)
	}
	b.WriteString("]}")
	return b.String()
}

// C1 over HTTP: the PUT that pushes the document past MaxConfigBytes is a 400
// naming the limit; the head is unchanged and nothing is published.
func TestFixRound4_HTTPTotalSizeCap(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	sub, cancel := e.hub.Subscribe()
	defer cancel()

	crossing := MaxConfigBytes/bigUpstreamBytes + 1
	for i := 1; i <= crossing; i++ {
		name := fmt.Sprintf("big%d", i)
		body := bigUpstreamBody(name)
		if len(body) > MaxBodyBytes {
			t.Fatalf("fixture body %d bytes exceeds MaxBodyBytes", len(body))
		}
		rec := e.admin(t, "PUT", "/v1/upstreams/"+name, body)
		if i < crossing {
			if rec.Code != 201 {
				t.Fatalf("PUT %d: status %d: %.200s", i, rec.Code, rec.Body)
			}
			continue
		}
		if rec.Code != 400 {
			t.Fatalf("PUT %d crossing the cap: status %d, want 400: %.200s", i, rec.Code, rec.Body)
		}
		msg := errBody(t, rec)
		if !strings.Contains(msg, fmt.Sprint(MaxConfigBytes)) && !strings.Contains(msg, "4 MiB") && !strings.Contains(msg, "4MiB") {
			t.Errorf("error %q does not name the limit (%d bytes / 4 MiB)", msg, MaxConfigBytes)
		}
	}
	if v := e.headVersion(t); v != int64(crossing-1) {
		t.Fatalf("head version %d after rejected write, want %d", v, crossing-1)
	}
	if hi := drainPublished(sub); hi != int64(crossing-1) {
		t.Fatalf("highest published version %d, want %d (rejected write must not publish)", hi, crossing-1)
	}
	if v := e.putUpstream(t, "small"); v != int64(crossing) {
		t.Fatalf("next version %d, want %d (rejected write consumed a version)", v, crossing)
	}
}

// C3 over HTTP: strings Postgres cannot store are a 400, whatever the store.
func TestFixRound4_HTTPNULInConfigIs400(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct{ name, body string }{
		{"consul service", `{"upstreams":[{"name":"c","consul":{"service":"svc\u0000x"}}]}`},
		{"consul tag", `{"upstreams":[{"name":"c","consul":{"service":"svc","tag":"a\u0000"}}]}`},
		{"jwt issuer", `{"auth":{"jwt":{"algorithm":"HS256","hmac_secret_env":"S","issuer":"i\u0000"}}}`},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			// Control: the same document without the NUL is accepted, so a
			// failure below is about the NUL and nothing else.
			ok := newEnv(t)
			if rec := ok.admin(t, "PUT", "/v1/config", strings.ReplaceAll(tc.body, `\u0000`, "x")); rec.Code != 200 {
				t.Fatalf("control document rejected: %d %s", rec.Code, rec.Body)
			}

			e := newEnv(t)
			e.seed(t)
			sub, cancel := e.hub.Subscribe()
			defer cancel()
			drainPublished(sub)
			rec := e.admin(t, "PUT", "/v1/config", tc.body)
			if rec.Code != 400 {
				t.Fatalf("status %d, want 400: %s", rec.Code, rec.Body)
			}
			errBody(t, rec)
			if v := e.headVersion(t); v != 1 {
				t.Fatalf("head version %d, want 1", v)
			}
			if hi := drainPublished(sub); hi != 0 {
				t.Fatalf("published v%d for a rejected write", hi)
			}
		})
	}
}

// C3: audit headers that are not valid UTF-8 or carry control characters are a
// 400 before the store is touched (Postgres TEXT would fail them with a 500).
func TestFixRound4_AuditHeaderValidation(t *testing.T) {
	defer failOnPanic(t)
	bad := []struct{ name, val string }{
		{"NUL", "a\x00b"},
		{"SOH", "a\x01b"},
		{"US", "a\x1fb"},
		{"DEL", "a\x7fb"},
		{"LF", "a\nb"},
		{"CR", "a\rb"},
		{"lone continuation byte", "a\x80b"},
		{"truncated sequence", "ab\xc3("},
		{"0xff", "\xff"},
		{"UTF-16 surrogate", "\xed\xa0\x80"},
	}
	good := []string{"alice", "José Müller", "日本語", "ops team <ops@example.com>"}
	headers := []string{HeaderAuthor, HeaderComment}
	endpoints := []struct {
		name, method, path, body string
		needsHistory             bool
	}{
		{"PUT config", "PUT", "/v1/config", seedJSON, false},
		{"PUT route", "PUT", "/v1/routes/api", routeJSON("api", "/api", "web"), false},
		{"PUT upstream", "PUT", "/v1/upstreams/web", upstreamJSON("web"), false},
		{"DELETE route", "DELETE", "/v1/routes/api", "", false},
		{"POST rollback", "POST", "/v1/config/rollback", "", true},
	}
	for _, ep := range endpoints {
		for _, h := range headers {
			for _, b := range bad {
				run(t, fmt.Sprintf("%s/%s/%s", ep.name, h, b.name), func(t *testing.T) {
					cs := &countingStore{Store: NewMemStore()}
					e := newEnvWithStore(t, cs)
					e.seed(t)
					e.putUpstream(t, "extra") // v2, so rollback has a target
					before := cs.writes.Load()
					rec := e.admin(t, ep.method, ep.path, ep.body, h, b.val)
					if rec.Code != 400 {
						t.Fatalf("status %d, want 400: %s", rec.Code, rec.Body)
					}
					errBody(t, rec)
					if n := cs.writes.Load(); n != before {
						t.Fatalf("store received %d write call(s) for a rejected header", n-before)
					}
					if v := e.headVersion(t); v != 2 {
						t.Fatalf("head version %d, want 2", v)
					}
				})
			}
		}
	}
	// Valid UTF-8 labels keep working (and round-trip through the version list).
	for _, g := range good {
		run(t, "valid label "+g, func(t *testing.T) {
			e := newEnv(t)
			rec := e.admin(t, "PUT", "/v1/config", seedJSON, HeaderAuthor, g, HeaderComment, g)
			if rec.Code != 200 {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			var list VersionList
			mustJSON(t, e.admin(t, "GET", "/v1/config/versions", ""), &list)
			if len(list.Versions) != 1 || list.Versions[0].Author != g || list.Versions[0].Comment != g {
				t.Fatalf("versions = %+v, want author and comment %q", list.Versions, g)
			}
		})
	}
}

// C4: a JSON body of null decodes "successfully" into a zero struct. For
// PUT /v1/config that silently replaces the whole config with an empty one;
// for rollback it silently means "head - 1". Both must be a 400.
func TestFixRound4_NullBodyIs400(t *testing.T) {
	defer failOnPanic(t)
	endpoints := []struct{ name, method, path string }{
		{"PUT config", "PUT", "/v1/config"},
		{"PUT route", "PUT", "/v1/routes/api"},
		{"PUT upstream", "PUT", "/v1/upstreams/web"},
		{"POST rollback", "POST", "/v1/config/rollback"},
	}
	bodies := []string{"null", " null ", "null\n"}
	for _, ep := range endpoints {
		for _, body := range bodies {
			run(t, fmt.Sprintf("%s/%q", ep.name, body), func(t *testing.T) {
				e := newEnv(t)
				e.seed(t)
				e.putUpstream(t, "extra") // v2
				sub, cancel := e.hub.Subscribe()
				defer cancel()
				drainPublished(sub)
				headBefore := canonJSON(mustHead(t, e.store))

				rec := e.admin(t, ep.method, ep.path, body)
				if rec.Code != 400 {
					t.Fatalf("status %d, want 400: %s", rec.Code, rec.Body)
				}
				errBody(t, rec)
				if v := e.headVersion(t); v != 2 {
					t.Fatalf("head version %d, want 2 (nothing stored)", v)
				}
				if got := canonJSON(mustHead(t, e.store)); got != headBefore {
					t.Fatalf("head changed:\n got %s\nwant %s", got, headBefore)
				}
				if hi := drainPublished(sub); hi != 0 {
					t.Fatalf("published v%d for a rejected write", hi)
				}
			})
		}
	}
}

// C4 (documented default, architecture.md section 6): rollback with an empty
// body, {} or {"to_version":0} means head - 1 and creates a new version.
func TestFixRound4_RollbackDefaultsStillWork(t *testing.T) {
	defer failOnPanic(t)
	for _, body := range []string{"", "{}", `{"to_version":0}`, " {} \n"} {
		run(t, fmt.Sprintf("body %q", body), func(t *testing.T) {
			e := newEnv(t)
			e.seed(t)
			e.putUpstream(t, "extra") // v2
			rec := e.admin(t, "POST", "/v1/config/rollback", body)
			if rec.Code != 200 {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			if v := etagVersion(t, rec); v != 3 {
				t.Fatalf("new version %d, want 3", v)
			}
			v1 := mustGet(t, e.store, 1)
			if got := decodeConfig(t, rec); canonBody(got) != canonBody(v1) {
				t.Fatalf("rolled-back body differs from v1:\n got %s\nwant %s", canonBody(got), canonBody(v1))
			}
		})
	}
}

func mustGet(t *testing.T, s Store, v int64) *config.Config {
	t.Helper()
	c, err := s.Get(testCtx(t), v)
	if err != nil {
		t.Fatalf("Get(%d): %v", v, err)
	}
	return c
}
