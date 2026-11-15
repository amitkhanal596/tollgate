package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// apiKeyGateway starts a gateway with one api_key route "/" and keys
// alice/alice-key, bob/bob-key in front of a recording upstream.
func apiKeyGateway(t *testing.T, opts Options, rlim *config.RateLimit) (*harness, *recUp) {
	t.Helper()
	u := newRecUp(t)
	opts.Getenv = testGetenv
	h := startRunWith(t, opts)
	h.apply(t, withAuth(
		cfgOf(1, []config.Route{authRoute("r", "/", "u", config.AuthAPIKey, rlim)}, up("u", u.srv.URL)),
		config.AuthSettings{APIKeys: keysFor("alice", "alice-key", "bob", "bob-key")}))
	return h, u
}

// clientIDValues returns every value the upstream saw for the client-ID
// header, whatever the spelling it arrived with.
func clientIDValues(h http.Header) []string {
	var out []string
	for k, v := range h {
		if strings.EqualFold(k, HeaderClientID) {
			out = append(out, v...)
		}
	}
	return out
}

func apiKeyValues(h http.Header) []string {
	var out []string
	for k, v := range h {
		if strings.EqualFold(k, "X-API-Key") {
			out = append(out, v...)
		}
	}
	return out
}

// A1: api_key routes (architecture.md section 4 step 2, section 8).
func TestWiringAPIKeyRejections(t *testing.T) {
	h, u := apiKeyGateway(t, Options{}, nil)

	tests := []struct {
		name string
		hdr  []string
		path string
	}{
		{"no key", nil, "/x"},
		{"wrong key", []string{"X-API-Key", "nope"}, "/x"},
		{"empty key", []string{"X-API-Key", ""}, "/x"},
		{"key with wrong case", []string{"X-API-Key", "ALICE-KEY"}, "/x"},
		{"key in the wrong header", []string{"Authorization", "Bearer alice-key"}, "/x"},
		{"key in the query string", nil, "/x?api_key=alice-key"},
		{"valid key plus a second, wrong one", []string{"X-API-Key", "alice-key", "X-API-Key", "nope"}, "/x"},
	}
	var refBody string
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A different client IP per case keeps the failed-auth throttle out of this test.
			rec := serveAs(h.g, "198.51.100."+itoa(i+1)+":4000", tc.path, tc.hdr...)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status %d, want 401", rec.Code)
			}
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Error("401 without a WWW-Authenticate header")
			}
			body := rec.Body.String()
			// section 4: "generic body". Pin: the same body whatever went wrong, and no echo.
			if i == 0 {
				refBody = body
			} else if body != refBody {
				t.Errorf("body %q differs from the missing-key body %q: it leaks which check failed", body, refBody)
			}
			for _, leak := range []string{"alice", "bob", "nope", "missing", "invalid", "wrong"} {
				if strings.Contains(strings.ToLower(body), leak) {
					t.Errorf("401 body %q mentions %q", body, leak)
				}
			}
		})
	}
	if got := u.hits.Load(); got != 0 {
		t.Errorf("upstream saw %d unauthenticated requests, want 0", got)
	}
}

func TestWiringAPIKeyAccepted(t *testing.T) {
	h, u := apiKeyGateway(t, Options{}, nil)

	t.Run("identity is set, key is stripped, Authorization is kept", func(t *testing.T) {
		rec := serveAs(h.g, defaultRemote, "/x", "X-API-Key", "bob-key", "Authorization", "Basic Zm9vOmJhcg==")
		if rec.Code != 200 {
			t.Fatalf("status %d", rec.Code)
		}
		got := u.lastHeader()
		if v := clientIDValues(got); len(v) != 1 || v[0] != "bob" {
			t.Errorf("upstream client-ID values = %q, want [bob]", v)
		}
		if v := apiKeyValues(got); len(v) != 0 {
			t.Errorf("upstream received X-API-Key %q; the gateway must strip it (section 4 step 5)", v)
		}
		if got.Get("Authorization") != "Basic Zm9vOmJhcg==" {
			t.Errorf("Authorization = %q, want it forwarded unchanged", got.Get("Authorization"))
		}
	})

	// A client must never choose its own identity, whatever the spelling.
	spoofs := []struct {
		name string
		set  func(h http.Header)
	}{
		{"canonical", func(h http.Header) { h.Set("X-Tollgate-Client-ID", "evil") }},
		{"lower case", func(h http.Header) { h["x-tollgate-client-id"] = []string{"evil"} }},
		{"upper case", func(h http.Header) { h["X-TOLLGATE-CLIENT-ID"] = []string{"evil"} }},
		{"several values", func(h http.Header) { h["X-Tollgate-Client-Id"] = []string{"evil1", "evil2"} }},
		{"canonical and lower case together", func(h http.Header) {
			h.Set("X-Tollgate-Client-ID", "evil")
			h["x-tollgate-client-id"] = []string{"evil2"}
		}},
	}
	for _, sp := range spoofs {
		t.Run("spoof "+sp.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			req.RemoteAddr = defaultRemote
			req.Header.Set("X-API-Key", "alice-key")
			sp.set(req.Header)
			rec := httptest.NewRecorder()
			h.g.ServeHTTP(rec, req)
			if rec.Code != 200 {
				t.Fatalf("status %d", rec.Code)
			}
			if v := clientIDValues(u.lastHeader()); len(v) != 1 || v[0] != "alice" {
				t.Errorf("upstream client-ID values = %q, want exactly [alice]", v)
			}
		})
	}
}

// A2: JWT routes.
func TestWiringJWTAuth(t *testing.T) {
	u := newRecUp(t)
	h := startRunWith(t, Options{Getenv: testGetenv})
	h.apply(t, withAuth(
		cfgOf(1, []config.Route{authRoute("r", "/", "u", config.AuthJWT, nil)}, up("u", u.srv.URL)),
		config.AuthSettings{JWT: jwtSettings()}))

	exp := time.Now().Add(time.Hour).Unix()
	noneTok, err := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"sub": "alice", "exp": exp}).
		SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	bearer := func(tok string) []string { return []string{"Authorization", "Bearer " + tok} }
	bad := []struct {
		name string
		hdr  []string
	}{
		{"missing", nil},
		{"expired", bearer(mintToken(t, testJWTSecret, jwt.MapClaims{"sub": "alice", "exp": time.Now().Add(-time.Hour).Unix()}))},
		{"wrong signature", bearer(mintToken(t, "another-secret-another-secret-another!", jwt.MapClaims{"sub": "alice", "exp": exp}))},
		{"alg none", bearer(noneTok)},
		{"no exp", bearer(mintToken(t, testJWTSecret, jwt.MapClaims{"sub": "alice"}))},
		{"no sub", bearer(mintToken(t, testJWTSecret, jwt.MapClaims{"exp": exp}))},
		{"not a token", bearer("garbage")},
		{"empty bearer", []string{"Authorization", "Bearer "}},
		{"basic scheme", []string{"Authorization", "Basic YWxpY2U6eA=="}},
		{"token in X-API-Key", []string{"X-API-Key", goodToken(t, "alice")}},
	}
	var refBody string
	for i, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			rec := serveAs(h.g, "198.51.100."+itoa(i+1)+":4000", "/x", tc.hdr...)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status %d, want 401", rec.Code)
			}
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Error("401 without a WWW-Authenticate header")
			}
			if i == 0 {
				refBody = rec.Body.String()
			} else if rec.Body.String() != refBody {
				t.Errorf("body %q differs from the missing-credentials body %q", rec.Body.String(), refBody)
			}
		})
	}
	if got := u.hits.Load(); got != 0 {
		t.Fatalf("upstream saw %d unauthenticated requests", got)
	}

	t.Run("valid token", func(t *testing.T) {
		tok := goodToken(t, "alice")
		rec := serveAs(h.g, defaultRemote, "/x", "Authorization", "Bearer "+tok, "X-Tollgate-Client-ID", "evil")
		if rec.Code != 200 {
			t.Fatalf("status %d", rec.Code)
		}
		got := u.lastHeader()
		if v := clientIDValues(got); len(v) != 1 || v[0] != "alice" {
			t.Errorf("client-ID values = %q, want [alice] (the sub claim)", v)
		}
		if got.Get("Authorization") != "Bearer "+tok {
			t.Errorf("Authorization = %q, want the token forwarded (section 4: keep Authorization)", got.Get("Authorization"))
		}
	})
}

// A3: auth none.
func TestWiringAuthNoneStripsIdentityAndKey(t *testing.T) {
	u := newRecUp(t)
	h := startRunWith(t, Options{Getenv: testGetenv})
	h.apply(t, withAuth(
		cfgOf(1, []config.Route{authRoute("r", "/", "u", config.AuthNone, nil)}, up("u", u.srv.URL)),
		config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.RemoteAddr = defaultRemote
	req.Header.Set("X-Tollgate-Client-ID", "evil")
	req.Header["x-tollgate-client-id"] = []string{"evil2"}
	req.Header.Set("X-API-Key", "alice-key")
	req.Header.Set("Authorization", "Bearer abc")
	rec := httptest.NewRecorder()
	h.g.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status %d, want 200: auth none needs no credentials", rec.Code)
	}
	got := u.lastHeader()
	if v := clientIDValues(got); len(v) != 0 {
		t.Errorf("upstream saw client-ID %q on an unauthenticated route; nothing may be set (and the client IP never goes there)", v)
	}
	// section 4 step 5 strips X-API-Key unconditionally, on every route.
	if v := apiKeyValues(got); len(v) != 0 {
		t.Errorf("upstream received X-API-Key %q", v)
	}
	if got.Get("Authorization") != "Bearer abc" {
		t.Errorf("Authorization = %q, want it forwarded", got.Get("Authorization"))
	}
}

// A4: auth is per route; the router decides first, and unsafe paths never
// slip from a public route into a private one.
func TestWiringAuthIsPerRouteAndPathBypassIsClosed(t *testing.T) {
	pub, priv := newRecUp(t), newRecUp(t)
	h := startRunWith(t, Options{Getenv: testGetenv})
	h.apply(t, withAuth(cfgOf(1,
		[]config.Route{
			authRoute("public", "/", "pub", config.AuthNone, nil),
			authRoute("private", "/private", "priv", config.AuthAPIKey, nil),
		}, up("pub", pub.srv.URL), up("priv", priv.srv.URL)),
		config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))

	t.Run("independent behaviour", func(t *testing.T) {
		cases := []struct {
			target string
			hdr    []string
			want   int
		}{
			{"/public", nil, 200},
			{"/private", nil, 401},
			{"/private/deep/er", nil, 401},
			{"/private", []string{"X-API-Key", "alice-key"}, 200},
			{"/privatex", nil, 200}, // segment-aware: not the private prefix
		}
		for _, c := range cases {
			rec := serveAs(h.g, defaultRemote, c.target, c.hdr...)
			if rec.Code != c.want {
				t.Errorf("GET %s: %d, want %d", c.target, rec.Code, c.want)
			}
		}
	})

	privBefore, pubBefore := priv.hits.Load(), pub.hits.Load()
	bypass := []string{
		"/public/..%2fprivate",
		"/public/..%2Fprivate",
		"//private",
		"/private;x",
		"/public/../private",
		"/public/%2e%2e/private",
		"/public/..;/private",
		"/public%2f..%2fprivate",
		"/./private",
		"/private/./x",
		`/public/..\private`,
	}
	for _, target := range bypass {
		t.Run("bypass "+target, func(t *testing.T) {
			var rec *httptest.ResponseRecorder
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("panic building request: %v", r)
					}
				}()
				rec = serveAs(h.g, defaultRemote, target)
			}()
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status %d, want 400", rec.Code)
			}
		})
	}
	if priv.hits.Load() != privBefore || pub.hits.Load() != pubBefore {
		t.Errorf("a bypass attempt reached an upstream (private +%d, public +%d)",
			priv.hits.Load()-privBefore, pub.hits.Load()-pubBefore)
	}
}

// A5: an unauthenticated request never reaches the upstream and never
// consumes the client's rate-limit tokens.
func TestWiringUnauthenticatedDoesNotConsumeTokens(t *testing.T) {
	clk := newFakeClock()
	h, u := apiKeyGateway(t, Options{Now: clk.Now}, rl(1, 2))

	for i := 0; i < 3; i++ {
		if rec := serveAs(h.g, defaultRemote, "/x"); rec.Code != 401 {
			t.Fatalf("no key #%d: %d, want 401", i, rec.Code)
		}
		if rec := serveAs(h.g, defaultRemote, "/x", "X-API-Key", "alice-wrong"); rec.Code != 401 {
			t.Fatalf("wrong key #%d: %d, want 401", i, rec.Code)
		}
	}
	if u.hits.Load() != 0 {
		t.Fatalf("upstream saw %d unauthenticated requests", u.hits.Load())
	}
	// alice's burst of 2 must be intact.
	first := serveAs(h.g, defaultRemote, "/x", "X-API-Key", "alice-key")
	if first.Code != 200 || first.Header().Get("X-RateLimit-Remaining") != "1" {
		t.Fatalf("first valid request: %d remaining=%q, want 200 and 1", first.Code, first.Header().Get("X-RateLimit-Remaining"))
	}
	if rec := serveAs(h.g, defaultRemote, "/x", "X-API-Key", "alice-key"); rec.Code != 200 {
		t.Fatalf("second valid request: %d, want 200", rec.Code)
	}
	if rec := serveAs(h.g, defaultRemote, "/x", "X-API-Key", "alice-key"); rec.Code != 429 {
		t.Fatalf("third valid request: %d, want 429", rec.Code)
	}
}

// A6: failed-auth throttle (ADR 0004, architecture.md section 4 step 2 and
// section 11 row 10): burst 10, refill 1/s, keyed by client IP.
func TestWiringFailedAuthThrottle(t *testing.T) {
	modes := []struct {
		name      string
		mode      config.AuthMode
		bad, good func() []string
	}{
		{"api_key", config.AuthAPIKey,
			func() []string { return []string{"X-API-Key", "wrong"} },
			func() []string { return []string{"X-API-Key", "alice-key"} }},
		{"jwt", config.AuthJWT,
			func() []string {
				return []string{"Authorization", "Bearer " + mintToken(t, "x-wrong-secret-x-wrong-secret-x-wrong", jwt.MapClaims{"sub": "alice", "exp": time.Now().Add(time.Hour).Unix()})}
			},
			func() []string { return []string{"Authorization", "Bearer " + goodToken(t, "alice")} }},
	}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			clk := newFakeClock()
			u := newRecUp(t)
			h := startRunWith(t, Options{Now: clk.Now, Getenv: testGetenv})
			h.apply(t, withAuth(
				cfgOf(1, []config.Route{authRoute("r", "/", "u", m.mode, nil)}, up("u", u.srv.URL)),
				config.AuthSettings{APIKeys: keysFor("alice", "alice-key"), JWT: jwtSettings()}))

			failN := func(remote string, n int) {
				t.Helper()
				for i := 0; i < n; i++ {
					if rec := serveAs(h.g, remote, "/x", m.bad()...); rec.Code != 401 {
						t.Fatalf("bad credentials #%d from %s: %d, want 401", i+1, remote, rec.Code)
					}
				}
			}
			assertThrottled := func(remote string, hdr []string, what string) {
				t.Helper()
				rec := serveAs(h.g, remote, "/x", hdr...)
				if rec.Code != http.StatusTooManyRequests {
					t.Fatalf("%s: %d, want 429", what, rec.Code)
				}
				if ra := rec.Header().Get("Retry-After"); ra != "1" {
					t.Errorf("%s: Retry-After %q, want \"1\" (bucket empty, refill 1/s)", what, ra)
				}
			}

			ip := "203.0.113.5:1111"
			failN(ip, 10)
			assertThrottled(ip, m.bad(), "11th bad attempt")

			// ADR 0004: Peek first; an empty bucket answers 429 WITHOUT
			// verifying, so even a valid credential is refused.
			hitsBefore := u.hits.Load()
			assertThrottled("203.0.113.5:2222", m.good(), "valid credentials from a throttled IP (other port)")
			assertThrottled(ip, nil, "missing credentials from a throttled IP (the Peek comes first)")
			if u.hits.Load() != hitsBefore {
				t.Error("a throttled request reached the upstream")
			}
			// Keyed by IP: someone else is unaffected.
			if rec := serveAs(h.g, "203.0.113.6:1111", "/x", m.good()...); rec.Code != 200 {
				t.Errorf("valid credentials from another IP: %d, want 200", rec.Code)
			}
			// Tokens come back with time (1/s).
			clk.Advance(time.Second)
			if rec := serveAs(h.g, ip, "/x", m.good()...); rec.Code != 200 {
				t.Errorf("after 1s the throttled IP got %d, want 200", rec.Code)
			}

			t.Run("missing credentials are not charged", func(t *testing.T) {
				ip := "203.0.113.10:1"
				for i := 0; i < 20; i++ {
					if rec := serveAs(h.g, ip, "/x"); rec.Code != 401 {
						t.Fatalf("no credentials #%d: %d, want 401 (never 429)", i+1, rec.Code)
					}
				}
				failN(ip, 10) // the full burst of 10 must still be there
				assertThrottled(ip, m.bad(), "11th bad attempt after 20 uncharged ones")
			})
			t.Run("successes are not charged", func(t *testing.T) {
				ip := "203.0.113.11:1"
				for i := 0; i < 20; i++ {
					if rec := serveAs(h.g, ip, "/x", m.good()...); rec.Code != 200 {
						t.Fatalf("valid #%d: %d", i+1, rec.Code)
					}
				}
				failN(ip, 10)
				assertThrottled(ip, m.bad(), "11th bad attempt after 20 successes")
			})
			t.Run("IPv6 clients share their /64", func(t *testing.T) {
				failN("[2001:db8:7:8::1]:1", 10)
				assertThrottled("[2001:db8:7:8:ffff::2]:2", m.good(), "valid credentials from the same /64")
				if rec := serveAs(h.g, "[2001:db8:7:9::1]:1", "/x", m.good()...); rec.Code != 200 {
					t.Errorf("another /64: %d, want 200", rec.Code)
				}
			})
		})
	}
}

// A7: a config whose auth settings cannot be built is rejected whole.
func TestWiringUnbuildableAuthRejectsWholeConfig(t *testing.T) {
	u := newRecUp(t)
	h := startRunWith(t, Options{Getenv: testGetenv})
	h.apply(t, withAuth(
		cfgOf(1, []config.Route{authRoute("r", "/", "u", config.AuthAPIKey, nil)}, up("u", u.srv.URL)),
		config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))

	broken := withAuth(
		cfgOf(2, []config.Route{
			authRoute("r", "/", "u", config.AuthAPIKey, nil),
			authRoute("new", "/new", "u", config.AuthJWT, nil),
		}, up("u", u.srv.URL)),
		config.AuthSettings{
			APIKeys: keysFor("alice", "alice-key"),
			JWT:     &config.JWTSettings{Algorithm: config.JWTHS256, HMACSecretEnv: "TOLLGATE_TEST_UNSET_VAR"},
		})
	h.src.push(broken)

	consistently(t, 300*time.Millisecond, "Version() must stay 1 and the old routes must keep serving", func() bool {
		return h.g.Version() == 1 &&
			serveAs(h.g, defaultRemote, "/x", "X-API-Key", "alice-key").Code == 200 &&
			serveAs(h.g, defaultRemote, "/new", "X-API-Key", "alice-key").Code == 200 // still the old catch-all "/"
	})

	// The loop is still alive: a good config applies afterwards.
	h.apply(t, withAuth(
		cfgOf(3, []config.Route{authRoute("r", "/", "u", config.AuthAPIKey, nil)}, up("u", u.srv.URL)),
		config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))
}

// A8: rotating API keys by hot reload, without dropping in-flight requests.
func TestWiringKeyRotationOnReload(t *testing.T) {
	u := newRecUp(t)
	h := startRunWith(t, Options{Getenv: testGetenv})
	cfgFor := func(version int64, key string) *config.Config {
		return withAuth(
			cfgOf(version, []config.Route{authRoute("r", "/", "u", config.AuthAPIKey, nil)}, up("u", u.srv.URL)),
			config.AuthSettings{APIKeys: keysFor("alice", key)})
	}
	h.apply(t, cfgFor(1, "old-key"))
	t.Cleanup(u.release) // before the upstream's own Close

	if serveAs(h.g, defaultRemote, "/x", "X-API-Key", "old-key").Code != 200 {
		t.Fatal("old key rejected before the reload")
	}
	if serveAs(h.g, defaultRemote, "/x", "X-API-Key", "new-key").Code != 401 {
		t.Fatal("new key accepted before the reload")
	}

	// A request in flight on the old snapshot.
	var wg sync.WaitGroup
	inflight := make(chan int, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		inflight <- serveAs(h.g, defaultRemote, "/slow", "X-API-Key", "old-key").Code
	}()
	waitFor(t, 3*time.Second, "slow request to reach the upstream", func() bool { return u.entered.Load() == 1 })

	h.apply(t, cfgFor(2, "new-key"))

	if code := serveAs(h.g, defaultRemote, "/x", "X-API-Key", "old-key").Code; code != 401 {
		t.Errorf("old key after rotation: %d, want 401", code)
	}
	if code := serveAs(h.g, defaultRemote, "/x", "X-API-Key", "new-key").Code; code != 200 {
		t.Errorf("new key after rotation: %d, want 200", code)
	}
	u.release()
	wg.Wait()
	if code := <-inflight; code != 200 {
		t.Errorf("in-flight request finished with %d, want 200 (it was authenticated on the old snapshot)", code)
	}
}
