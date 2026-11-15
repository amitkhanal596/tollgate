package proxy

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

func callKey(g *Gateway, path, key string) *httptest.ResponseRecorder {
	return serveAs(g, defaultRemote, path, "X-API-Key", key)
}

// R1: per-route rate limit with the headers of architecture.md section 4
// step 3 and ADR 0004.
func TestWiringRateLimitBurstHeadersAndRefill(t *testing.T) {
	clk := newFakeClock()
	h, u := apiKeyGateway(t, Options{Now: clk.Now}, rl(5, 5))

	for i := 0; i < 5; i++ {
		rec := callKey(h.g, "/x", "alice-key")
		if rec.Code != 200 {
			t.Fatalf("request %d: %d, want 200", i+1, rec.Code)
		}
		if got := rec.Header().Get("X-RateLimit-Limit"); got != "5" {
			t.Errorf("request %d: X-RateLimit-Limit %q, want 5", i+1, got)
		}
		if got, want := rec.Header().Get("X-RateLimit-Remaining"), itoa(4-i); got != want {
			t.Errorf("request %d: X-RateLimit-Remaining %q, want %s", i+1, got, want)
		}
	}
	rec := callKey(h.g, "/x", "alice-key")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("6th request: %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After %q, want \"1\" (200ms rounded up to a whole second)", got)
	}
	if rec.Header().Get("X-RateLimit-Limit") != "5" || rec.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Errorf("429 headers Limit=%q Remaining=%q, want 5 and 0",
			rec.Header().Get("X-RateLimit-Limit"), rec.Header().Get("X-RateLimit-Remaining"))
	}
	if got := u.hits.Load(); got != 5 {
		t.Fatalf("upstream hits %d, want 5: rate-limited requests must not reach it", got)
	}

	clk.Advance(time.Second) // exactly one second at 5/s = 5 tokens
	for i := 0; i < 5; i++ {
		if code := callKey(h.g, "/x", "alice-key").Code; code != 200 {
			t.Fatalf("after refill, request %d: %d, want 200", i+1, code)
		}
	}
	if code := callKey(h.g, "/x", "alice-key").Code; code != 429 {
		t.Fatalf("after refill, 6th request: %d, want 429", code)
	}
}

// Retry-After is the time to the next whole token, rounded UP to whole
// seconds, minimum 1 (ADR 0004).
func TestWiringRetryAfterRounding(t *testing.T) {
	tests := []struct {
		name string
		rps  float64
		want string
	}{
		{"2.5s rounds up to 3", 0.4, "3"},
		{"3.33s rounds up to 4", 0.3, "4"},
		{"10ms is raised to the minimum of 1", 100, "1"},
		{"200ms is raised to the minimum of 1", 5, "1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := newFakeClock()
			h, _ := apiKeyGateway(t, Options{Now: clk.Now}, rl(tc.rps, 1))
			if code := callKey(h.g, "/x", "alice-key").Code; code != 200 {
				t.Fatalf("first request: %d", code)
			}
			rec := callKey(h.g, "/x", "alice-key")
			if rec.Code != 429 {
				t.Fatalf("second request: %d, want 429", rec.Code)
			}
			if got := rec.Header().Get("Retry-After"); got != tc.want {
				t.Errorf("Retry-After %q, want %q", got, tc.want)
			}
		})
	}
}

// R2: buckets are per client identity.
func TestWiringRateLimitPerAPIKey(t *testing.T) {
	clk := newFakeClock()
	h, _ := apiKeyGateway(t, Options{Now: clk.Now}, rl(1, 2))
	for i := 0; i < 2; i++ {
		if code := callKey(h.g, "/x", "alice-key").Code; code != 200 {
			t.Fatalf("alice %d: %d", i+1, code)
		}
	}
	if code := callKey(h.g, "/x", "alice-key").Code; code != 429 {
		t.Fatalf("alice over the limit: %d, want 429", code)
	}
	for i := 0; i < 2; i++ {
		rec := callKey(h.g, "/x", "bob-key")
		if rec.Code != 200 {
			t.Fatalf("bob %d: %d, want 200 (alice's bucket is not his)", i+1, rec.Code)
		}
	}
	if code := callKey(h.g, "/x", "bob-key").Code; code != 429 {
		t.Fatalf("bob over the limit: %d, want 429", code)
	}
}

// R3: buckets are per route; a route without a limit is never limited.
func TestWiringRateLimitPerRoute(t *testing.T) {
	clk := newFakeClock()
	u := newRecUp(t)
	h := startRunWith(t, Options{Now: clk.Now, Getenv: testGetenv})
	h.apply(t, withAuth(cfgOf(1,
		[]config.Route{
			authRoute("a", "/a", "u", config.AuthAPIKey, rl(1, 1)),
			authRoute("b", "/b", "u", config.AuthAPIKey, rl(1, 2)),
			authRoute("c", "/c", "u", config.AuthAPIKey, nil),
		}, up("u", u.srv.URL)),
		config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))

	seq := func(path string, want ...int) {
		t.Helper()
		for i, w := range want {
			if got := callKey(h.g, path, "alice-key").Code; got != w {
				t.Fatalf("%s request %d: %d, want %d", path, i+1, got, w)
			}
		}
	}
	seq("/a", 200, 429, 429)
	seq("/b", 200, 200, 429) // its own bucket, its own burst
	for i := 0; i < 30; i++ {
		rec := callKey(h.g, "/c", "alice-key")
		if rec.Code != 200 {
			t.Fatalf("unlimited route request %d: %d, want 200", i+1, rec.Code)
		}
		if v := rec.Header().Values("X-RateLimit-Limit"); len(v) != 0 {
			t.Fatalf("route without rate_limit sets X-RateLimit-Limit %q", v)
		}
		if v := rec.Header().Values("X-RateLimit-Remaining"); len(v) != 0 {
			t.Fatalf("route without rate_limit sets X-RateLimit-Remaining %q", v)
		}
	}
}

// R4: auth none routes are limited per client IP, from RemoteAddr only.
func TestWiringRateLimitByClientIP(t *testing.T) {
	clk := newFakeClock()
	u := newRecUp(t)
	h := startRunWith(t, Options{Now: clk.Now})
	h.apply(t, cfgOf(1, []config.Route{authRoute("r", "/", "u", config.AuthNone, rl(1, 3))}, up("u", u.srv.URL)))

	codes := func(remote string, n int, hdr ...string) []int {
		var out []int
		for i := 0; i < n; i++ {
			out = append(out, serveAs(h.g, remote, "/x", hdr...).Code)
		}
		return out
	}
	expect := func(what string, got []int, want ...int) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: %v, want %v", what, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s: %v, want %v", what, got, want)
			}
		}
	}

	t.Run("IPv4 addresses have separate buckets, the port is ignored", func(t *testing.T) {
		expect("198.51.100.1", codes("198.51.100.1:1000", 4), 200, 200, 200, 429)
		expect("same IP, new port", codes("198.51.100.1:2000", 1), 429)
		expect("198.51.100.2", codes("198.51.100.2:1000", 3), 200, 200, 200)
	})
	t.Run("X-Forwarded-For is not trusted", func(t *testing.T) {
		// 198.51.100.1 is exhausted: a fresh XFF must not give it a new bucket.
		expect("spoofed XFF", codes("198.51.100.1:1000", 1, "X-Forwarded-For", "203.0.113.77"), 429)
		// Rotating XFF values from a fresh IP still drain one bucket.
		expect("rotating XFF", []int{
			serveAs(h.g, "198.51.100.3:1", "/x", "X-Forwarded-For", "10.0.0.1").Code,
			serveAs(h.g, "198.51.100.3:1", "/x", "X-Forwarded-For", "10.0.0.2").Code,
			serveAs(h.g, "198.51.100.3:1", "/x", "X-Forwarded-For", "10.0.0.3").Code,
			serveAs(h.g, "198.51.100.3:1", "/x", "X-Forwarded-For", "10.0.0.4").Code,
		}, 200, 200, 200, 429)
	})
	t.Run("IPv4-mapped IPv6 is the IPv4 client", func(t *testing.T) {
		expect("::ffff:198.51.100.1", codes("[::ffff:198.51.100.1]:9", 1), 429)
	})
	t.Run("IPv6 clients share their /64", func(t *testing.T) {
		got := []int{
			serveAs(h.g, "[2001:db8:1:2::1]:1", "/x").Code,
			serveAs(h.g, "[2001:db8:1:2:aaaa::7]:2", "/x").Code,
			serveAs(h.g, "[2001:db8:1:2:ffff:ffff:ffff:ffff]:3", "/x").Code,
			serveAs(h.g, "[2001:db8:1:2::99]:4", "/x").Code,
		}
		expect("same /64", got, 200, 200, 200, 429)
		expect("another /64", codes("[2001:db8:1:3::1]:1", 3), 200, 200, 200)
	})
	t.Run("headers on an unauthenticated route", func(t *testing.T) {
		rec := serveAs(h.g, "198.51.100.50:1", "/x")
		if rec.Header().Get("X-RateLimit-Limit") != "3" || rec.Header().Get("X-RateLimit-Remaining") != "2" {
			t.Errorf("Limit=%q Remaining=%q, want 3 and 2",
				rec.Header().Get("X-RateLimit-Limit"), rec.Header().Get("X-RateLimit-Remaining"))
		}
	})
}

// R5: buckets live outside the snapshot, so a reload neither refills them
// nor ignores a changed limit.
func TestWiringRateLimitSurvivesReload(t *testing.T) {
	clk := newFakeClock()
	u := newRecUp(t)
	h := startRunWith(t, Options{Now: clk.Now, Getenv: testGetenv})
	auth := config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}
	cfgWith := func(version int64, limit *config.RateLimit, extra ...config.Route) *config.Config {
		routes := append([]config.Route{authRoute("r", "/", "u", config.AuthAPIKey, limit)}, extra...)
		return withAuth(cfgOf(version, routes, up("u", u.srv.URL)), auth)
	}
	h.apply(t, cfgWith(1, rl(1, 2)))

	for i := 0; i < 2; i++ {
		if code := callKey(h.g, "/x", "alice-key").Code; code != 200 {
			t.Fatalf("request %d: %d", i+1, code)
		}
	}
	if code := callKey(h.g, "/x", "alice-key").Code; code != 429 {
		t.Fatalf("exhausted: %d, want 429", code)
	}

	// Unrelated change: the bucket must not be reset.
	h.apply(t, cfgWith(2, rl(1, 2), authRoute("other", "/other", "u", config.AuthNone, nil)))
	if code := callKey(h.g, "/x", "alice-key").Code; code != 429 {
		t.Fatalf("after an unrelated reload: %d, want 429 (bucket state must survive)", code)
	}

	// Raise the limit: it applies to the very next request (the bucket is
	// still empty, so it is denied, but under the new limit).
	h.apply(t, cfgWith(3, rl(5, 10)))
	rec := callKey(h.g, "/x", "alice-key")
	if rec.Code != 429 || rec.Header().Get("X-RateLimit-Limit") != "10" {
		t.Fatalf("right after raising the limit: %d Limit=%q, want 429 with Limit 10",
			rec.Code, rec.Header().Get("X-RateLimit-Limit"))
	}
	clk.Advance(time.Second) // 5 tokens at the NEW rate
	for i := 0; i < 5; i++ {
		if code := callKey(h.g, "/x", "alice-key").Code; code != 200 {
			t.Fatalf("after raising the rate, request %d: %d, want 200", i+1, code)
		}
	}
	if code := callKey(h.g, "/x", "alice-key").Code; code != 429 {
		t.Fatalf("6th request at the new rate: %d, want 429", code)
	}
}

// R6: the limit is exact under contention.
func TestWiringRateLimitConcurrentExactCount(t *testing.T) {
	clk := newFakeClock() // frozen: no refill
	h, u := apiKeyGateway(t, Options{Now: clk.Now}, rl(1, 100))

	const workers, per = 50, 20
	var ok, limited, other int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var o, l, x int64
			for i := 0; i < per; i++ {
				switch callKey(h.g, "/x", "alice-key").Code {
				case 200:
					o++
				case 429:
					l++
				default:
					x++
				}
			}
			mu.Lock()
			ok, limited, other = ok+o, limited+l, other+x
			mu.Unlock()
		}()
	}
	wg.Wait()
	if ok != 100 || limited != 900 || other != 0 {
		t.Fatalf("200s=%d 429s=%d other=%d, want exactly 100, 900 and 0", ok, limited, other)
	}
	if got := u.hits.Load(); got != 100 {
		t.Errorf("upstream hits %d, want 100", got)
	}
}

// R7: order of checks: auth, then rate limit, then balancer.
func TestWiringCheckOrder(t *testing.T) {
	t.Run("unauthenticated request to an exhausted route is 401, not 429", func(t *testing.T) {
		clk := newFakeClock()
		h, _ := apiKeyGateway(t, Options{Now: clk.Now}, rl(1, 1))
		callKey(h.g, "/x", "alice-key")
		if code := callKey(h.g, "/x", "alice-key").Code; code != 429 {
			t.Fatalf("setup: %d, want 429", code)
		}
		if code := serveAs(h.g, defaultRemote, "/x").Code; code != 401 {
			t.Errorf("no key: %d, want 401", code)
		}
		if code := callKey(h.g, "/x", "nope").Code; code != 401 {
			t.Errorf("wrong key: %d, want 401", code)
		}
	})

	t.Run("a rate-limited request never touches the balancer", func(t *testing.T) {
		clk := newFakeClock()
		a, b := echoUpstream(t, "A"), echoUpstream(t, "B")
		h := startRunWith(t, Options{Now: clk.Now, Getenv: testGetenv})
		h.apply(t, withAuth(cfgOf(1,
			[]config.Route{authRoute("r", "/", "u", config.AuthAPIKey, rl(1, 1))}, up("u", a.URL, b.URL)),
			config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))

		rec := callKey(h.g, "/x", "alice-key")
		first := rec.Body.String()
		if rec.Code != 200 {
			t.Fatalf("first: %d", rec.Code)
		}
		// An ODD number of denied requests: if each one advanced the
		// round-robin cursor, the next allowed request would repeat `first`.
		for i := 0; i < 7; i++ {
			if code := callKey(h.g, "/x", "alice-key").Code; code != 429 {
				t.Fatalf("denied #%d: %d, want 429", i+1, code)
			}
		}
		for _, raw := range []string{a.URL, b.URL} {
			if n := inFlight(t, h.g, "u", raw); n != 0 {
				t.Errorf("in-flight on %s = %d after 429s, want 0", raw, n)
			}
		}
		clk.Advance(time.Second)
		rec = callKey(h.g, "/x", "alice-key")
		if rec.Code != 200 {
			t.Fatalf("after refill: %d", rec.Code)
		}
		if second := rec.Body.String(); second == first {
			t.Errorf("both allowed requests went to %q: the 429s consumed round-robin turns", first)
		}
	})
}
