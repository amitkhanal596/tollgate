package proxy

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// ipN returns a distinct IPv4 RemoteAddr for n.
func ipN(n int) string {
	return "10." + itoa(n>>16&255) + "." + itoa(n>>8&255) + "." + itoa(n&255) + ":4000"
}

// D3: a JWT client-ID claim becomes the X-Tollgate-Client-ID header, so a
// control character or an oversized value must be a 401 (never a 502 from a
// transport that refuses the header), must not reach the backend, and must
// not count against it.
func TestJWTBadClientIDClaimIs401NotBackendFailure(t *testing.T) {
	tests := []struct{ name, sub string }{
		{"CRLF header injection", "alice\r\nX-Admin: 1"},
		{"NUL", "a\x00b"},
		{"tab", "a\tb"},
		{"257 bytes", strings.Repeat("a", 257)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A gateway per case, so one case's ejection cannot mask another's.
			u := newRecUp(t)
			cfg := withAuth(
				cfgOf(1, []config.Route{authRoute("r", "/", "u", config.AuthJWT, nil)},
					upx("u", config.AlgorithmRoundRobin, nil, passive3(), u.srv.URL)),
				config.AuthSettings{JWT: jwtSettings()})
			h, _ := startGatewayWith(t, Options{Getenv: testGetenv}, cfg)

			tok := mintToken(t, testJWTSecret, jwt.MapClaims{"sub": tc.sub, "exp": time.Now().Add(time.Hour).Unix()})
			for i := 0; i < 5; i++ {
				// A fresh IP each time keeps the failed-auth throttle out of this test.
				rec := serveAs(h.g, ipN(i), "/x", "Authorization", "Bearer "+tok)
				if rec.Code != http.StatusUnauthorized {
					t.Errorf("request %d: status %d, want 401", i+1, rec.Code)
				}
			}
			if got := u.hits.Load(); got != 0 {
				t.Errorf("backend saw %d requests with a bad client ID, want 0", got)
			}
			rec := serveAs(h.g, ipN(100), "/x", "Authorization", "Bearer "+goodToken(t, "alice"))
			if rec.Code != http.StatusOK {
				t.Fatalf("valid token after 5 bad ones: %d, want 200 (backend was ejected)", rec.Code)
			}
		})
	}
}

// D4: anonymous traffic is keyed by IP, so an attacker with many addresses
// can fill a limiter's key space. That must not lock authenticated clients
// out: the Gateway keeps separate limiters for the two kinds of identity.
func TestAnonymousFloodDoesNotDenyAuthenticatedClients(t *testing.T) {
	u := newRecUp(t)
	clk := newFakeClock() // frozen: no bucket ever becomes idle, so none can be evicted
	pub := authRoute("pub", "/pub", "u", config.AuthNone, rl(0.01, 1))
	api := authRoute("api", "/api", "u", config.AuthAPIKey, rl(5, 5))
	h := startRunWith(t, Options{Now: clk.Now, Getenv: testGetenv, LimiterMaxKeys: 64})
	h.apply(t, withAuth(cfgOf(1, []config.Route{pub, api}, up("u", u.srv.URL)),
		config.AuthSettings{APIKeys: keysFor("alice", "alice-key")}))

	var ok, limited int
	for i := 0; i < 5000; i++ {
		switch code := serveAs(h.g, ipN(i), "/pub").Code; code {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			limited++
		default:
			t.Fatalf("flood request %d: unexpected status %d", i, code)
		}
	}
	if ok == 0 || ok > 64 || limited < 4000 {
		t.Fatalf("flood: %d allowed, %d limited; want at most 64 allowed: Options.LimiterMaxKeys must cap the Gateway's own limiters", ok, limited)
	}

	rec := callKey(h.g, "/api/x", "alice-key")
	if rec.Code != http.StatusOK {
		t.Fatalf("first-time authenticated client after an anonymous flood: %d, want 200", rec.Code)
	}
}

// D5: at capacity the failed-auth throttle must fail closed for FAILED
// attempts (a new IP cannot guess keys for free) while still letting valid
// credentials through (it must not lock out legitimate clients).
func TestFailedAuthThrottleAtCapacityFailsClosedForWrongKeys(t *testing.T) {
	clk := newFakeClock() // frozen: nothing refills or becomes idle
	h, _ := apiKeyGateway(t, Options{Now: clk.Now, LimiterMaxKeys: 64}, nil)

	for i := 0; i < 2000; i++ { // far more IPs than the 64 keys the limiter may hold
		serveAs(h.g, ipN(i), "/x", "X-API-Key", "wrong")
	}

	if code := serveAs(h.g, ipN(900_000), "/x", "X-API-Key", "wrong").Code; code != http.StatusTooManyRequests {
		t.Errorf("wrong key from a new IP at capacity: %d, want 429", code)
	}
	if code := serveAs(h.g, ipN(900_001), "/x", "X-API-Key", "alice-key").Code; code != http.StatusOK {
		t.Errorf("valid key from a new IP at capacity: %d, want 200", code)
	}
}

// D6 (regression lock): the throttle is Peek-then-Allow, so concurrent wrong
// guesses from one IP can overshoot the burst of 10 by the number of
// goroutines racing between the two calls, but never more, and the bucket is
// then empty: with the clock frozen the next wave is entirely 429.
func TestFailedAuthThrottleConcurrentWrongKeys(t *testing.T) {
	clk := newFakeClock()
	h, _ := apiKeyGateway(t, Options{Now: clk.Now}, nil)

	const n = 200
	wave := func() (unauthorized int) {
		var count atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				switch serveAs(h.g, defaultRemote, "/x", "X-API-Key", "wrong").Code {
				case http.StatusUnauthorized:
					count.Add(1)
				case http.StatusTooManyRequests:
				default:
					t.Errorf("unexpected status")
				}
			}()
		}
		close(start)
		wg.Wait()
		return int(count.Load())
	}

	if got := wave(); got < 10 || got > 10+n {
		t.Errorf("first wave: %d 401s, want between 10 and %d", got, 10+n)
	}
	if got := wave(); got != 0 {
		t.Errorf("second wave: %d 401s, want 0 (bucket is empty and the clock is frozen)", got)
	}
}
