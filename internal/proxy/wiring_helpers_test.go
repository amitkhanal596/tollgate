package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// ---- clock -----------------------------------------------------------------

// fakeClock is a manually advanced clock, safe for concurrent use.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// ---- gateway startup with Options ---------------------------------------------

// startRunWith is startRun with explicit Options.
func startRunWith(t *testing.T, opts Options) *harness {
	t.Helper()
	g := New(opts)
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{g: g, src: newFakeSource(), cancel: cancel, done: make(chan error, 1)}
	go func() { h.done <- g.Run(ctx, h.src) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-h.done:
			if err != nil {
				t.Logf("Run returned: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cleanup cancel")
		}
	})
	return h
}

// startGatewayWith starts Run with opts, applies cfg and fronts it with a server.
func startGatewayWith(t *testing.T, opts Options, cfg *config.Config) (*harness, *httptest.Server) {
	t.Helper()
	h := startRunWith(t, opts)
	h.apply(t, cfg)
	srv := httptest.NewServer(h.g)
	t.Cleanup(srv.Close)
	return h, srv
}

// consistently fails the test if cond is ever false during d.
func consistently(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("condition broke within %v: %s", d, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func ms(n int) config.Duration { return config.Duration(time.Duration(n) * time.Millisecond) }

// ---- controllable backend (health / passive tests) ---------------------------

// ctrlBackend is a fake upstream whose health endpoint, traffic status and
// latency can be changed while it runs.
type ctrlBackend struct {
	srv  *httptest.Server
	name string

	healthy atomic.Bool  // verdict served on /healthz
	status  atomic.Int32 // status served on traffic
	delay   atomic.Int64 // ns; traffic delay, aborted when the client goes away

	hits   atomic.Int64 // traffic requests (everything except /healthz)
	quick  atomic.Int64 // traffic requests to /quick
	probes atomic.Int64 // /healthz requests

	holding atomic.Int64 // requests currently blocked on /hold
	hold    atomic.Pointer[chan struct{}]

	mu       sync.Mutex
	hitTimes []time.Time
}

func newCtrlBackend(t *testing.T, name string) *ctrlBackend {
	t.Helper()
	b := &ctrlBackend{name: name}
	b.healthy.Store(true)
	b.status.Store(http.StatusOK)
	b.srv = httptest.NewServer(http.HandlerFunc(b.handle))
	t.Cleanup(b.srv.Close)
	return b
}

func (b *ctrlBackend) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		b.probes.Add(1)
		if b.healthy.Load() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	b.hits.Add(1)
	if r.URL.Path == "/quick" {
		b.quick.Add(1)
	}
	b.mu.Lock()
	b.hitTimes = append(b.hitTimes, time.Now())
	b.mu.Unlock()

	if ch := b.hold.Load(); ch != nil && r.URL.Path == "/hold" {
		b.holding.Add(1)
		defer b.holding.Add(-1)
		select {
		case <-*ch:
		case <-r.Context().Done():
			return
		}
	}
	if d := b.delay.Load(); d > 0 {
		select {
		case <-time.After(time.Duration(d)):
		case <-r.Context().Done():
			return
		}
	}
	w.WriteHeader(int(b.status.Load()))
	_, _ = w.Write([]byte(b.name))
}

func (b *ctrlBackend) url() string { return b.srv.URL }

// setDown makes both the health endpoint and traffic fail with 500.
func (b *ctrlBackend) setDown() { b.healthy.Store(false); b.status.Store(500) }

// setUp restores health and traffic.
func (b *ctrlBackend) setUp() { b.healthy.Store(true); b.status.Store(200) }

func (b *ctrlBackend) times() []time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]time.Time(nil), b.hitTimes...)
}

// hcFast is the active health check used by the wiring tests.
func hcFast() *config.HealthCheck {
	return &config.HealthCheck{
		Path:               "/healthz",
		Interval:           ms(20),
		Timeout:            ms(100),
		HealthyThreshold:   2,
		UnhealthyThreshold: 2,
	}
}

// upx builds an upstream with every Phase 2 knob.
func upx(name string, algo config.Algorithm, hc *config.HealthCheck, ph *config.PassiveHealth, backends ...string) config.Upstream {
	return config.Upstream{Name: name, Algorithm: algo, Backends: backends, HealthCheck: hc, PassiveHealth: ph}
}

// getCode issues a GET and returns the status (fatal on transport error).
func getCode(t *testing.T, c *http.Client, url string) int {
	t.Helper()
	code, _ := getBody(t, c, url)
	return code
}

// ---- recording upstream (auth / rate-limit tests) -------------------------------

// recUp records what the upstream saw. Requests to /slow block until gate is
// closed.
type recUp struct {
	srv  *httptest.Server
	hits atomic.Int64

	mu   sync.Mutex
	last http.Header
	raw  []http.Header // every request's headers, in arrival order

	gate    chan struct{}
	entered atomic.Int64 // requests currently blocked on /slow
}

func newRecUp(t *testing.T) *recUp {
	t.Helper()
	u := &recUp{gate: make(chan struct{})}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		u.mu.Lock()
		u.last = r.Header.Clone()
		u.raw = append(u.raw, r.Header.Clone())
		u.mu.Unlock()
		if r.URL.Path == "/slow" {
			u.entered.Add(1)
			defer u.entered.Add(-1)
			select {
			case <-u.gate:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *recUp) lastHeader() http.Header {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.last.Clone()
}

// release unblocks every /slow request (idempotent via recover).
func (u *recUp) release() {
	defer func() { _ = recover() }()
	close(u.gate)
}

// ---- crafted requests -------------------------------------------------------------

const defaultRemote = "198.51.100.7:4000"

// serveAs sends a GET straight into g.ServeHTTP with a chosen RemoteAddr.
// hdr is name, value pairs (canonicalised by Header.Set).
func serveAs(g *Gateway, remote, target string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.RemoteAddr = remote
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

// ---- credentials ----------------------------------------------------------------------

func sha256hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// keysFor builds API-key config from client, key, client, key ... pairs.
func keysFor(pairs ...string) []config.APIKey {
	var out []config.APIKey
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, config.APIKey{ClientID: pairs[i], SHA256: sha256hex(pairs[i+1])})
	}
	return out
}

const (
	testJWTEnv    = "TEST_JWT_SECRET"
	testJWTSecret = "unit-test-hmac-secret-0123456789-abcdef"
)

func testGetenv(k string) string {
	if k == testJWTEnv {
		return testJWTSecret
	}
	return ""
}

func jwtSettings() *config.JWTSettings {
	return &config.JWTSettings{Algorithm: config.JWTHS256, HMACSecretEnv: testJWTEnv}
}

// mintToken signs claims with secret (HS256).
func mintToken(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// goodToken is a valid token for sub, expiring in an hour (real time).
func goodToken(t *testing.T, sub string) string {
	t.Helper()
	return mintToken(t, testJWTSecret, jwt.MapClaims{"sub": sub, "exp": time.Now().Add(time.Hour).Unix()})
}

// authRoute is a route with auth mode and an optional rate limit.
func authRoute(name, prefix, upstream string, mode config.AuthMode, rl *config.RateLimit) config.Route {
	r := rt(name, "", prefix, upstream)
	r.Auth = mode
	r.RateLimit = rl
	return r
}

// withAuth sets the top-level auth settings on cfg.
func withAuth(cfg *config.Config, a config.AuthSettings) *config.Config {
	cfg.Auth = a
	return cfg
}

func rl(rps float64, burst int) *config.RateLimit {
	return &config.RateLimit{RequestsPerSecond: rps, Burst: burst}
}

func itoa(n int) string { return strconv.Itoa(n) }
