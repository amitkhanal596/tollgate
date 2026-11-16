package proxy

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/discovery"
)

// Fix round 5, discovery items D1-D6 (ADR 0007).

// fr5WatchFor waits for a live watch of service and returns it.
func fr5WatchFor(t *testing.T, res *igResolver, service string) *igWatch {
	t.Helper()
	return fr5WatchForQ(t, res, discovery.Query{Service: service})
}

// fr5WatchForQ waits for a live watch of exactly q (a cancelled watch of the
// same service with another tag does not count).
func fr5WatchForQ(t *testing.T, res *igResolver, q discovery.Query) *igWatch {
	t.Helper()
	var found *igWatch
	waitFor(t, 3*time.Second, fmt.Sprintf("a live watch for %+v", q), func() bool {
		for _, w := range res.live() {
			if w.q == q {
				found = w
				return true
			}
		}
		return false
	})
	return found
}

// fr5Backend returns the live backend of upstream whose URL host is hp.
func fr5Backend(g *Gateway, upstream, hp string) *balancer.Backend {
	s := g.snap.Load()
	if s == nil || s.upstreams[upstream] == nil {
		return nil
	}
	for _, b := range s.upstreams[upstream].backends {
		if b.URL.Host == hp {
			return b
		}
	}
	return nil
}

func fr5WaitBackendCount(t *testing.T, g *Gateway, upstream string, n int) {
	t.Helper()
	waitFor(t, 3*time.Second, fmt.Sprintf("%d backends in upstream %s", n, upstream),
		func() bool { return len(igBackendURLs(g, upstream)) == n })
}

// fr5Logger returns a JSON logger at debug level writing into buf.
func fr5Logger(buf *igBuf) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// fr5Warns returns the WARN lines whose text mentions every one of words.
func fr5Warns(t *testing.T, buf *igBuf, words ...string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, m := range buf.jsonLines(t) {
		if m["level"] != "WARN" {
			continue
		}
		line := logsLine(m)
		all := true
		for _, w := range words {
			all = all && strings.Contains(line, w)
		}
		if all {
			out = append(out, m)
		}
	}
	return out
}

// ---- D1: readiness waits for the first endpoint sets ----------------------------

func TestFixRound5_ReadyWaitsForFirstEndpointSets(t *testing.T) {
	a := echoUpstream(t, "A")
	twoSvc := func() *config.Config {
		return cfgOf(1,
			[]config.Route{rt("ru", "", "/u", "users"), rt("ro", "", "/o", "orders")},
			discUp("users", "users", "", "http"), discUp("orders", "orders", "", "http"))
	}
	mixed := func() *config.Config {
		return cfgOf(1,
			[]config.Route{rt("rs", "", "/s", "static"), rt("ru", "", "/u", "users")},
			up("static", a.URL), discUp("users", "users", "", "http"))
	}

	tests := []struct {
		name string
		cfg  func() *config.Config
		// steps: services answered one by one; ready must stay false until
		// the last one, then become true.
		answers []struct {
			service string
			eps     []discovery.Endpoint
		}
	}{
		{"one service, empty first answer", func() *config.Config { return discCfg(1, discUp("users", "users", "", "http")) },
			[]struct {
				service string
				eps     []discovery.Endpoint
			}{{"users", []discovery.Endpoint{}}}},
		{"one service, non-empty first answer", func() *config.Config { return discCfg(1, discUp("users", "users", "", "http")) },
			[]struct {
				service string
				eps     []discovery.Endpoint
			}{{"users", []discovery.Endpoint{igEP("n/a", a)}}}},
		{"two services need both", twoSvc,
			[]struct {
				service string
				eps     []discovery.Endpoint
			}{{"users", []discovery.Endpoint{igEP("n/a", a)}}, {"orders", []discovery.Endpoint{}}}},
		{"static upstream does not count, consul one does", mixed,
			[]struct {
				service string
				eps     []discovery.Endpoint
			}{{"users", []discovery.Endpoint{}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := &igResolver{closeOnCancel: true}
			h, _ := startDisc(t, Options{}, res, tc.cfg())
			if h.g.Version() != 1 {
				t.Fatalf("Version() = %d, want 1 (the config is applied at once)", h.g.Version())
			}
			if h.g.Ready() {
				t.Fatal("Ready() = true before any service answered")
			}
			for i, ans := range tc.answers {
				consistently(t, 120*time.Millisecond, "Ready() stays false until every service answered", func() bool { return !h.g.Ready() })
				fr5WatchFor(t, res, ans.service).send(ans.eps)
				if i < len(tc.answers)-1 {
					consistently(t, 120*time.Millisecond, "Ready() stays false with a service still unanswered", func() bool { return !h.g.Ready() })
				}
			}
			waitFor(t, 3*time.Second, "Ready() after every service answered", h.g.Ready)
		})
	}
}

func TestFixRound5_ReadyWithoutAnyAnswerAfterTimeout(t *testing.T) {
	res := &igResolver{closeOnCancel: true}
	h, srv := startDisc(t, Options{DiscoveryReadyTimeout: 100 * time.Millisecond}, res,
		discCfg(1, discUp("users", "users", "", "http")))
	if h.g.Ready() {
		t.Fatal("Ready() = true right after the config was applied, before the timeout")
	}
	waitFor(t, 2*time.Second, "Ready() once DiscoveryReadyTimeout has elapsed", h.g.Ready)
	if code := getCode(t, newClient(t), srv.URL+"/x"); code != http.StatusServiceUnavailable {
		t.Errorf("status %d with an unanswered upstream, want 503", code)
	}
}

func TestFixRound5_ReadyWithoutWaitingWhenNothingCanAnswer(t *testing.T) {
	a := echoUpstream(t, "A")
	t.Run("only static upstreams, resolver set", func(t *testing.T) {
		res := &igResolver{closeOnCancel: true}
		h, srv := startDisc(t, Options{}, res, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", a.URL)))
		waitFor(t, 500*time.Millisecond, "Ready() with no consul upstream", h.g.Ready)
		if body := igCount(t, srv.URL, 1); body["A"] != 1 {
			t.Errorf("answers = %v, want A", body)
		}
	})
	t.Run("consul upstream but Resolver nil", func(t *testing.T) {
		h := startRunWith(t, Options{})
		h.apply(t, discCfg(1, discUp("users", "users", "", "http")))
		waitFor(t, 500*time.Millisecond, "Ready() when no resolver can ever answer", h.g.Ready)
	})
}

// Only the FIRST applied config is gated: later changes never flip Ready back.
func TestFixRound5_ReadyNeverFlipsBackAfterLaterConfigs(t *testing.T) {
	res := &igResolver{closeOnCancel: true}
	h, _ := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))
	fr5WatchFor(t, res, "users").send([]discovery.Endpoint{})
	waitFor(t, 3*time.Second, "Ready()", h.g.Ready)

	h.apply(t, cfgOf(2, []config.Route{rt("ru", "", "/u", "users"), rt("ro", "", "/o", "orders")},
		discUp("users", "users", "", "http"), discUp("orders", "orders", "", "http")))
	consistently(t, 200*time.Millisecond, "Ready() stays true after a config adding an unanswered service",
		func() bool { return h.g.Ready() })
}

// ---- D2: a changed query does not empty the upstream -----------------------------

func TestFixRound5_QueryChangeKeepsOldBackendsUntilNewAnswer(t *testing.T) {
	a, b := echoUpstream(t, "A"), echoUpstream(t, "B")
	tests := []struct {
		name     string
		v1, v2   config.Upstream
		oldWatch bool // v1 is consul-fed (a watch exists before v2)
	}{
		{"tag change", discUp("users", "users", "v1", "http"), discUp("users", "users", "v2", "http"), true},
		{"service rename", discUp("users", "users", "", "http"), discUp("users", "users-v2", "", "http"), true},
		{"static to consul", up("users", a.URL), discUp("users", "users", "", "http"), false},
	}
	answers := []struct {
		name string
		eps  []discovery.Endpoint
		want string // body of every request once the answer is live
	}{
		{"new watch answers B", []discovery.Endpoint{igEP("n/b", b)}, "B"},
		{"new watch answers an empty set", []discovery.Endpoint{}, "status-503"},
	}
	for _, tc := range tests {
		for _, ans := range answers {
			t.Run(tc.name+"/"+ans.name, func(t *testing.T) {
				res := &igResolver{closeOnCancel: true}
				h, srv := startDisc(t, Options{}, res, discCfg(1, tc.v1))
				if tc.oldWatch {
					res.waitWatches(t, 1).send([]discovery.Endpoint{igEP("n/a", a)})
				}
				waitFor(t, 3*time.Second, "v1 traffic to reach A", func() bool { return igCount(t, srv.URL, 2)["A"] == 2 })

				h.apply(t, discCfg(2, tc.v2))
				// The new watch has not answered: no 503 window, the old set serves.
				if got := igCount(t, srv.URL, 30); got["A"] != 30 {
					t.Fatalf("answers right after the query change = %v, want all 30 from the old backend A", got)
				}
				nw := fr5WatchForQ(t, res, discovery.Query{Service: tc.v2.Consul.Service, Tag: tc.v2.Consul.Tag})
				nw.send(ans.eps)
				waitFor(t, 3*time.Second, "traffic to follow the new watch's answer ("+ans.name+")", func() bool {
					return igCount(t, srv.URL, 4)[ans.want] == 4
				})
			})
		}
	}
}

// ---- D3: discovered endpoints are validated ---------------------------------------

func TestFixRound5_InvalidDiscoveredEndpointsAreDroppedWithWarn(t *testing.T) {
	good := echoUpstream(t, "GOOD")
	bad := []string{
		"127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:99999", ":80", "[::1]:0",
		"evil.example/../x?y:80", "a b:80", "host:80/path", "user@host:80", "bad_host:80", "no-port",
	}
	check := func(t *testing.T, badAddrs []string) {
		t.Helper()
		var logs igBuf
		res := &igResolver{closeOnCancel: true}
		h, srv := startDisc(t, Options{Logger: fr5Logger(&logs)}, res, discCfg(1, discUp("api", "svc-users", "", "http")))
		eps := []discovery.Endpoint{igEP("n/good", good)}
		for i, a := range badAddrs {
			eps = append(eps, discovery.Endpoint{ID: fmt.Sprintf("n/bad%d", i), Address: a})
		}
		res.waitWatches(t, 1).send(eps)
		waitFor(t, 3*time.Second, "the endpoint set to be applied", func() bool { return len(igBackendURLs(h.g, "api")) >= 1 })

		if got, want := igBackendURLs(h.g, "api"), []string{"http://" + hostport(good)}; !slices.Equal(got, want) {
			t.Errorf("backends = %v, want only %v", got, want)
		}
		if got := igCount(t, srv.URL, 30); got["GOOD"] != 30 {
			t.Errorf("answers = %v, want all 30 from the valid endpoint (no 502s)", got)
		}
		if len(fr5Warns(t, &logs, "svc-users")) == 0 {
			t.Errorf("no WARN naming the service svc-users; logs:\n%s", logs.String())
		}
	}
	for _, addr := range bad {
		t.Run("bad "+addr, func(t *testing.T) { check(t, []string{addr}) })
	}
	t.Run("one good and two bad", func(t *testing.T) { check(t, []string{"127.0.0.1:0", "evil.example/../x?y:80"}) })
}

func TestFixRound5_ValidDiscoveredHostsAreKept(t *testing.T) {
	res := &igResolver{closeOnCancel: true}
	h, _ := startDisc(t, Options{}, res, discCfg(1, discUp("api", "svc", "", "http")))
	res.waitWatches(t, 1).send([]discovery.Endpoint{
		{ID: "n/1", Address: "127.0.0.1:8081"},
		{ID: "n/2", Address: "my-host.example.com:8082"},
		{ID: "n/3", Address: "[::1]:8083"},
		{ID: "n/4", Address: "localhost:65535"},
	})
	want := []string{"http://127.0.0.1:8081", "http://[::1]:8083", "http://localhost:65535", "http://my-host.example.com:8082"}
	waitFor(t, 3*time.Second, "all four valid endpoints", func() bool { return len(igBackendURLs(h.g, "api")) >= 1 })
	if got := igBackendURLs(h.g, "api"); !slices.Equal(got, want) {
		t.Errorf("backends = %v, want %v (valid IPs and DNS names must not be dropped)", got, want)
	}
}

// ---- D4: Consul flapping keeps Backend state for a grace period --------------------

func TestFixRound5_FlapKeepsPassiveEjectionWithinGrace(t *testing.T) {
	const cooloff = time.Hour
	tests := []struct {
		name      string
		grace     time.Duration // Options.BackendGrace
		away      time.Duration // clock advance while B is out of the set
		wantSame  bool
		viaOption bool
	}{
		{"re-added at once", 10 * time.Second, 0, true, true},
		{"re-added within the grace", 10 * time.Second, 9 * time.Second, true, true},
		{"re-added after the grace", 10 * time.Second, 11 * time.Second, false, true},
		{"default grace: 59s is within", 0, 59 * time.Second, true, false},
		{"default grace: 61s is past", 0, 61 * time.Second, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			a, b, c, d := newCtrlBackend(t, "A"), newCtrlBackend(t, "B"), newCtrlBackend(t, "C"), newCtrlBackend(t, "D")
			b.status.Store(500)
			res := &igResolver{closeOnCancel: true}
			u := discUp("u", "svc", "", "http")
			u.PassiveHealth = &config.PassiveHealth{ConsecutiveFailures: 2, Cooloff: config.Duration(cooloff)}
			h, srv := startDisc(t, Options{Now: clock.Now, BackendGrace: tc.grace}, res, cfgOf(1, []config.Route{rt("r", "", "/", "u")}, u))
			w := res.waitWatches(t, 1)
			cl := newClient(t)
			ep := func(id string, s *ctrlBackend) discovery.Endpoint { return igEP(id, s.srv) }

			w.send([]discovery.Endpoint{ep("n/a", a), ep("n/b", b)})
			fr5WaitBackendCount(t, h.g, "u", 2)
			burst(t, cl, srv.URL, "/x", 20)
			bOld := fr5Backend(h.g, "u", hostport(b.srv))
			if bOld == nil || bOld.Available(clock.Now()) {
				t.Fatalf("fixture: B should be passively ejected (backend=%v)", bOld)
			}

			// B drops out of the set, time passes, a rebuild happens meanwhile,
			// then B comes back.
			w.send([]discovery.Endpoint{ep("n/a", a), ep("n/c", c)})
			waitFor(t, 3*time.Second, "B to leave the live set", func() bool {
				return fr5Backend(h.g, "u", hostport(b.srv)) == nil && len(igBackendURLs(h.g, "u")) == 2
			})
			clock.Advance(tc.away)
			w.send([]discovery.Endpoint{ep("n/a", a), ep("n/c", c), ep("n/d", d)})
			fr5WaitBackendCount(t, h.g, "u", 3)
			b.setUp()
			w.send([]discovery.Endpoint{ep("n/a", a), ep("n/b", b), ep("n/c", c), ep("n/d", d)})
			fr5WaitBackendCount(t, h.g, "u", 4)

			bNew := fr5Backend(h.g, "u", hostport(b.srv))
			before := b.hits.Load()
			burst(t, cl, srv.URL, "/x", 40)
			gotHits := b.hits.Load() - before
			if tc.wantSame {
				if bNew != bOld {
					t.Error("B came back as a different *Backend, want the same one (state kept during the grace)")
				}
				if gotHits != 0 {
					t.Errorf("B got %d requests after a flap within the grace, want 0 (still ejected)", gotHits)
				}
			} else {
				if bNew == bOld {
					t.Error("B kept its old *Backend after the grace elapsed, want a fresh one")
				}
				if gotHits == 0 {
					t.Error("fresh B got no traffic after the grace elapsed")
				}
			}
		})
	}
}

func TestFixRound5_FlapKeepsInFlightCount(t *testing.T) {
	clock := newFakeClock()
	a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
	hold := make(chan struct{})
	b.hold.Store(&hold)
	release := func() { defer func() { _ = recover() }(); close(hold) }
	defer release()

	res := &igResolver{closeOnCancel: true}
	h, srv := startDisc(t, Options{Now: clock.Now, BackendGrace: 10 * time.Second}, res,
		discCfg(1, discUp("u", "svc", "", "http")))
	w := res.waitWatches(t, 1)
	both := []discovery.Endpoint{igEP("n/a", a.srv), igEP("n/b", b.srv)}
	w.send(both)
	fr5WaitBackendCount(t, h.g, "u", 2)

	result := igHold(t, srv.URL, b)
	bOld := fr5Backend(h.g, "u", hostport(b.srv))
	if bOld == nil || bOld.InFlight() != 1 {
		t.Fatalf("fixture: in-flight on B = %v, want 1", bOld)
	}

	w.send(both[:1])
	waitFor(t, 3*time.Second, "B to leave the live set", func() bool { return len(igBackendURLs(h.g, "u")) == 1 })
	w.send(both)
	fr5WaitBackendCount(t, h.g, "u", 2)
	bNew := fr5Backend(h.g, "u", hostport(b.srv))
	if bNew != bOld {
		t.Fatal("B came back as a different *Backend while a request was still in flight on it")
	}
	if n := bNew.InFlight(); n != 1 {
		t.Errorf("in-flight on the re-added B = %d, want 1 (the held request is still counted)", n)
	}
	release()
	if r := <-result; r.err != nil || r.code != 200 {
		t.Errorf("held request = %+v, want 200", r)
	}
	waitFor(t, 2*time.Second, "in-flight to return to 0", func() bool { return bNew.InFlight() == 0 })
}

// While B is out of the live set its backend series are gone, even though
// the Backend itself is kept for the grace period; they come back with it.
func TestFixRound5_FlapSeriesGoneWhileOutOfLiveSet(t *testing.T) {
	reg, m := newMetrics(t)
	a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
	res := &igResolver{closeOnCancel: true}
	h, _ := startDisc(t, Options{Metrics: m, Now: newFakeClock().Now, BackendGrace: time.Minute}, res,
		discCfg(1, discUp("u", "svc", "", "http")))
	w := res.waitWatches(t, 1)
	both := []discovery.Endpoint{igEP("n/a", a.srv), igEP("n/b", b.srv)}
	w.send(both)
	fr5WaitBackendCount(t, h.g, "u", 2)
	hp := hostport(b.srv)
	waitFor(t, 2*time.Second, "B's backend_up series", func() bool { _, ok := igBackendVal(t, reg, "tollgate_backend_up", "u", hp); return ok })

	w.send(both[:1])
	waitFor(t, 3*time.Second, "B to leave the live set", func() bool { return len(igBackendURLs(h.g, "u")) == 1 })
	consistently(t, 700*time.Millisecond, "no tollgate_backend_* series for B while it is out of the live set (also after a sampler tick)",
		func() bool { return igAnyBackendSeries(t, reg, "u", hp) == 0 })

	w.send(both)
	fr5WaitBackendCount(t, h.g, "u", 2)
	waitFor(t, 2*time.Second, "B's backend_up series to return with it", func() bool { _, ok := igBackendVal(t, reg, "tollgate_backend_up", "u", hp); return ok })
	if igAnyBackendSeries(t, reg, "u", hostport(a.srv)) == 0 {
		t.Error("A lost its series")
	}
}

// ---- D5: an endpoint rebuild does not read files or the environment ----------------

func TestFixRound5_EndpointRebuildReusesAuthenticators(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	const keyPath = "/fixround5/pub.pem"

	tests := []struct {
		name  string
		auth  config.AuthSettings
		token func(t *testing.T) string
	}{
		{"HS256", config.AuthSettings{JWT: jwtSettings()}, func(t *testing.T) string { return goodToken(t, "alice") }},
		{"RS256", config.AuthSettings{JWT: &config.JWTSettings{Algorithm: config.JWTRS256, PublicKeyFile: keyPath}},
			func(t *testing.T) string {
				s, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
					"sub": "alice", "exp": time.Now().Add(time.Hour).Unix(),
				}).SignedString(key)
				if err != nil {
					t.Fatal(err)
				}
				return s
			}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, b := echoUpstream(t, "A"), echoUpstream(t, "B")
			var failing atomic.Bool
			opts := Options{
				Getenv: func(k string) string {
					if failing.Load() {
						return ""
					}
					return testGetenv(k)
				},
				ReadFile: func(p string) ([]byte, error) {
					if failing.Load() || p != keyPath {
						return nil, errors.New("file is gone")
					}
					return pubPEM, nil
				},
			}
			res := &igResolver{closeOnCancel: true}
			cfg := withAuth(cfgOf(1, []config.Route{authRoute("r", "/", "users", config.AuthJWT, nil)},
				discUp("users", "users", "", "http")), tc.auth)
			h, srv := startDisc(t, opts, res, cfg)
			w := res.waitWatches(t, 1)
			tok := "Bearer " + tc.token(t)
			get := func() (int, string) {
				req, _ := http.NewRequest(http.MethodGet, srv.URL+"/x", nil)
				req.Header.Set("Authorization", tok)
				resp, err := newClient(t).Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = resp.Body.Close() }()
				buf := make([]byte, 16)
				n, _ := resp.Body.Read(buf)
				return resp.StatusCode, string(buf[:n])
			}
			w.send([]discovery.Endpoint{igEP("n/a", a)})
			waitFor(t, 3*time.Second, "A to serve the authenticated request", func() bool {
				code, body := get()
				return code == 200 && body == "A"
			})

			failing.Store(true) // the secret / key file disappears after v1 was built
			w.send([]discovery.Endpoint{igEP("n/b", b)})
			waitFor(t, 3*time.Second, "the endpoint update to be applied although the secret is gone", func() bool {
				code, body := get()
				return code == 200 && body == "B"
			})
			if h.g.Version() != 1 {
				t.Errorf("Version() = %d, want 1", h.g.Version())
			}
		})
	}
}

// ---- D6: an empty first answer is a loud warning ------------------------------------

func TestFixRound5_EmptyFirstAnswerLogsOneWarn(t *testing.T) {
	a := echoUpstream(t, "A")
	t.Run("empty first answer", func(t *testing.T) {
		var logs igBuf
		res := &igResolver{closeOnCancel: true}
		h, _ := startDisc(t, Options{Logger: fr5Logger(&logs)}, res, discCfg(1, discUp("api", "typo-svc", "", "http")))
		before := len(fr5Warns(t, &logs, "typo-svc"))
		res.waitWatches(t, 1).send([]discovery.Endpoint{})
		waitFor(t, 3*time.Second, "the empty set to be applied", h.g.Ready)
		waitFor(t, 2*time.Second, "a WARN naming upstream api and service typo-svc", func() bool {
			return len(fr5Warns(t, &logs, "typo-svc", "api")) > before
		})
		time.Sleep(100 * time.Millisecond)
		if n := len(fr5Warns(t, &logs, "typo-svc", "api")) - before; n != 1 {
			t.Errorf("%d WARN lines for the empty first answer, want exactly 1; logs:\n%s", n, logs.String())
		}
	})
	t.Run("non-empty first answer is quiet", func(t *testing.T) {
		var logs igBuf
		res := &igResolver{closeOnCancel: true}
		h, _ := startDisc(t, Options{Logger: fr5Logger(&logs)}, res, discCfg(1, discUp("api", "typo-svc", "", "http")))
		res.waitWatches(t, 1).send([]discovery.Endpoint{igEP("n/a", a)})
		waitFor(t, 3*time.Second, "the set to be applied", h.g.Ready)
		time.Sleep(100 * time.Millisecond)
		if n := len(fr5Warns(t, &logs, "typo-svc")); n != 0 {
			t.Errorf("%d WARN lines for a healthy service; logs:\n%s", n, logs.String())
		}
	})
}
