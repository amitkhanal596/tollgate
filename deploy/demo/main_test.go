package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHandler(t *testing.T) {
	tests := []struct {
		name       string
		steps      []string // "METHOD path" requests run in order; the last is checked
		wantCode   int
		wantPath   string // expected "path" field in JSON body, if 200 JSON
		wantNoJSON bool
	}{
		{"root", []string{"GET /"}, 200, "/", false},
		{"any path", []string{"GET /api/users/42"}, 200, "/api/users/42", false},
		{"healthz ok", []string{"GET /healthz"}, 200, "", true},
		{"fail 500 affects traffic", []string{"POST /admin/fail?mode=500", "GET /x"}, 500, "", true},
		{"fail 500 keeps healthz up", []string{"POST /admin/fail?mode=500", "GET /healthz"}, 200, "", true},
		{"unhealthy fails healthz", []string{"POST /admin/fail?mode=unhealthy", "GET /healthz"}, 503, "", true},
		{"off restores", []string{"POST /admin/fail?mode=500", "POST /admin/fail?mode=off", "GET /x"}, 200, "/x", false},
		{"slow still answers", []string{"POST /admin/fail?mode=slow", "GET /x"}, 200, "/x", false},
		{"bad mode", []string{"POST /admin/fail?mode=bogus"}, 400, "", true},
		{"fail needs POST", []string{"GET /admin/fail?mode=500"}, 405, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer("svc", "host-1")
			s.faultInjection = true
			s.sleep = func(context.Context, time.Duration) {} // do not really wait
			h := s.handler()
			var rec *httptest.ResponseRecorder
			for _, step := range tc.steps {
				var method, target string
				for i := range step {
					if step[i] == ' ' {
						method, target = step[:i], step[i+1:]
						break
					}
				}
				rec = httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
			}
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantNoJSON {
				return
			}
			var got map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("body not JSON: %v", err)
			}
			if got["service"] != "svc" || got["instance"] != "host-1" || got["path"] != tc.wantPath {
				t.Fatalf("body = %v", got)
			}
		})
	}
}

func TestProbe(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer bad.Close()
	tests := []struct {
		name string
		urls []string
		want int
	}{
		{"all ok", []string{ok.URL}, 0},
		{"one bad", []string{ok.URL, bad.URL}, 1},
		{"unreachable", []string{"http://127.0.0.1:1"}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := runProbe(tc.urls); got != tc.want {
				t.Fatalf("runProbe = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestFaultInjectionFlag(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		method   string
		target   string
		wantCode int
	}{
		{"off: POST is 404", false, "POST", "/admin/fail?mode=unhealthy", 404},
		{"off: GET is 404", false, "GET", "/admin/fail", 404},
		{"on: POST works", true, "POST", "/admin/fail?mode=500", 200},
		{"on: GET is 405", true, "GET", "/admin/fail", 405},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer("svc", "host-1")
			s.faultInjection = tc.enabled
			rec := httptest.NewRecorder()
			s.handler().ServeHTTP(rec, httptest.NewRequest(tc.method, tc.target, nil))
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantCode)
			}
			// With the flag off, traffic must be unaffected.
			rec = httptest.NewRecorder()
			s.handler().ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
			wantTraffic := 200
			if tc.enabled && tc.wantCode == 200 {
				wantTraffic = 500
			}
			if rec.Code != wantTraffic {
				t.Fatalf("traffic status = %d, want %d", rec.Code, wantTraffic)
			}
		})
	}
}

// fakeConsul implements just enough of the agent API: register, lookup, and
// a forget() that simulates a dev-agent restart.
type fakeConsul struct {
	mu        sync.Mutex
	services  map[string]bool
	registers int
	// lookupStatus, if non-zero, is returned for every service lookup.
	lookupStatus int
	deregisters  int
	// onDeregister runs (outside the lock) when a deregister call arrives.
	onDeregister func()
}

func (f *fakeConsul) setLookupStatus(code int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookupStatus = code
}

func (f *fakeConsul) forget() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.services = map[string]bool{}
}

func (f *fakeConsul) registerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registers
}

func (f *fakeConsul) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/agent/service/deregister/") {
		f.mu.Lock()
		f.deregisters++
		hook := f.onDeregister
		f.mu.Unlock()
		if hook != nil {
			hook()
		}
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPut && r.URL.Path == "/v1/agent/service/register":
		var body struct{ ID string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.services[body.ID] = true
		f.registers++
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/agent/service/"):
		if f.lookupStatus != 0 {
			http.Error(w, "injected", f.lookupStatus)
			return
		}
		if !f.services[strings.TrimPrefix(r.URL.Path, "/v1/agent/service/")] {
			http.NotFound(w, r)
		}
	default:
		http.Error(w, "unexpected", http.StatusBadRequest)
	}
}

func TestKeepRegisteredHealsAfterConsulForgets(t *testing.T) {
	fc := &fakeConsul{services: map[string]bool{}}
	srv := httptest.NewServer(fc)
	defer srv.Close()
	r := &registration{
		consul: strings.TrimPrefix(srv.URL, "http://"), id: "demo-1", service: "demo",
		addr: "127.0.0.1", port: 8080, client: srv.Client(),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.keepRegistered(ctx, 10*time.Millisecond) }()

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor("initial registration", func() bool { return fc.registerCount() == 1 })
	// While Consul still knows the service, no extra registrations happen.
	time.Sleep(60 * time.Millisecond)
	if n := fc.registerCount(); n != 1 {
		t.Fatalf("registrations = %d while registered, want 1", n)
	}
	fc.forget()
	waitFor("re-registration", func() bool { return fc.registerCount() == 2 })

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("keepRegistered did not stop after ctx cancel")
	}
}

// flakyTransport fails every request at the transport level while down is set,
// simulating Consul being unreachable or restarting.
type flakyTransport struct {
	mu   sync.Mutex
	down bool
	next http.RoundTripper
}

func (t *flakyTransport) setDown(v bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.down = v
}

func (t *flakyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.mu.Lock()
	down := t.down
	t.mu.Unlock()
	if down {
		return nil, errors.New("connection refused (simulated)")
	}
	return t.next.RoundTrip(r)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// startKeeper runs keepRegistered against fc with a 10ms interval and returns
// the registration plus a stop func that waits for the goroutine to exit.
func startKeeper(t *testing.T, fc *fakeConsul, rt http.RoundTripper) (*registration, func()) {
	t.Helper()
	srv := httptest.NewServer(fc)
	t.Cleanup(srv.Close)
	client := srv.Client()
	if rt != nil {
		client = &http.Client{Transport: rt, Timeout: time.Second}
	}
	r := &registration{
		consul: strings.TrimPrefix(srv.URL, "http://"), id: "demo-1", service: "demo",
		addr: "127.0.0.1", port: 8080, client: client,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.keepRegistered(ctx, 10*time.Millisecond) }()
	return r, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("keepRegistered did not stop after ctx cancel")
		}
	}
}

func TestKeepRegisteredIgnoresNon404Errors(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusForbidden} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			fc := &fakeConsul{services: map[string]bool{}}
			_, stop := startKeeper(t, fc, nil)
			defer stop()
			waitFor(t, "initial registration", func() bool { return fc.registerCount() == 1 })
			fc.setLookupStatus(code)
			time.Sleep(150 * time.Millisecond) // ~15 ticks
			if n := fc.registerCount(); n != 1 {
				t.Fatalf("registrations = %d with lookup status %d, want 1 (re-register would reset the check)", n, code)
			}
		})
	}
}

func TestKeepRegisteredReRegistersExactlyOnceOn404(t *testing.T) {
	fc := &fakeConsul{services: map[string]bool{}}
	_, stop := startKeeper(t, fc, nil)
	defer stop()
	waitFor(t, "initial registration", func() bool { return fc.registerCount() == 1 })
	fc.forget()
	waitFor(t, "re-registration", func() bool { return fc.registerCount() == 2 })
	time.Sleep(100 * time.Millisecond)
	if n := fc.registerCount(); n != 2 {
		t.Fatalf("registrations = %d, want exactly 2", n)
	}
}

func TestKeepRegisteredRecoversFromTransportError(t *testing.T) {
	fc := &fakeConsul{services: map[string]bool{}}
	ft := &flakyTransport{next: http.DefaultTransport}
	_, stop := startKeeper(t, fc, ft)
	defer stop()
	waitFor(t, "initial registration", func() bool { return fc.registerCount() == 1 })
	ft.setDown(true)
	fc.forget() // Consul "restarted" and lost state while unreachable
	time.Sleep(60 * time.Millisecond)
	if n := fc.registerCount(); n != 1 {
		t.Fatalf("registrations = %d while Consul unreachable, want 1", n)
	}
	ft.setDown(false)
	waitFor(t, "re-registration after recovery", func() bool { return fc.registerCount() == 2 })
	time.Sleep(100 * time.Millisecond)
	if n := fc.registerCount(); n != 2 {
		t.Fatalf("registrations = %d, want exactly 2", n)
	}
}

func TestRunRejectsBadIntervals(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"reregister zero", []string{"-reregister-interval", "0"}},
		{"reregister negative", []string{"-reregister-interval", "-5s"}},
		{"deregister-delay negative", []string{"-deregister-delay", "-1s"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			res := make(chan int, 1)
			go func() { res <- runCtx(ctx, append([]string{"-listen", "127.0.0.1:0"}, tc.args...)) }()
			// Without validation runCtx would serve until cancelled, so bound the wait.
			select {
			case code := <-res:
				cancel()
				if code != 2 {
					t.Fatalf("run = %d, want 2", code)
				}
			case <-time.After(2 * time.Second):
				cancel()
				<-res // no stray goroutine or listener left behind
				t.Fatal("run did not reject the flag value")
			}
		})
	}
}

func TestValidateIntervals(t *testing.T) {
	tests := []struct {
		name             string
		reregister, wait time.Duration
		wantFlag         string // "" means valid
	}{
		{"defaults", 15 * time.Second, 2 * time.Second, ""},
		{"zero delay ok", time.Second, 0, ""},
		{"tiny interval ok", time.Nanosecond, time.Hour, ""},
		{"zero interval", 0, time.Second, "-reregister-interval"},
		{"negative interval", -time.Second, time.Second, "-reregister-interval"},
		{"negative delay", time.Second, -time.Second, "-deregister-delay"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateIntervals(tc.reregister, tc.wait)
			switch {
			case tc.wantFlag == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantFlag != "" && (err == nil || !strings.Contains(err.Error(), tc.wantFlag)):
				t.Fatalf("err = %v, want mention of %s", err, tc.wantFlag)
			}
		})
	}
}

// TestShutdownOrderWithDeregisterDelay drives runCtx end to end: when its
// context is cancelled it must deregister first, keep serving for
// -deregister-delay, then stop. No process signal is involved.
func TestShutdownOrderWithDeregisterDelay(t *testing.T) {
	const delay = 200 * time.Millisecond
	var (
		demoAddr string
		fc       *fakeConsul
		consul   *httptest.Server
		cancel   context.CancelFunc
		res      chan int
	)
	deregSeen := make(chan time.Time, 1)
	servedAtDereg := make(chan bool, 1)
	// The port is picked, freed and re-listened by runCtx; retry if someone
	// else grabbed it in between (runCtx then returns 1 before registering).
	for attempt := 0; ; attempt++ {
		if attempt == 5 {
			t.Fatal("could not bind a demo port")
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		demoAddr = ln.Addr().String()
		_ = ln.Close()

		fc = &fakeConsul{services: map[string]bool{}}
		addr := demoAddr
		fc.onDeregister = func() {
			c := &http.Client{Timeout: time.Second}
			resp, err := c.Get("http://" + addr + "/healthz")
			ok := err == nil && resp.StatusCode == 200
			if err == nil {
				_ = resp.Body.Close()
			}
			servedAtDereg <- ok // the listener was still open when Consul saw the deregister
			deregSeen <- time.Now()
		}
		consul = httptest.NewServer(fc)
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		res = make(chan int, 1)
		go func() {
			res <- runCtx(ctx, []string{
				"-listen", addr, "-name", "n1", "-service", "demo",
				"-consul-addr", strings.TrimPrefix(consul.URL, "http://"),
				"-advertise-addr", "127.0.0.1", "-deregister-delay", delay.String(),
				"-reregister-interval", "1h",
			})
		}()
		registered := false
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && !registered {
			select {
			case <-res: // bind failed; try another port
				deadline = time.Time{}
			default:
				registered = fc.registerCount() == 1
				time.Sleep(2 * time.Millisecond)
			}
		}
		if registered {
			break
		}
		cancel()
		consul.Close()
	}
	defer consul.Close()
	t.Cleanup(func() {
		cancel()
		select {
		case <-res:
		default:
		}
	})

	cancel()
	var dereg time.Time
	select {
	case dereg = <-deregSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("no deregister call after cancel")
	}
	if !<-servedAtDereg {
		t.Fatal("demo was not serving when it deregistered")
	}
	// During the delay the demo must still answer.
	resp, err := http.Get("http://" + demoAddr + "/healthz")
	if err != nil {
		t.Fatalf("request during deregister delay failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status during delay = %d", resp.StatusCode)
	}

	select {
	case code := <-res:
		if code != 0 {
			t.Fatalf("run = %d, want 0", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return")
	}
	if elapsed := time.Since(dereg); elapsed < delay {
		t.Fatalf("stopped %v after deregistering, want at least %v", elapsed, delay)
	}
	if _, err := http.Get("http://" + demoAddr + "/healthz"); err == nil {
		t.Fatal("listener still open after run returned")
	}
}
