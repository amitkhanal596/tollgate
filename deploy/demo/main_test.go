package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
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
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPut && r.URL.Path == "/v1/agent/service/register":
		var body struct{ ID string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.services[body.ID] = true
		f.registers++
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/agent/service/"):
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
