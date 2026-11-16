package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
