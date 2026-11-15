package controlplane

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestOps_Healthz(t *testing.T) {
	defer failOnPanic(t)
	// Healthy even when the store is down: it only says the process is up.
	fs := &faultyStore{Store: NewMemStore(), pingFn: func(context.Context) error { return errors.New("db down") }}
	e := newEnvWithStore(t, fs)
	rec := do(t, e.h, "GET", "/healthz", "", "")
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != "ok" || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("%d %q %q", rec.Code, rec.Body, rec.Header().Get("Content-Type"))
	}
}

func TestOps_Readyz(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name     string
		ping     func(context.Context) error
		wantCode int
		wantBody string
	}{
		{"store reachable", func(context.Context) error { return nil }, 200, "ok"},
		{"store down", func(context.Context) error { return errors.New("pq: connection refused 10.0.0.5") }, 503, "not ready"},
		{"schema behind", func(context.Context) error { return errors.New("schema at 1, want 2") }, 503, "not ready"},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnvWithStore(t, &faultyStore{Store: NewMemStore(), pingFn: tc.ping})
			rec := do(t, e.h, "GET", "/readyz", "", "") // no token needed
			if rec.Code != tc.wantCode {
				t.Fatalf("status %d, want %d", rec.Code, tc.wantCode)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tc.wantBody {
				t.Fatalf("body %q, want %q (no error details)", got, tc.wantBody)
			}
			if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
				t.Fatalf("Content-Type %q", rec.Header().Get("Content-Type"))
			}
		})
	}
}

func TestOps_ReadyzRecoversWhenStoreComesBack(t *testing.T) {
	defer failOnPanic(t)
	down := true
	fs := &faultyStore{Store: NewMemStore(), pingFn: func(context.Context) error {
		if down {
			return errors.New("down")
		}
		return nil
	}}
	e := newEnvWithStore(t, fs)
	if rec := do(t, e.h, "GET", "/readyz", "", ""); rec.Code != 503 {
		t.Fatalf("status %d", rec.Code)
	}
	down = false
	if rec := do(t, e.h, "GET", "/readyz", "", ""); rec.Code != 200 {
		t.Fatalf("status %d after recovery", rec.Code)
	}
}

func TestOps_ReadyzTimesOutAHungStore(t *testing.T) {
	defer failOnPanic(t)
	if testing.Short() {
		t.Skip("waits for ReadyzTimeout")
	}
	fs := &faultyStore{Store: NewMemStore(), pingFn: func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return nil
		}
	}}
	e := newEnvWithStore(t, fs)
	done := make(chan int, 1)
	start := time.Now()
	go func() { done <- do(t, e.h, "GET", "/readyz", "", "").Code }()
	select {
	case code := <-done:
		if code != 503 {
			t.Fatalf("status %d, want 503", code)
		}
		if d := time.Since(start); d < ReadyzTimeout/2 || d > ReadyzTimeout+2*time.Second {
			t.Fatalf("answered after %v, want about %v", d, ReadyzTimeout)
		}
	case <-time.After(ReadyzTimeout + 3*time.Second):
		t.Fatal("/readyz hung with the store")
	}
}

func TestOps_Metrics(t *testing.T) {
	defer failOnPanic(t)
	run(t, "served from Options.Metrics without a token", func(t *testing.T) {
		m := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = w.Write([]byte("tollgate_test_metric 1\n"))
		})
		e := newEnv(t, func(o *Options) { o.Metrics = m })
		rec := do(t, e.h, "GET", "/metrics", "", "")
		if rec.Code != 200 || rec.Body.String() != "tollgate_test_metric 1\n" {
			t.Fatalf("%d %q", rec.Code, rec.Body)
		}
	})
	run(t, "404 when Options.Metrics is nil", func(t *testing.T) {
		e := newEnv(t)
		if rec := do(t, e.h, "GET", "/metrics", "", ""); rec.Code != 404 {
			t.Fatalf("status %d, want 404", rec.Code)
		}
	})
}

func TestOps_NilLoggerAndZeroDurationsUseDefaults(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t, func(o *Options) {
		o.Logger = nil
		o.Heartbeat, o.StreamWriteTimeout, o.StoreTimeout = 0, 0, 0
	})
	e.seed(t)
	if rec := e.admin(t, "GET", "/v1/config", ""); rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
}
