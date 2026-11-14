package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
)

// The caller's client follows redirects; the Checker must still treat a 3xx
// as a failure and must not modify the caller's client.
func TestRedirectNotFollowedEvenIfCallerClientFollows(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ok", http.StatusFound)
	})
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	b := balancer.NewBackend("svc|"+srv.URL, u)

	follows := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	c := NewChecker(follows, time.Now, nil)
	if follows.CheckRedirect != nil {
		t.Fatal("NewChecker mutated the caller's client")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	c.Sync([]Target{{b, check("/healthz", 1, 1)}})
	eventually(t, wait, "3xx marks down", func() bool { return !b.ActiveHealthy() })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// nil client, nil clock and nil callback all get working defaults, and the
// private transport's idle connections do not outlive Run.
func TestNewCheckerNilDefaults(t *testing.T) {
	f := newFakeBackend(t)
	b := f.backend(t, "svc")
	baseline := runtimeGoroutines()

	c := NewChecker(nil, nil, nil)
	if c.now == nil || c.onChange == nil || c.client == nil {
		t.Fatal("nil arguments were not defaulted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	c.Sync([]Target{{b, check("/healthz", 1, 1)}})
	eventually(t, wait, "probed with keep-alive transport", func() bool { return f.hits.Load() >= 3 })
	f.set(500)
	eventually(t, wait, "down without onChange", func() bool { return !b.ActiveHealthy() })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	f.close() // server-side keep-alive goroutines belong to the fake, not us
	goroutinesSettleTo(t, baseline)
}

func TestRunTwiceConcurrentlyFails(t *testing.T) {
	c := NewChecker(newClient(), time.Now, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	eventually(t, wait, "first Run started", c.running.Load)
	if err := c.Run(ctx); err == nil {
		t.Fatal("second concurrent Run returned nil, want error")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("first Run = %v, want nil", err)
	}
	// After it returns, Run may be called again.
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := c.Run(ctx2); err != nil {
		t.Fatalf("Run after previous Run returned = %v", err)
	}
}

func TestSettingsDefaults(t *testing.T) {
	tests := []struct {
		name              string
		in                config.HealthCheck
		interval, timeout time.Duration
		rise, fall        int
	}{
		{"zero value gets defaults", config.HealthCheck{},
			config.DefaultHealthInterval, config.DefaultHealthTimeout, config.DefaultHealthyThreshold, config.DefaultUnhealthyThreshold},
		{"negative values get defaults", config.HealthCheck{Interval: -1, Timeout: -1, HealthyThreshold: -1, UnhealthyThreshold: -1},
			config.DefaultHealthInterval, config.DefaultHealthTimeout, config.DefaultHealthyThreshold, config.DefaultUnhealthyThreshold},
		{"explicit values kept, timeout > interval allowed",
			config.HealthCheck{Interval: config.Duration(time.Second), Timeout: config.Duration(2 * time.Second), HealthyThreshold: 4, UnhealthyThreshold: 7},
			time.Second, 2 * time.Second, 4, 7},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			iv, to, r, f := settings(tc.in)
			if iv != tc.interval || to != tc.timeout || r != tc.rise || f != tc.fall {
				t.Fatalf("settings = %v %v %d %d, want %v %v %d %d", iv, to, r, f, tc.interval, tc.timeout, tc.rise, tc.fall)
			}
		})
	}
}

func TestProbeURL(t *testing.T) {
	tests := []struct{ base, path, want string }{
		{"http://h:1", "/healthz", "http://h:1/healthz"},
		{"http://h:1/", "/healthz", "http://h:1/healthz"},
		{"http://h:1", "healthz", "http://h:1/healthz"},
		{"http://h:1/api", "/health?full=1", "http://h:1/api/health?full=1"},
	}
	for _, tc := range tests {
		u, err := url.Parse(tc.base)
		if err != nil {
			t.Fatal(err)
		}
		if got := probeURL(balancer.NewBackend("x", u), tc.path); got != tc.want {
			t.Errorf("probeURL(%q, %q) = %q, want %q", tc.base, tc.path, got, tc.want)
		}
	}
}

func TestProbeOnceBadURLIsFailure(t *testing.T) {
	c := NewChecker(newClient(), time.Now, nil)
	if c.probeOnce(context.Background(), "http://bad host/\x7f", time.Second) {
		t.Fatal("unparsable URL counted as success")
	}
}
