package config

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// C1: Validate keeps accepting an empty config (control plane starts empty).
func TestValidateAcceptsEmptyConfig(t *testing.T) {
	if err := (&Config{}).Validate(); err != nil {
		t.Fatalf("empty config must validate: %v", err)
	}
}

const zeroRoutesDoc = "version: 5\nroutes: []\nupstreams: []\n"

func TestFileSourceRejectsZeroRoutes(t *testing.T) {
	t.Run("at startup", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "c.yaml")
		writeFile(t, path, zeroRoutesDoc)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := NewFileSource(path, poll, quietLogger()).Watch(ctx)
		if err == nil || ch != nil {
			t.Fatalf("Watch = (%v, %v), want error", ch, err)
		}
	})
	t.Run("on update keeps last known good", func(t *testing.T) {
		path, ch, _ := start(t, doc(1))
		recv(t, ch)
		writeFile(t, path, zeroRoutesDoc)
		expectQuiet(t, ch, 15*poll)
		writeFile(t, path, doc(7))
		if c := recv(t, ch); c.Version != 7 {
			t.Fatalf("version = %d, want 7", c.Version)
		}
	})
}

// C1: partial writes are never delivered (two-poll debounce).
func TestFileSourceDebouncesPartialWrite(t *testing.T) {
	path, ch, _ := start(t, doc(1))
	recv(t, ch)
	// A truncated-but-valid-looking prefix: routes cut off after the first
	// route, with a different version.
	full := doc(2)
	partial := "version: 2\nroutes:\n  - name: r\n    path_prefix: /\n    upstream: u\nupstreams:\n  - name: u\n    backends: [\"http://127.0.0.1:9001\"]\ndefaults:\n  timeouts:\n    dial: 9s\n"
	writeFile(t, path, partial)
	// Final content arrives before a second poll can confirm the partial one.
	time.Sleep(poll * 3 / 2) // long enough for exactly one poll to observe the partial content
	writeFile(t, path, full)
	c := recv(t, ch)
	if c.Defaults.Timeouts.Dial != 0 {
		t.Fatalf("delivered the half-written content: %+v", c.Defaults)
	}
	if c.Version != 2 {
		t.Errorf("version = %d, want 2", c.Version)
	}
}

// A content change is applied only after it is seen on two consecutive polls.
func TestFileSourceSteadyFileDeliveredAfterDebounce(t *testing.T) {
	path, ch, _ := start(t, doc(1))
	recv(t, ch)
	writeFile(t, path, doc(2))
	start := time.Now()
	c := recv(t, ch)
	if c.Version != 2 {
		t.Fatalf("version = %d", c.Version)
	}
	if d := time.Since(start); d > 20*poll {
		t.Errorf("took %v, want within a few poll intervals", d)
	}
}

// Content that keeps changing between polls is never applied while unstable.
//
// FileSource has no injectable clock or tick channel, so the polls cannot be
// driven by hand. Instead the poll interval is long (200 ms) and the file is
// rewritten every 20 ms: for the debounce to wrongly fire, the writer would
// have to stall for more than a whole poll interval (10x the write period,
// versus 3x before). If such a stall is observed the run is inconclusive and
// is skipped rather than reported as a false failure.
func TestFileSourceUnstableContentNotApplied(t *testing.T) {
	const (
		interval    = 200 * time.Millisecond
		writeEvery  = 20 * time.Millisecond
		unstableFor = 5 * interval
	)
	path := filepath.Join(t.TempDir(), "tollgate.yaml")
	writeFile(t, path, doc(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := NewFileSource(path, interval, quietLogger()).Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	recv(t, ch)

	deadline := time.Now().Add(unstableFor)
	last := time.Now()
	var maxGap time.Duration
	for i := 100; time.Now().Before(deadline); i++ {
		writeFile(t, path, doc(i))
		if gap := time.Since(last); gap > maxGap {
			maxGap = gap
		}
		last = time.Now()
		time.Sleep(writeEvery)
		select {
		case c := <-ch:
			if maxGap >= interval {
				t.Skipf("writer stalled %v (>= poll interval); premise not established", maxGap)
			}
			t.Fatalf("delivered version %d while the file was still changing", c.Version)
		default:
		}
	}
	if maxGap >= interval {
		t.Skipf("writer stalled %v (>= poll interval); premise not established", maxGap)
	}

	// Positive control: once the content stops changing it IS delivered.
	writeFile(t, path, doc(999))
	if c := recv(t, ch); c.Version != 999 {
		t.Fatalf("stable content: version = %d, want 999", c.Version)
	}
}

// C2: strictly increasing versions.
func TestFileSourceMonotonicVersions(t *testing.T) {
	t.Run("file version then versionless", func(t *testing.T) {
		path, ch, _ := start(t, doc(10))
		if c := recv(t, ch); c.Version != 10 {
			t.Fatalf("first = %d, want 10", c.Version)
		}
		writeFile(t, path, doc(0)+"defaults:\n  timeouts:\n    dial: 3s\n")
		if c := recv(t, ch); c.Version != 11 {
			t.Fatalf("versionless after 10 = %d, want 11", c.Version)
		}
	})
	t.Run("changed content same version", func(t *testing.T) {
		path, ch, _ := start(t, doc(10))
		recv(t, ch)
		writeFile(t, path, doc(10)+"defaults:\n  timeouts:\n    dial: 3s\n")
		if c := recv(t, ch); c.Version != 11 {
			t.Fatalf("same version changed content = %d, want 11", c.Version)
		}
	})
	t.Run("jump forward", func(t *testing.T) {
		path, ch, _ := start(t, doc(10))
		recv(t, ch)
		writeFile(t, path, doc(50))
		if c := recv(t, ch); c.Version != 50 {
			t.Fatalf("jump = %d, want 50", c.Version)
		}
	})
	t.Run("lower version in file still increases", func(t *testing.T) {
		path, ch, _ := start(t, doc(10))
		recv(t, ch)
		writeFile(t, path, doc(3))
		if c := recv(t, ch); c.Version != 11 {
			t.Fatalf("lower file version = %d, want 11", c.Version)
		}
	})
	t.Run("first delivery versionless is 1", func(t *testing.T) {
		_, ch, _ := start(t, doc(0))
		if c := recv(t, ch); c.Version != 1 {
			t.Fatalf("first = %d, want 1", c.Version)
		}
	})
}

// C3: Validate gaps.
func TestValidateFixRoundRejects(t *testing.T) {
	prefix := func(p string) func(*Config) {
		return func(c *Config) { c.Routes[0].PathPrefix = p }
	}
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"host with port", func(c *Config) { c.Routes[0].Host = "api.example.com:8080" }, "host"},
		{"prefix percent escape", prefix("/a%2fb"), "path_prefix"},
		{"prefix encoded dot", prefix("/a%2eb"), "path_prefix"},
		{"prefix semicolon", prefix("/a;b"), "path_prefix"},
		{"prefix double slash", prefix("/a//b"), "path_prefix"},
		{"prefix dot segment", prefix("/a/./b"), "path_prefix"},
		{"prefix dotdot segment", prefix("/a/../b"), "path_prefix"},
		{"prefix backslash", prefix(`/a\b`), "path_prefix"},
		{"prefix control char", prefix("/a\x01b"), "path_prefix"},
		{"duplicate backend after canonicalisation", func(c *Config) {
			c.Upstreams[0].Backends = []string{"http://Example.test:80", "http://example.test"}
		}, "backend"},
		{"duplicate api key sha256", func(c *Config) {
			c.Auth.APIKeys = append(c.Auth.APIKeys, APIKey{ClientID: "other", SHA256: keyA})
		}, "sha256"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(c)
			c.Normalize()
			err := c.Validate()
			if err == nil {
				t.Fatal("want validation error")
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Errorf("error %q should mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateFixRoundAccepts(t *testing.T) {
	for _, p := range []string{"/", "/api", "/api/", "/a.b/c..d", "/~user", "/café"} {
		c := validConfig()
		c.Routes[0].PathPrefix = p
		c.Normalize()
		if err := c.Validate(); err != nil {
			t.Errorf("prefix %q rejected: %v", p, err)
		}
	}
}

func TestNormalizeFixRound(t *testing.T) {
	t.Run("hosts", func(t *testing.T) {
		c := &Config{Routes: []Route{{Host: "api.example.com."}, {Host: "[::1]"}, {Host: "API.Example.COM."}}}
		c.Normalize()
		want := []string{"api.example.com", "::1", "api.example.com"}
		for i, w := range want {
			if c.Routes[i].Host != w {
				t.Errorf("host[%d] = %q, want %q", i, c.Routes[i].Host, w)
			}
		}
	})
	t.Run("backend urls canonical", func(t *testing.T) {
		c := &Config{Upstreams: []Upstream{{Name: "u", Backends: []string{
			"http://Example.test:80", "HTTPS://Example.TEST:443", "http://example.test:8080",
		}}}}
		c.Normalize()
		want := []string{"http://example.test", "https://example.test", "http://example.test:8080"}
		for i, w := range want {
			if c.Upstreams[0].Backends[i] != w {
				t.Errorf("backend[%d] = %q, want %q", i, c.Upstreams[0].Backends[i], w)
			}
		}
	})
	t.Run("trailing slash prefix unchanged", func(t *testing.T) {
		c := &Config{Routes: []Route{{PathPrefix: "/api/"}}}
		c.Normalize()
		if c.Routes[0].PathPrefix != "/api/" {
			t.Errorf("prefix = %q", c.Routes[0].PathPrefix)
		}
	})
	t.Run("health timeout derives from interval", func(t *testing.T) {
		c := validConfig()
		c.Upstreams[0].HealthCheck = &HealthCheck{Path: "/h", Interval: Duration(500 * time.Millisecond)}
		c.Normalize()
		if got := c.Upstreams[0].HealthCheck.Timeout; got != Duration(250*time.Millisecond) {
			t.Errorf("timeout = %v, want 250ms", got.Std())
		}
		if err := c.Validate(); err != nil {
			t.Errorf("should validate: %v", err)
		}
	})
	t.Run("health defaults unchanged", func(t *testing.T) {
		c := &Config{Upstreams: []Upstream{{Name: "a", HealthCheck: &HealthCheck{Path: "/h"}}}}
		c.Normalize()
		h := c.Upstreams[0].HealthCheck
		if h.Interval != Duration(DefaultHealthInterval) || h.Timeout != Duration(DefaultHealthTimeout) {
			t.Errorf("got interval %v timeout %v", h.Interval.Std(), h.Timeout.Std())
		}
	})
	t.Run("idempotent", func(t *testing.T) {
		c := &Config{Routes: []Route{{Host: "[::1]"}}, Upstreams: []Upstream{{Name: "u", Backends: []string{"http://Example.test:80"}}}}
		c.Normalize()
		c.Normalize()
		if c.Routes[0].Host != "::1" || c.Upstreams[0].Backends[0] != "http://example.test" {
			t.Errorf("not idempotent: %+v", c)
		}
	})
}
