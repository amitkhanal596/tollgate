package config

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const poll = 10 * time.Millisecond

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// doc returns a minimal valid YAML config carrying the given version
// (version 0 omits the field).
func doc(version int) string {
	v := ""
	if version > 0 {
		v = fmt.Sprintf("version: %d\n", version)
	}
	return v + "routes:\n  - name: r\n    path_prefix: /\n    upstream: u\n" +
		"upstreams:\n  - name: u\n    backends: [\"http://127.0.0.1:9001\"]\n"
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// start writes initial content, starts a watch and returns the channel.
func start(t *testing.T, initial string) (path string, ch <-chan *Config, cancel context.CancelFunc) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "tollgate.yaml")
	writeFile(t, path, initial)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch, err := NewFileSource(path, poll, quietLogger()).Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	return path, ch, cancel
}

func recv(t *testing.T, ch <-chan *Config) *Config {
	t.Helper()
	select {
	case c, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a config")
	}
	return nil
}

// expectQuiet asserts nothing is delivered (and the channel stays open) for d.
func expectQuiet(t *testing.T, ch <-chan *Config, d time.Duration) {
	t.Helper()
	select {
	case c, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		t.Fatalf("unexpected delivery: version %d", c.Version)
	case <-time.After(d):
	}
}

func TestFileSourceInitialConfig(t *testing.T) {
	_, ch, _ := start(t, doc(1))
	c := recv(t, ch)
	if c.Version != 1 {
		t.Errorf("version = %d", c.Version)
	}
	// Rule 4: already normalized.
	if c.Upstreams[0].Algorithm != AlgorithmRoundRobin || c.Routes[0].Auth != AuthNone {
		t.Errorf("not normalized: %+v", c)
	}
	if cap(ch) != 1 {
		t.Errorf("channel capacity = %d, want 1", cap(ch))
	}
	expectQuiet(t, ch, 10*poll) // no spurious re-delivery
}

func TestFileSourceStartupErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) string
	}{
		{"missing file", func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent.yaml") }},
		{"empty file", func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "c.yaml")
			writeFile(t, p, "")
			return p
		}},
		{"malformed yaml", func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "c.yaml")
			writeFile(t, p, "routes: [unclosed\n")
			return p
		}},
		{"unknown field", func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "c.yaml")
			writeFile(t, p, doc(1)+"bogus: 1\n")
			return p
		}},
		{"semantically invalid", func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "c.yaml")
			writeFile(t, p, "routes:\n  - name: r\n    path_prefix: /\n    upstream: ghost\nupstreams: []\n")
			return p
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch, err := NewFileSource(tc.setup(t), poll, quietLogger()).Watch(ctx)
			if err == nil {
				t.Fatal("expected synchronous error")
			}
			if ch != nil {
				t.Error("channel should be nil on error")
			}
			if errors.Is(err, ErrAlreadyWatching) {
				t.Error("wrong error")
			}
		})
	}
}

func TestFileSourceSecondWatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeFile(t, path, doc(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewFileSource(path, poll, quietLogger())
	if _, err := s.Watch(ctx); err != nil {
		t.Fatal(err)
	}
	ch2, err := s.Watch(ctx)
	if !errors.Is(err, ErrAlreadyWatching) || ch2 != nil {
		t.Fatalf("got (%v, %v), want (nil, ErrAlreadyWatching)", ch2, err)
	}
}

func TestFileSourceDeliversUpdates(t *testing.T) {
	path, ch, _ := start(t, doc(1))
	recv(t, ch)
	for v := 2; v <= 4; v++ { // reading between writes: every version arrives, in order
		writeFile(t, path, doc(v))
		if c := recv(t, ch); c.Version != int64(v) {
			t.Fatalf("got version %d, want %d", c.Version, v)
		}
	}
}

func TestFileSourceInvalidUpdateSkipped(t *testing.T) {
	tests := []struct{ name, content string }{
		{"malformed yaml", "routes: [unclosed\n"},
		{"unknown field", doc(2) + "bogus: 1\n"},
		{"fails validation", "version: 2\nroutes:\n  - name: r\n    path_prefix: nope\n    upstream: u\nupstreams:\n  - name: u\n    backends: [\"http://h:1\"]\n"},
		{"empty file", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path, ch, _ := start(t, doc(1))
			recv(t, ch)
			writeFile(t, path, tc.content)
			expectQuiet(t, ch, 15*poll)
			writeFile(t, path, doc(3))
			if c := recv(t, ch); c.Version != 3 {
				t.Fatalf("got version %d, want 3", c.Version)
			}
		})
	}
}

func TestFileSourceUnchangedContentNotRedelivered(t *testing.T) {
	path, ch, _ := start(t, doc(1))
	recv(t, ch)
	writeFile(t, path, doc(1)) // identical bytes, new mtime
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	expectQuiet(t, ch, 15*poll)
}

func TestFileSourceLatestWins(t *testing.T) {
	t.Run("unread buffer is replaced", func(t *testing.T) {
		path, ch, _ := start(t, doc(1))
		// Do not read: v1 sits in the capacity-1 buffer.
		writeFile(t, path, doc(2))
		time.Sleep(6 * poll) // let the poller observe v2
		writeFile(t, path, doc(3))
		time.Sleep(6 * poll) // and v3
		if c := recv(t, ch); c.Version != 3 {
			t.Fatalf("first receive = version %d, want 3 (older values must be replaced)", c.Version)
		}
		expectQuiet(t, ch, 10*poll)
	})
	t.Run("never goes backwards", func(t *testing.T) {
		path, ch, _ := start(t, doc(1))
		recv(t, ch)
		for v := 2; v <= 6; v++ {
			writeFile(t, path, doc(v))
			time.Sleep(2 * poll)
		}
		last := int64(1)
		deadline := time.After(2 * time.Second)
		for last != 6 {
			select {
			case c := <-ch:
				if c.Version <= last {
					t.Fatalf("version %d after %d", c.Version, last)
				}
				last = c.Version
			case <-deadline:
				t.Fatalf("latest never delivered; last seen %d", last)
			}
		}
		expectQuiet(t, ch, 10*poll)
	})
}

func TestFileSourceVersionFallbackCounter(t *testing.T) {
	path, ch, _ := start(t, doc(0))
	first := recv(t, ch)
	if first.Version <= 0 {
		t.Fatalf("version without file value = %d, want > 0", first.Version)
	}
	writeFile(t, path, doc(0)+"defaults:\n  timeouts:\n    dial: 3s\n")
	second := recv(t, ch)
	if second.Version <= first.Version {
		t.Fatalf("local counter not increasing: %d then %d", first.Version, second.Version)
	}
}

func TestFileSourceAtomicReplacement(t *testing.T) {
	path, ch, _ := start(t, doc(1))
	recv(t, ch)
	tmp := path + ".tmp"
	writeFile(t, tmp, doc(2))
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	if c := recv(t, ch); c.Version != 2 {
		t.Fatalf("version = %d", c.Version)
	}
}

// Kubernetes ConfigMap style: the config path is a symlink that is swapped.
func TestFileSourceSymlinkSwap(t *testing.T) {
	dir := t.TempDir()
	d1, d2 := filepath.Join(dir, "d1"), filepath.Join(dir, "d2")
	for _, d := range []string{d1, d2} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(t, filepath.Join(d1, "c.yaml"), doc(1))
	writeFile(t, filepath.Join(d2, "c.yaml"), doc(2))
	link := filepath.Join(dir, "current")
	if err := os.Symlink(d1, link); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := NewFileSource(filepath.Join(link, "c.yaml"), poll, quietLogger()).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c := recv(t, ch); c.Version != 1 {
		t.Fatalf("version = %d", c.Version)
	}
	tmp := filepath.Join(dir, "current.tmp")
	if err := os.Symlink(d2, tmp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, link); err != nil {
		t.Fatal(err)
	}
	if c := recv(t, ch); c.Version != 2 {
		t.Fatalf("version = %d", c.Version)
	}
}

// Rule 2: a transiently unreadable file is not fatal and not a close.
func TestFileSourceTransientlyMissingFile(t *testing.T) {
	path, ch, _ := start(t, doc(1))
	recv(t, ch)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	expectQuiet(t, ch, 15*poll)
	writeFile(t, path, doc(2))
	if c := recv(t, ch); c.Version != 2 {
		t.Fatalf("version = %d", c.Version)
	}
}

func TestFileSourceClosesChannelOnlyAfterCancel(t *testing.T) {
	_, ch, cancel := start(t, doc(1))
	recv(t, ch)
	expectQuiet(t, ch, 10*poll) // still open while ctx is live
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("channel not closed after cancel")
		}
	}
}

func TestFileSourceCancelBeforeConsumerReads(t *testing.T) {
	// Buffered initial value, never read, then cancel: channel must still close.
	_, ch, cancel := start(t, doc(1))
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("channel not closed after cancel")
		}
	}
}

func TestFileSourceNoGoroutineLeak(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()
	path := filepath.Join(t.TempDir(), "c.yaml")
	writeFile(t, path, doc(1))
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := NewFileSource(path, poll, quietLogger()).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recv(t, ch)
	cancel()
	for range ch { // closed channel means the source is done
	}
	waitFor(t, 2*time.Second, "goroutines to exit", func() bool {
		return runtime.NumGoroutine() <= before
	})
}
