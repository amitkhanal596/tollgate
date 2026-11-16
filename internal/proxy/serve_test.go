package proxy

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type serveHandle struct {
	addr   string
	cancel context.CancelFunc
	done   chan error
}

func startServe(t *testing.T, h http.Handler, drain time.Duration) *serveHandle {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &serveHandle{addr: ln.Addr().String(), cancel: cancel, done: make(chan error, 1)}
	go func() { s.done <- Serve(ctx, ln, h, drain, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	t.Cleanup(cancel)
	return s
}

func (s *serveHandle) wait(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case err := <-s.done:
		return err
	case <-time.After(d):
		t.Fatalf("Serve did not return within %v", d)
		return nil
	}
}

func TestServeReturnsNilOnCancelWithoutTraffic(t *testing.T) {
	s := startServe(t, http.NotFoundHandler(), time.Second)
	s.cancel()
	if err := s.wait(t, 3*time.Second); err != nil {
		t.Fatalf("Serve returned %v, want nil", err)
	}
}

func TestServeGracefulDrain(t *testing.T) {
	var finished atomic.Bool
	entered := make(chan struct{})
	s := startServe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, "done")
		finished.Store(true)
	}), 5*time.Second)

	type result struct {
		code int
		body string
		err  error
	}
	res := make(chan result, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + s.addr + "/")
		if err != nil {
			res <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		res <- result{resp.StatusCode, string(b), nil}
	}()
	<-entered
	s.cancel()

	if err := s.wait(t, 3*time.Second); err != nil {
		t.Fatalf("Serve returned %v, want nil after a clean drain", err)
	}
	if !finished.Load() {
		t.Error("Serve returned before the in-flight handler finished")
	}
	r := <-res
	if r.err != nil || r.code != 200 || r.body != "done" {
		t.Fatalf("in-flight request: %d %q err=%v, want 200 done", r.code, r.body, r.err)
	}
}

func TestServeRefusesNewConnectionsAfterCancel(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	s := startServe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	}), 5*time.Second)

	go func() {
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + s.addr + "/")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered
	s.cancel()

	// The in-flight request keeps Serve draining, yet the listener must be closed.
	waitFor(t, 2*time.Second, "new connections to be refused", func() bool {
		c, err := net.DialTimeout("tcp", s.addr, 200*time.Millisecond)
		if err != nil {
			return true
		}
		_ = c.Close()
		return false
	})
	select {
	case err := <-s.done:
		t.Fatalf("Serve returned (%v) while a request was still in flight", err)
	default:
	}
	close(release)
	if err := s.wait(t, 3*time.Second); err != nil {
		t.Fatalf("Serve returned %v, want nil", err)
	}
}

func TestServeDrainTimeoutReturnsError(t *testing.T) {
	entered := make(chan struct{})
	var once atomic.Bool
	s := startServe(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if once.CompareAndSwap(false, true) {
			close(entered)
		}
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}), 100*time.Millisecond)

	go func() {
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + s.addr + "/")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered
	start := time.Now()
	s.cancel()
	err := s.wait(t, 1500*time.Millisecond)
	if err == nil {
		t.Fatal("Serve returned nil although the handler outlived the drain deadline")
	}
	if elapsed := time.Since(start); elapsed > 1200*time.Millisecond {
		t.Errorf("Serve took %v with a 100ms drain", elapsed)
	}
}

func TestNoGoroutineLeakAfterShutdown(t *testing.T) {
	settle := func() int {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
		return runtime.NumGoroutine()
	}
	baseline := settle()

	upstream := echoUpstream(t, "A")
	g := New(Options{})
	src := newFakeSource()
	runCtx, runCancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(runCtx, src) }()
	cfg := cfgOf(1, nil, up("u", upstream.URL))
	cfg.Routes = append(cfg.Routes, rt("r", "", "/", "u"))
	src.push(cfg)
	waitFor(t, 3*time.Second, "config applied", func() bool { return g.Version() == 1 })

	s := startServe(t, g, time.Second)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
	for i := 0; i < 5; i++ {
		resp, err := client.Get("http://" + s.addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}

	s.cancel()
	if err := s.wait(t, 3*time.Second); err != nil {
		t.Fatalf("Serve returned %v", err)
	}
	runCancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}
	upstream.Close()
	client.CloseIdleConnections()

	const tolerance = 3
	var now int
	waitFor(t, 5*time.Second, "goroutines to return to baseline", func() bool {
		now = settle()
		return now <= baseline+tolerance
	})
	_ = now
}
