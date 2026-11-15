package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// endingSource delivers one config and then closes its channel on its own,
// like a source that has given up, while the process is still running.
// ConfigSource says only ctx cancellation closes the channel, so a real
// FileSource cannot do this; this is the seam for testing the reaction.
type endingSource struct {
	cfg  *config.Config
	once sync.Once
	// endAfterCancel makes the source behave correctly (close on ctx done
	// only), for the control test.
	endAfterCancel bool
}

func (s *endingSource) Watch(ctx context.Context) (<-chan *config.Config, error) {
	ch := make(chan *config.Config, 1)
	ch <- s.cfg
	go func() {
		if s.endAfterCancel {
			<-ctx.Done()
		} else {
			time.Sleep(100 * time.Millisecond) // let the config apply first
		}
		s.once.Do(func() { close(ch) })
	}()
	return ch, nil
}

// callRunWithSource runs the seam, converting the stub's panic into a test
// failure instead of crashing the whole test binary.
func callRunWithSource(t *testing.T, ctx context.Context, args []string, src config.ConfigSource) <-chan int {
	t.Helper()
	done := make(chan int, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("runWithSource panicked: %v", r)
				done <- -1
			}
		}()
		done <- runWithSource(ctx, args, io.Discard,
			func(string, time.Duration, *slog.Logger) config.ConfigSource { return src })
	}()
	return done
}

func testArgs(t *testing.T, upstreamURL string) []string {
	return []string{"-config", writeCfg(t, upstreamURL), "-listen", freeAddr(t), "-admin-listen", freeAddr(t)}
}

// D12: if the config source's stream ends while nobody asked the process to
// stop, the reconcile loop is gone (health checks and limiter sweepers stop
// with it) but the listeners would keep serving a frozen config. run must
// treat that as a failure and exit non-zero promptly.
func TestRunExitsNonZeroWhenConfigStreamEndsUnexpectedly(t *testing.T) {
	up := okUpstream(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // never canceled before the assertion: the process was NOT asked to stop

	src := &endingSource{cfg: &config.Config{
		Version:   1,
		Routes:    []config.Route{{Name: "api", PathPrefix: "/api", Upstream: "u"}},
		Upstreams: []config.Upstream{{Name: "u", Backends: []string{up.URL}}},
	}}
	done := callRunWithSource(t, ctx, testArgs(t, up.URL), src)

	select {
	case code := <-done:
		if code == 0 {
			t.Fatal("run returned 0 after the config stream ended on its own, want non-zero")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run kept serving after the config stream ended on its own; want a prompt non-zero exit")
	}
}

// Control: the same source, but it only closes after ctx is canceled, is a
// clean shutdown and exits 0.
func TestRunWithSourceCleanShutdownStillExitsZero(t *testing.T) {
	up := okUpstream(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := &endingSource{endAfterCancel: true, cfg: &config.Config{
		Version:   1,
		Routes:    []config.Route{{Name: "api", PathPrefix: "/api", Upstream: "u"}},
		Upstreams: []config.Upstream{{Name: "u", Backends: []string{up.URL}}},
	}}
	done := callRunWithSource(t, ctx, testArgs(t, up.URL), src)

	time.Sleep(300 * time.Millisecond) // well past the point where an early exit would show
	select {
	case code := <-done:
		t.Fatalf("run returned %d before it was asked to stop", code)
	default:
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit code %d after a clean shutdown, want 0", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}
