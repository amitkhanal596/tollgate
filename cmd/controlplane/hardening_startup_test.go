package main

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/controlplane"
)

// hangingStore models Postgres accepting the connection and never answering:
// Head blocks until its context is done. Other methods are nil-interface
// panics on purpose; startup must not reach them.
type hangingStore struct {
	controlplane.Store
	headCalls   atomic.Int32
	sawDeadline atomic.Bool
}

func (h *hangingStore) Head(ctx context.Context) (*config.Config, error) {
	h.headCalls.Add(1)
	if _, ok := ctx.Deadline(); ok {
		h.sawDeadline.Store(true)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// Seam chosen: run gets a sibling runWithStore(ctx, args, logOut, getenv, store)
// that uses store instead of building one (run delegates to it with nil), plus a
// -startup-timeout flag (default 10s) bounding every startup store call.
func TestHardening_StartupHeadTimeout(t *testing.T) {
	seedFile := writeSeed(t, "routes: []\nupstreams: []\n")
	tests := []struct {
		name string
		args []string
	}{
		{"metrics head read (no seed)", nil},
		{"seed head read", []string{"-seed", seedFile}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &hangingStore{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel() // also unblocks Head so the goroutine never leaks
			logs := &syncBuf{}
			args := append([]string{"-memory-store", "-listen", freeAddr(t), "-startup-timeout", "200ms"}, tc.args...)

			done := make(chan int, 1)
			start := time.Now()
			go func() {
				defer func() {
					if recover() != nil {
						done <- panicCode
					}
				}()
				done <- runWithStore(ctx, args, logs, envOf(goodEnv()), store)
			}()

			select {
			case code := <-done:
				if code == panicCode {
					t.Fatalf("runWithStore panicked; logs: %s", logs.String())
				}
				if code != 1 {
					t.Errorf("exit code = %d, want 1; logs: %s", code, logs.String())
				}
				if el := time.Since(start); el > 2*time.Second {
					t.Errorf("startup took %v, want well under 2s", el)
				}
			case <-time.After(3 * time.Second):
				cancel()
				<-done
				t.Fatalf("startup hung with a Head that never answers; logs: %s", logs.String())
			}

			if store.headCalls.Load() == 0 {
				t.Errorf("injected store was never used (Head not called)")
			}
			if !store.sawDeadline.Load() {
				t.Errorf("Head ctx had no deadline")
			}
			out := logs.String()
			if !strings.Contains(out, `"level":"ERROR"`) || !strings.Contains(out, "head") {
				t.Errorf("want an ERROR log line mentioning head, got: %s", out)
			}
			if !strings.Contains(out, "deadline exceeded") && !strings.Contains(out, "timeout") && !strings.Contains(out, "timed out") {
				t.Errorf("error log should say it was a timeout, got: %s", out)
			}
		})
	}
}

func TestHardening_StartupTimeoutFlagValidation(t *testing.T) {
	for _, v := range []string{"0", "-1s"} {
		t.Run(v, func(t *testing.T) {
			code, out := runOnce(t, []string{"-memory-store", "-startup-timeout", v}, goodEnv(), 3*time.Second)
			if code != 2 || !strings.Contains(out, "startup-timeout") {
				t.Errorf("code=%d out=%s; want exit 2 naming -startup-timeout", code, out)
			}
		})
	}
}
