// Command gateway runs the Tollgate data plane from a YAML config file.
//
// Shutdown on SIGINT/SIGTERM follows docs/architecture.md section 7: mark
// not ready, keep serving for -shutdown-delay, drain the traffic listener,
// stop the admin listener, cancel the background context (config watcher
// and reconcile loop), wait for every goroutine, then exit. A second signal
// kills the process immediately.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/proxy"
)

// adminDrain is short: admin handlers answer instantly.
const adminDrain = 2 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// After the first signal, restore default signal handling, so a second
	// Ctrl-C/SIGTERM kills the process at once (skipping the shutdown delay
	// and the drain) instead of being swallowed by NotifyContext.
	context.AfterFunc(ctx, stop)
	// run returns only after every goroutine it started has exited, so
	// os.Exit cannot cut anything short.
	code := run(ctx, os.Args[1:], os.Stdout)
	stop()
	os.Exit(code)
}

// run is the whole gateway process: it serves until ctx is canceled (main
// cancels it on SIGINT/SIGTERM), then shuts down in the order of
// docs/architecture.md section 7. It returns the exit code: 0 after a clean
// shutdown, 1 on a startup failure, a component failure or a drain timeout,
// and 2 on bad flags. All output (logs and flag usage) goes to logOut, so
// run touches no process-global state and tests can call it directly.
func run(ctx context.Context, args []string, logOut io.Writer) int {
	return runWithSource(ctx, args, logOut,
		func(path string, poll time.Duration, logger *slog.Logger) config.ConfigSource {
			return config.NewFileSource(path, poll, logger)
		})
}

// errSourceEnded reports that the config stream closed while nobody had asked
// the process to stop.
var errSourceEnded = errors.New("config source closed its stream unexpectedly")

// runWithSource is run with the ConfigSource built by newSource (from the
// -config path and -config-poll interval) instead of always a FileSource. It
// is the seam for testing how the process reacts to its source.
func runWithSource(ctx context.Context, args []string, logOut io.Writer,
	newSource func(path string, poll time.Duration, logger *slog.Logger) config.ConfigSource) int {
	// A FlagSet local to run, never flag.CommandLine: no global state, and
	// tests can call run repeatedly.
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.SetOutput(logOut)
	cfgPath := fs.String("config", "", "path to the gateway YAML config (required)")
	listen := fs.String("listen", ":8080", "address for client traffic")
	adminListen := fs.String("admin-listen", ":9090", "address for /healthz and /readyz")
	drain := fs.Duration("drain-timeout", 15*time.Second, "how long in-flight requests may take to finish on shutdown")
	shutdownDelay := fs.Duration("shutdown-delay", 0,
		"after a shutdown signal, how long to keep serving while /readyz reports 503, so load balancers stop sending traffic first")
	poll := fs.Duration("config-poll", 2*time.Second, "how often to check the config file for changes")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	logger := slog.New(slog.NewJSONHandler(logOut, nil))
	switch {
	case *cfgPath == "":
		logger.Error("missing required flag -config")
		return 2
	case *drain <= 0:
		logger.Error("-drain-timeout must be positive", "drain_timeout", drain.String())
		return 2
	case *shutdownDelay < 0:
		logger.Error("-shutdown-delay must not be negative", "shutdown_delay", shutdownDelay.String())
		return 2
	}

	// Bind both listeners before starting anything, so a port clash fails
	// fast with nothing to clean up.
	trafficLn, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("listen failed", "addr", *listen, "err", err)
		return 1
	}
	adminLn, err := net.Listen("tcp", *adminListen)
	if err != nil {
		_ = trafficLn.Close()
		logger.Error("listen failed", "addr", *adminListen, "err", err)
		return 1
	}

	// Three contexts, canceled in shutdown order. None derives from ctx:
	// each stage is canceled explicitly only after the previous one has
	// finished, which a context derived from the signal could not
	// guarantee (it would stop everything at once).
	trafficCtx, stopTraffic := context.WithCancel(context.Background())
	defer stopTraffic()
	adminCtx, stopAdmin := context.WithCancel(context.Background())
	defer stopAdmin()
	bgCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()

	gw := proxy.New(proxy.Options{Logger: logger})
	src := newSource(*cfgPath, *poll, logger)
	var shuttingDown atomic.Bool

	// Every goroutine reports at most one error, so a buffer of 3 means no
	// send ever blocks, even after run has stopped reading.
	errc := make(chan error, 3)
	var traffic, admin, background sync.WaitGroup
	start := func(wg *sync.WaitGroup, name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				errc <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}

	start(&background, "reconcile loop", func() error {
		if err := gw.Run(bgCtx, src); err != nil {
			return err
		}
		// Run also returns nil when the source closes its channel. If we did
		// not cancel bgCtx, that was not a shutdown: the reconcile loop is
		// gone, and with it the health checks and limiter sweepers, so the
		// listeners would keep serving a frozen config with no health
		// tracking. Report it as a failure so the main select shuts the
		// process down in the normal order and exits non-zero.
		if bgCtx.Err() == nil {
			return errSourceEnded
		}
		return nil
	})
	start(&traffic, "traffic server", func() error {
		return proxy.Serve(trafficCtx, trafficLn, gw, *drain, logger)
	})
	start(&admin, "admin server", func() error {
		return proxy.Serve(adminCtx, adminLn, adminHandler(gw, &shuttingDown), adminDrain, logger)
	})
	logger.Info("gateway started", "listen", trafficLn.Addr().String(),
		"admin_listen", adminLn.Addr().String(), "config", *cfgPath)

	var errs []error
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested", "shutdown_delay", shutdownDelay.String())
		// 1. Not ready, so load balancers stop sending new traffic here.
		shuttingDown.Store(true)
		// 1b. Keep serving normally for the delay: in Kubernetes, endpoint
		//     removal races SIGTERM, so new requests can still arrive for
		//     a few seconds. Stop early only if a component fails (then
		//     there is nothing left worth waiting for). A second signal
		//     skips this wait by killing the process (see main).
		if *shutdownDelay > 0 {
			delay := time.NewTimer(*shutdownDelay)
			select {
			case <-delay.C:
			case err := <-errc:
				delay.Stop()
				logger.Error("component failed during shutdown delay", "err", err)
				errs = append(errs, err)
			}
		}
	case err := <-errc:
		// A component failed on its own (e.g. config missing at startup).
		// No delay: either we never became ready, or the failed component
		// means we cannot serve properly anyway.
		logger.Error("component failed; shutting down", "err", err)
		errs = append(errs, err)
		shuttingDown.Store(true)
	}

	// 2. Stop accepting and drain in-flight requests. The admin server
	//    stays up meanwhile so probes see 503 rather than a refused port.
	stopTraffic()
	traffic.Wait()
	stopAdmin()
	admin.Wait()
	// 3-4. Stop the config watcher and reconcile loop, and wait for them.
	stopBackground()
	background.Wait()

	// All senders have exited; collect anything else they reported.
	close(errc)
	for err := range errc {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		logger.Error("gateway stopped with errors", "err", err)
		return 1
	}
	logger.Info("gateway stopped")
	return 0
}

// adminHandler serves the liveness and readiness probes. They live only on
// the admin listener, never on the traffic port (section 11 row 19).
func adminHandler(gw *proxy.Gateway, shuttingDown *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if shuttingDown.Load() || !gw.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}
