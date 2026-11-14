// Command gateway runs the Tollgate data plane from a YAML config file.
//
// Shutdown on SIGINT/SIGTERM follows docs/architecture.md section 7: mark
// not ready, drain the traffic listener, stop the admin listener, cancel the
// background context (config watcher and reconcile loop), wait for every
// goroutine, then exit.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
	// run returns only after every goroutine it started has exited, so
	// os.Exit cannot cut anything short.
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to the gateway YAML config (required)")
	listen := fs.String("listen", ":8080", "address for client traffic")
	adminListen := fs.String("admin-listen", ":9090", "address for /healthz and /readyz")
	drain := fs.Duration("drain-timeout", 15*time.Second, "how long in-flight requests may take to finish on shutdown")
	poll := fs.Duration("config-poll", 2*time.Second, "how often to check the config file for changes")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if *cfgPath == "" {
		logger.Error("missing required flag -config")
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

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	// Three contexts, canceled in shutdown order. None derives from sigCtx:
	// each stage is canceled explicitly only after the previous one has
	// finished, which a signal-derived context could not guarantee.
	trafficCtx, stopTraffic := context.WithCancel(context.Background())
	defer stopTraffic()
	adminCtx, stopAdmin := context.WithCancel(context.Background())
	defer stopAdmin()
	bgCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()

	gw := proxy.New(proxy.Options{Logger: logger})
	src := config.NewFileSource(*cfgPath, *poll, logger)
	var draining atomic.Bool

	// Every goroutine reports at most one error, so a buffer of 3 means no
	// send ever blocks, even after main has stopped reading.
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

	start(&background, "reconcile loop", func() error { return gw.Run(bgCtx, src) })
	start(&traffic, "traffic server", func() error {
		return proxy.Serve(trafficCtx, trafficLn, gw, *drain, logger)
	})
	start(&admin, "admin server", func() error {
		return proxy.Serve(adminCtx, adminLn, adminHandler(gw, &draining), adminDrain, logger)
	})
	logger.Info("gateway started", "listen", trafficLn.Addr().String(),
		"admin_listen", adminLn.Addr().String(), "config", *cfgPath)

	var errs []error
	select {
	case <-sigCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-errc:
		// A component failed on its own (e.g. config missing at startup):
		// shut the rest down in the same orderly way.
		logger.Error("component failed; shutting down", "err", err)
		errs = append(errs, err)
	}
	// Restore default signal handling: a second Ctrl-C kills immediately.
	stopSignals()

	// 1. Not ready, so load balancers stop sending new traffic here.
	draining.Store(true)
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

// adminHandler serves the liveness and readiness probes.
func adminHandler(gw *proxy.Gateway, draining *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if draining.Load() || !gw.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}
