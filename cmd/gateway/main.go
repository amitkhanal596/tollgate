// Command gateway runs the Tollgate data plane. It takes its config either
// from a YAML file (-config, standalone) or from the control plane's SSE
// stream (-control-plane, ADR 0005), never both.
//
// Shutdown on SIGINT/SIGTERM follows docs/architecture.md section 7: mark
// not ready, keep serving for -shutdown-delay, drain the traffic listener,
// stop the admin listener, cancel the background context (config source,
// reconcile loop and metrics sampler), wait for every goroutine, then exit.
// A second signal kills the process immediately.
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
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/discovery"
	"github.com/amitkhanal596/tollgate/internal/metrics"
	"github.com/amitkhanal596/tollgate/internal/proxy"
)

// adminDrain is short: admin handlers answer instantly.
const adminDrain = 2 * time.Second

// envDataPlaneToken holds the bearer token for the control-plane stream.
// It is read from the environment only, never from a flag (flags show up in
// ps and in pod specs), and it is never logged.
const envDataPlaneToken = "TOLLGATE_DATAPLANE_TOKEN"

// accessLogLines is the access-log queue (metrics.AsyncWriter) in lines;
// at a few hundred bytes a line it is a few MiB at most.
const accessLogLines = 8192

// accessLogFlush bounds how long shutdown waits for queued access-log
// lines to reach a slow sink.
const accessLogFlush = 2 * time.Second

// maxNodeIDLen and validNodeID mirror the node ID rule of config.RemoteSource
// and the control plane (architecture.md section 6.4), which keep it
// unexported. Checked here only to tell "your hostname is not a usable
// default" (exit 1) apart from "your -node-id is invalid" (exit 2);
// NewRemoteSource still enforces the rule itself.
const maxNodeIDLen = 128

func validNodeID(s string) bool {
	if len(s) == 0 || len(s) > maxNodeIDLen || s[0] == '.' || s[0] == '_' || s[0] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// redactURL returns raw with any userinfo replaced by "redacted", for logs.
// It also handles strings url.Parse rejects, so not even a malformed URL
// can leak a password into the logs.
func redactURL(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		if u.User == nil {
			return raw
		}
		c := *u
		c.User = url.User("redacted")
		return c.String()
	}
	// Unparseable: cut everything between "://" and the last "@" of the
	// authority.
	rest, scheme := raw, ""
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme, rest = raw[:i+3], raw[i+3:]
	}
	authority := rest
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authority = rest[:i]
	}
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		return scheme + "redacted@" + rest[at+1:]
	}
	return raw
}

// hasUserinfo reports whether raw carries credentials (or looks like it
// does: an unparseable URL with an "@" in its authority counts).
func hasUserinfo(raw string) bool {
	if u, err := url.Parse(raw); err == nil {
		return u.User != nil
	}
	return redactURL(raw) != raw
}

// lockedWriter serializes Writes to w. The application logger and the
// access log's drain goroutine both write to the same sink; before the
// access log went asynchronous one slog handler (and its mutex) served
// both, and this keeps their lines from interleaving on any io.Writer. Not
// on the request path: only the drain goroutine and application logs take
// it.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// streamGaugeInterval is how often tollgate_config_stream_connected is
// refreshed from RemoteSource.Connected, which has no change hook.
const streamGaugeInterval = 250 * time.Millisecond

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// After the first signal, restore default signal handling, so a second
	// Ctrl-C/SIGTERM kills the process at once (skipping the shutdown delay
	// and the drain) instead of being swallowed by NotifyContext.
	context.AfterFunc(ctx, stop)
	// runEnv returns only after every goroutine it started has exited, so
	// os.Exit cannot cut anything short.
	code := runEnv(ctx, os.Args[1:], os.Stdout, os.Getenv)
	stop()
	os.Exit(code)
}

// run is runEnv with the process environment.
func run(ctx context.Context, args []string, logOut io.Writer) int {
	return runEnv(ctx, args, logOut, os.Getenv)
}

// runEnv is the whole gateway process: it serves until ctx is canceled
// (main cancels it on SIGINT/SIGTERM), then shuts down in the order of
// docs/architecture.md section 7. It returns the exit code: 0 after a clean
// shutdown, 1 on a startup failure, a component failure or a drain timeout,
// and 2 on bad flags. All output (logs, access log and flag usage) goes to
// logOut, and getenv replaces os.Getenv (TOLLGATE_DATAPLANE_TOKEN and the
// JWT secret variables), so it touches no process-global state and tests
// can call it directly.
func runEnv(ctx context.Context, args []string, logOut io.Writer, getenv func(string) string) int {
	return runGateway(ctx, args, logOut, getenv, newFileSource)
}

// newFileSource is the production factory for standalone mode.
func newFileSource(path string, poll time.Duration, logger *slog.Logger) config.ConfigSource {
	return config.NewFileSource(path, poll, logger)
}

// errSourceEnded reports that the config stream closed while nobody had asked
// the process to stop.
var errSourceEnded = errors.New("config source closed its stream unexpectedly")

// runWithSource is run with the standalone-mode ConfigSource built by
// newSource (from the -config path and -config-poll interval) instead of a
// FileSource. It is the seam for testing how the process reacts to its
// source.
func runWithSource(ctx context.Context, args []string, logOut io.Writer,
	newSource func(path string, poll time.Duration, logger *slog.Logger) config.ConfigSource) int {
	return runGateway(ctx, args, logOut, os.Getenv, newSource)
}

// runGateway is the implementation behind run, runEnv and runWithSource.
func runGateway(ctx context.Context, args []string, logOut io.Writer, getenv func(string) string,
	newSource func(path string, poll time.Duration, logger *slog.Logger) config.ConfigSource) int {
	// A FlagSet local to run, never flag.CommandLine: no global state, and
	// tests can call run repeatedly.
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.SetOutput(logOut)
	cfgPath := fs.String("config", "", "path to the gateway YAML config (standalone mode; exactly one of -config and -control-plane)")
	cpURL := fs.String("control-plane", "", "base URL of the control plane, e.g. http://controlplane:9000 (needs $"+envDataPlaneToken+")")
	nodeID := fs.String("node-id", "", "this gateway's ID at the control plane (default: $HOSTNAME, else the hostname)")
	lkgPath := fs.String("lkg-path", "", "file holding the last applied config, served at startup if the control plane is down (\"\" = off)")
	consulAddr := fs.String("consul-addr", "", "host:port of the local Consul agent; enables consul upstreams")
	accessLog := fs.Bool("access-log", true, "write one JSON line per request")
	listen := fs.String("listen", ":8080", "address for client traffic")
	adminListen := fs.String("admin-listen", ":9090", "address for /healthz, /readyz and /metrics")
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

	sink := &lockedWriter{w: logOut}
	logger := slog.New(slog.NewJSONHandler(sink, nil))
	switch {
	case (*cfgPath == "") == (*cpURL == ""):
		logger.Error("exactly one of -config and -control-plane is required")
		return 2
	case *drain <= 0:
		logger.Error("-drain-timeout must be positive", "drain_timeout", drain.String())
		return 2
	case *shutdownDelay < 0:
		logger.Error("-shutdown-delay must not be negative", "shutdown_delay", shutdownDelay.String())
		return 2
	}

	// Everything that can fail on bad input is built before the listeners
	// are bound, so such a failure has nothing to clean up.
	var (
		src    config.ConfigSource
		remote *config.RemoteSource // non-nil in control-plane mode
		// idle holds transports whose idle connections are closed once
		// the background goroutines have stopped, so no keep-alive
		// connection outlives runGateway.
		idle []*http.Transport
	)
	if *cpURL != "" {
		// Credentials belong in $TOLLGATE_DATAPLANE_TOKEN, never in a flag
		// (visible in ps and pod specs) or a URL (which ends up in logs and
		// error messages). Reject them rather than silently strip them.
		if hasUserinfo(*cpURL) {
			logger.Error("-control-plane must not contain credentials (user:password@); the data-plane token is read from $"+envDataPlaneToken,
				"control_plane", redactURL(*cpURL))
			return 2
		}
		token := getenv(envDataPlaneToken)
		if token == "" {
			logger.Error("-control-plane requires the " + envDataPlaneToken + " environment variable")
			return 1
		}
		id := *nodeID
		if id == "" {
			// $HOSTNAME first: it is the pod name in Kubernetes and goes
			// through getenv, so tests can set it.
			host := getenv("HOSTNAME")
			if host == "" {
				var err error
				if host, err = os.Hostname(); err != nil {
					logger.Error("cannot determine the hostname for the default node ID; pass -node-id", "err", err)
					return 1
				}
			}
			// The operator did not type this value, so it is an environment
			// problem (exit 1), not a flag error (exit 2).
			if !validNodeID(host) {
				logger.Error("the hostname is not a valid node ID (1-128 bytes of [A-Za-z0-9._-], starting with a letter or digit); pass -node-id",
					"hostname", host)
				return 1
			}
			id = host
		}
		// A dedicated transport (not http.DefaultTransport, which other
		// code shares) so its idle connections can be closed at exit.
		tr := http.DefaultTransport.(*http.Transport).Clone()
		idle = append(idle, tr)
		rs, err := config.NewRemoteSource(config.RemoteOptions{
			BaseURL:    *cpURL,
			Token:      token,
			NodeID:     id,
			LKGPath:    *lkgPath,
			HTTPClient: &http.Client{Transport: tr}, // Timeout must stay 0: it would cut the stream
			Logger:     logger,
		})
		if err != nil {
			// The error names the bad flag value (URL or node ID), never the token.
			logger.Error("invalid control-plane settings", "err", err)
			return 2
		}
		src, remote = rs, rs
		*nodeID = id
	} else {
		if *lkgPath != "" || *nodeID != "" {
			logger.Warn("-node-id and -lkg-path only apply with -control-plane; ignored")
		}
		src = newSource(*cfgPath, *poll, logger)
	}

	var resolver discovery.Resolver
	if *consulAddr != "" {
		// DefaultConfig still honours CONSUL_HTTP_TOKEN, CONSUL_HTTP_SSL and
		// friends; only the address comes from the flag.
		ccfg := consulapi.DefaultConfig()
		ccfg.Address = *consulAddr
		// The agent is local: never route it through HTTP(S)_PROXY, which
		// the default transport honours (same reasoning as the backend
		// transports and health probes).
		ccfg.Transport.Proxy = nil
		idle = append(idle, ccfg.Transport)
		client, err := consulapi.NewClient(ccfg)
		if err != nil {
			logger.Error("invalid -consul-addr", "addr", *consulAddr, "err", err)
			return 2
		}
		resolver = discovery.NewConsul(client, discovery.ConsulOptions{Logger: logger})
	}
	defer func() {
		for _, tr := range idle {
			tr.CloseIdleConnections()
		}
	}()

	// A private registry, never prometheus.DefaultRegisterer: no global
	// state, and runGateway can run several times in one test binary.
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)
	opts := proxy.Options{Logger: logger, Getenv: getenv, Metrics: m, Resolver: resolver}
	var accessOut *metrics.AsyncWriter
	if *accessLog {
		// Same sink and format as the logs: one JSON stream, lines told
		// apart by msg "request". The benchmark turns it off with
		// -access-log=false.
		//
		// The tradeoff: the access log goes through a bounded queue drained
		// by one goroutine, so when stdout blocks (a stuck log collector, a
		// full pipe) the gateway keeps serving and DROPS access-log lines,
		// counted in tollgate_access_log_dropped_total, instead of stalling
		// every request on a write. Application logs stay synchronous: they
		// are rare, and losing one silently would hide the problem.
		accessOut = metrics.NewAsyncWriter(sink, accessLogLines, m.AccessLogDropped)
		opts.AccessLogger = slog.New(slog.NewJSONHandler(accessOut, nil))
	}
	// closeAccessLog flushes and stops the access-log writer. Idempotent
	// (AsyncWriter.Close is), and a no-op with -access-log=false.
	closeAccessLog := func() {
		if accessOut == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), accessLogFlush)
		defer cancel()
		if err := accessOut.Close(ctx); err != nil {
			logger.Warn("access log not fully flushed: the log sink is not accepting writes", "err", err)
		}
	}

	// Bind both listeners before starting anything, so a port clash fails
	// fast with nothing to clean up.
	trafficLn, err := net.Listen("tcp", *listen)
	if err != nil {
		closeAccessLog()
		logger.Error("listen failed", "addr", *listen, "err", err)
		return 1
	}
	adminLn, err := net.Listen("tcp", *adminListen)
	if err != nil {
		_ = trafficLn.Close()
		closeAccessLog()
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

	gw := proxy.New(opts)
	var shuttingDown atomic.Bool

	// Every goroutine started below reports at most one error, and there
	// are at most four of them, so no send ever blocks, even after run has
	// stopped reading.
	errc := make(chan error, 4)
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
	if remote != nil {
		// RemoteSource has no change hook for its connection state, so the
		// gauge is sampled. In file mode it simply stays 0.
		start(&background, "config stream gauge", func() error {
			t := time.NewTicker(streamGaugeInterval)
			defer t.Stop()
			for {
				m.SetConfigStreamConnected(remote.Connected())
				select {
				case <-bgCtx.Done():
					return nil
				case <-t.C:
				}
			}
		})
	}
	start(&traffic, "traffic server", func() error {
		return proxy.Serve(trafficCtx, trafficLn, gw, *drain, logger)
	})
	start(&admin, "admin server", func() error {
		return proxy.Serve(adminCtx, adminLn, adminHandler(gw, &shuttingDown, metrics.Handler(reg)), adminDrain, logger)
	})
	if remote != nil {
		logger.Info("gateway started", "listen", trafficLn.Addr().String(),
			"admin_listen", adminLn.Addr().String(), "control_plane", redactURL(*cpURL), "node_id", *nodeID,
			"lkg_path", *lkgPath, "consul_addr", *consulAddr, "access_log", *accessLog)
	} else {
		logger.Info("gateway started", "listen", trafficLn.Addr().String(),
			"admin_listen", adminLn.Addr().String(), "config", *cfgPath,
			"consul_addr", *consulAddr, "access_log", *accessLog)
	}

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
	// 3-4. Stop the config source, the reconcile loop (and with it the
	//      discovery watches) and the gauge sampler, and wait for them.
	stopBackground()
	background.Wait()
	// Traffic has drained, so no more access-log lines are coming (a
	// still-open upgraded connection that finishes later is dropped, not a
	// panic). Bounded: a stuck sink cannot hold up the exit.
	closeAccessLog()

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

// adminHandler serves the liveness and readiness probes and the metrics.
// They live only on the admin listener, never on the traffic port (section
// 11 row 19). /readyz stays 503 until the first config has been applied.
func adminHandler(gw *proxy.Gateway, shuttingDown *atomic.Bool, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metricsHandler)
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
