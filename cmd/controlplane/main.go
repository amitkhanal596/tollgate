// Command controlplane runs the Tollgate control plane: the admin API, the
// versioned config store and the SSE config stream for data planes.
//
// Shutdown on SIGINT/SIGTERM follows docs/architecture.md section 7: close
// the hub (so SSE streams end), drain the HTTP server, stop the background
// goroutines, close the database pool, exit. A second signal kills the
// process immediately.
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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/controlplane"
	"github.com/amitkhanal596/tollgate/internal/metrics"
)

// dbStartupTimeout bounds connecting to Postgres and running migrations, so
// an unreachable database fails fast instead of hanging the process.
const dbStartupTimeout = 10 * time.Second

// planesPollInterval is how often the connected-data-planes gauge is refreshed.
const planesPollInterval = 250 * time.Millisecond

// headPollTimeout bounds each head-poller store read, so a hung database
// cannot stall the poller past shutdown (its context is also canceled then).
const headPollTimeout = 5 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// Restore default signal handling after the first signal so a second
	// Ctrl-C kills the process instead of being swallowed.
	context.AfterFunc(ctx, stop)
	code := run(ctx, os.Args[1:], os.Stdout, os.Getenv)
	stop()
	os.Exit(code)
}

// run is the whole control-plane process. It returns 0 after a clean
// shutdown, 1 on a startup failure, server failure or drain timeout, and 2
// on bad flags. It touches no process-global state (flags, env and log sink
// are parameters), so tests can call it directly.
func run(ctx context.Context, args []string, logOut io.Writer, getenv func(string) string) int {
	return runWithStore(ctx, args, logOut, getenv, nil)
}

// runWithStore is run with an optional injected store. A nil store means
// "build one from the flags"; tests inject one to simulate a hung database.
func runWithStore(ctx context.Context, args []string, logOut io.Writer, getenv func(string) string, store controlplane.Store) int {
	// A FlagSet local to run, never flag.CommandLine.
	fs := flag.NewFlagSet("controlplane", flag.ContinueOnError)
	fs.SetOutput(logOut)
	listen := fs.String("listen", ":9000", "address for the admin API, config stream and /metrics")
	dbURL := fs.String("database-url", "", "Postgres connection string (default: $"+controlplane.EnvDatabaseURL+")")
	memStore := fs.Bool("memory-store", false, "keep configs in memory (development only; nothing is persisted)")
	seed := fs.String("seed", "", "YAML config to store as version 1 if the store is empty")
	drain := fs.Duration("drain-timeout", 10*time.Second, "how long open requests may take to finish on shutdown")
	heartbeat := fs.Duration("heartbeat", controlplane.DefaultHeartbeat, "config stream heartbeat interval")
	readTimeout := fs.Duration("read-timeout", 30*time.Second, "maximum time to read a whole request, body included (does not cut config streams)")
	startupTimeout := fs.Duration("startup-timeout", 10*time.Second, "bound on each store call made during startup (head read, seed); a store that does not answer in time aborts startup")
	headPoll := fs.Duration("head-poll", 2*time.Second, "how often to publish a store head newer than this replica has seen (0 disables)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	dbURLFlagSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "database-url" {
			dbURLFlagSet = true
		}
	})

	logger := slog.New(slog.NewJSONHandler(logOut, nil))
	dsn := *dbURL
	if dsn == "" {
		dsn = getenv(controlplane.EnvDatabaseURL)
	}
	switch {
	case *memStore && dbURLFlagSet:
		logger.Error("-memory-store and -database-url are mutually exclusive")
		return 2
	case !*memStore && dsn == "":
		logger.Error("no store configured: set -database-url (or " + controlplane.EnvDatabaseURL + ") or -memory-store")
		return 2
	case *drain <= 0:
		logger.Error("-drain-timeout must be positive", "drain_timeout", drain.String())
		return 2
	case *heartbeat <= 0:
		logger.Error("-heartbeat must be positive", "heartbeat", heartbeat.String())
		return 2
	case *readTimeout <= 0:
		logger.Error("-read-timeout must be positive", "read_timeout", readTimeout.String())
		return 2
	case *startupTimeout <= 0:
		logger.Error("-startup-timeout must be positive", "startup_timeout", startupTimeout.String())
		return 2
	case *headPoll < 0:
		logger.Error("-head-poll must not be negative (0 disables it)", "head_poll", headPoll.String())
		return 2
	}
	// Shutdown ends streams by closing the hub, but a handler blocked in a
	// write to a peer that stopped reading only returns when its write
	// deadline passes. With a drain no longer than that, one stuck stream
	// turns every shutdown into a drain timeout (exit code 1).
	if *drain <= controlplane.DefaultStreamWriteTimeout {
		logger.Warn("-drain-timeout is not above the stream write timeout; a stuck config stream can make the drain time out",
			"drain_timeout", drain.String(), "stream_write_timeout", controlplane.DefaultStreamWriteTimeout.String())
	}

	// Tokens come only from the environment (never flags, which show up in
	// ps) and are never logged; only the variable name is.
	adminTok, dpTok := getenv(controlplane.EnvAdminToken), getenv(controlplane.EnvDataPlaneToken)
	switch {
	case len(adminTok) < controlplane.MinTokenBytes:
		logger.Error("token missing or too short", "env", controlplane.EnvAdminToken, "min_bytes", controlplane.MinTokenBytes)
		return 1
	case len(dpTok) < controlplane.MinTokenBytes:
		logger.Error("token missing or too short", "env", controlplane.EnvDataPlaneToken, "min_bytes", controlplane.MinTokenBytes)
		return 1
	case adminTok == dpTok:
		logger.Error("tokens must differ", "env_a", controlplane.EnvAdminToken, "env_b", controlplane.EnvDataPlaneToken)
		return 1
	}

	if store != nil {
		// Injected by a test: skip building one.
	} else if *memStore {
		logger.Warn("using the in-memory store: configs are not persisted and are lost on restart (development only)")
		store = controlplane.NewMemStore()
	} else {
		startCtx, cancel := context.WithTimeout(ctx, dbStartupTimeout)
		pool, err := pgxpool.New(startCtx, dsn)
		if err != nil {
			cancel()
			// pgx errors can echo the DSN; log only that parsing/connecting failed.
			logger.Error("invalid database url")
			return 1
		}
		defer pool.Close()
		ps, err := controlplane.NewPostgresStore(startCtx, pool)
		cancel()
		if err != nil {
			logger.Error("database startup failed", "err", err)
			return 1
		}
		store = ps
	}

	hub := controlplane.NewHub()
	nodes := controlplane.NewNodes(nil, 0)

	if *seed != "" {
		if err := applySeed(ctx, store, hub, *seed, *startupTimeout, logger); err != nil {
			logger.Error("seed failed", "path", *seed, "err", err)
			return 1
		}
	}

	// Private registry, never the default one: no global state.
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	cpm := metrics.NewControlPlane(reg)

	handler, err := controlplane.NewHandler(controlplane.Options{
		Store:          store,
		Hub:            hub,
		Nodes:          nodes,
		AdminToken:     adminTok,
		DataPlaneToken: dpTok,
		Heartbeat:      *heartbeat,
		Metrics:        metrics.Handler(reg),
		Logger:         logger,
	})
	if err != nil {
		logger.Error("building handler failed", "err", err)
		return 1
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("listen failed", "addr", *listen, "err", err)
		return 1
	}

	srv := &http.Server{
		Handler:           instrument(handler, cpm),
		ReadHeaderTimeout: 10 * time.Second,
		// ReadTimeout is the only bound on a slowly trickled request body,
		// so it stays. It does NOT cut SSE streams: once the handler is
		// running, net/http clears the read deadline before its background
		// read (that watches for the client going away). Verified by
		// TestFixRound4_StreamOutlivesReadTimeout and by a reviewer on
		// Go 1.26.9 and 1.27.4.
		ReadTimeout:    *readTimeout,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 64 << 10,
		// No WriteTimeout: it would kill SSE streams. The stream handler
		// sets a per-write deadline itself.
	}
	// Closing the hub ends every SSE handler; otherwise Shutdown would wait
	// for streams that never go idle until the drain timeout.
	srv.RegisterOnShutdown(hub.Close)
	// Shutdown waits up to 5 s for connections that never sent a request
	// (e.g. a speculative dial by an HTTP client's connection pool). Close
	// those at shutdown so they cannot eat the drain budget.
	fresh := newFreshConns()
	srv.ConnState = fresh.track
	srv.RegisterOnShutdown(fresh.closeAll)

	bgCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	var background sync.WaitGroup

	// Head-version gauge: initial head now, then every version the hub sees.
	// Subscribe before reading head so no publish falls between the two.
	headCh, cancelSub := hub.Subscribe()
	// An empty store is not an error (Head returns a version-0 config), so any
	// error here is a real store failure or a timeout and must stop startup.
	headCtx, cancelHead := context.WithTimeout(ctx, *startupTimeout)
	head, err := store.Head(headCtx)
	cancelHead()
	if err != nil {
		logger.Error("startup: reading store head failed", "err", err, "startup_timeout", startupTimeout.String())
		// Nothing else has started yet; release what was acquired. The pool
		// is closed by its defer.
		cancelSub()
		_ = ln.Close()
		return 1
	}
	cpm.SetHeadVersion(head.Version)
	background.Add(2)
	go func() {
		defer background.Done()
		defer cancelSub()
		for {
			select {
			case cfg, ok := <-headCh:
				if !ok {
					return // hub closed
				}
				cpm.SetHeadVersion(cfg.Version)
			case <-bgCtx.Done():
				return
			}
		}
	}()
	// Connected-planes gauge: polled, because Nodes has no change hook.
	go func() {
		defer background.Done()
		t := time.NewTicker(planesPollInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				n := 0
				for _, ns := range nodes.List() {
					if ns.Connected {
						n++
					}
				}
				cpm.SetConnectedDataPlanes(n)
			case <-bgCtx.Done():
				return
			}
		}
	}()

	if *headPoll > 0 {
		// The hub is in-memory, so without this a write through replica A
		// never reaches gateways streaming from replica B; it also repairs
		// the rare case where a commit landed but its handler never
		// published (e.g. the request context died right after COMMIT).
		logger.Info("head poller enabled: versions committed by other control-plane replicas reach this replica's streams within one interval",
			"head_poll", headPoll.String())
		background.Add(1)
		go func() {
			defer background.Done()
			pollHead(bgCtx, store, hub, *headPoll, logger)
		}()
	} else {
		logger.Info("head poller disabled: run a single control-plane replica, or writes through another replica will not reach this replica's streams")
	}

	// Buffer 1: the serve goroutine never blocks reporting its result.
	serveErr := make(chan error, 1)
	var serving sync.WaitGroup
	serving.Add(1)
	go func() {
		defer serving.Done()
		serveErr <- srv.Serve(ln)
	}()
	logger.Info("control plane listening", "addr", ln.Addr().String())

	code := 0
	select {
	case <-ctx.Done():
		logger.Info("shutting down", "drain_timeout", drain.String())
	case err := <-serveErr:
		// Serve only returns before Shutdown on a real failure.
		logger.Error("server failed", "err", err)
		code = 1
	}

	shutCtx, cancelShut := context.WithTimeout(context.Background(), *drain)
	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Error("drain did not finish; closing remaining connections", "err", err)
		_ = srv.Close()
		code = 1
	}
	cancelShut()
	serving.Wait()
	stopBackground()
	background.Wait()
	logger.Info("stopped")
	return code
}

// applySeed stores the YAML file at path as the first version, but only if
// the store is still empty. A non-empty store is left alone: the stored
// config is the source of truth, the seed is only a bootstrap.
func applySeed(ctx context.Context, store controlplane.Store, hub *controlplane.Hub, path string, timeout time.Duration, logger *slog.Logger) error {
	// One budget for the Head read and the Update, so a hung store cannot stall startup.
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	head, err := store.Head(ctx)
	if err != nil {
		return fmt.Errorf("reading head: %w", err)
	}
	if head.Version != 0 {
		logger.Info("store already has a config; ignoring -seed", "head_version", head.Version)
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading seed file: %w", err)
	}
	seedCfg, err := config.ParseYAML(data)
	if err != nil {
		return fmt.Errorf("parsing seed file: %w", err)
	}
	// IfMatch 0 means "only if empty", so two control planes starting
	// together cannot both seed.
	zero := int64(0)
	stored, err := store.Update(ctx, controlplane.UpdateRequest{IfMatch: &zero, Author: "seed"},
		func(c *config.Config) error {
			// Replace the (empty) head with the parsed seed; Update assigns the version.
			*c = *seedCfg
			return nil
		})
	if errors.Is(err, controlplane.ErrVersionConflict) {
		// Another replica started at the same moment and won the IfMatch 0
		// write: the store is seeded, which is all this step wants.
		logger.Info("another control plane seeded the store first; ignoring -seed", "err", err)
		return nil
	}
	if err != nil {
		return fmt.Errorf("storing seed: %w", err)
	}
	hub.Publish(stored)
	logger.Info("seeded config", "version", stored.Version)
	return nil
}

// pollHead publishes the store head to hub whenever it is newer than
// anything the hub has seen, every interval, until ctx is done. Each tick
// first reads only the newest version's metadata (List with limit 1), so an
// unchanged head costs one small query rather than transferring a document
// of up to MaxConfigBytes every interval. Hub.Publish drops anything <= its
// max, so racing with a local write's own Publish is harmless.
func pollHead(ctx context.Context, store controlplane.Store, hub *controlplane.Hub, interval time.Duration, logger *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	failing := false // log a failure once, not every interval, until it recovers
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cfg, err := newerHead(ctx, store, hub.Max())
		switch {
		case err != nil && ctx.Err() != nil:
			return // shutdown canceled the read
		case err != nil:
			if !failing {
				logger.Warn("head poller: reading the store head failed; will retry", "err", err)
				failing = true
			}
			continue
		case failing:
			logger.Info("head poller: store reachable again")
			failing = false
		}
		if cfg != nil {
			hub.Publish(cfg)
			logger.Info("head poller: published a version committed outside this replica's handlers", "version", cfg.Version)
		}
	}
}

// newerHead returns the store head if its version is above have, else nil.
// Both store reads share one headPollTimeout budget.
func newerHead(ctx context.Context, store controlplane.Store, have int64) (*config.Config, error) {
	ctx, cancel := context.WithTimeout(ctx, headPollTimeout)
	defer cancel()
	latest, err := store.List(ctx, 1, 0)
	if err != nil {
		return nil, err
	}
	if len(latest) == 0 || latest[0].Version <= have {
		return nil, nil
	}
	head, err := store.Head(ctx)
	if err != nil {
		return nil, err
	}
	if head.Version <= have { // cannot go backwards, but never publish stale data
		return nil, nil
	}
	return head, nil
}

// instrument records every request in m, labelled by the mux pattern (not
// the raw path) so label cardinality stays bounded.
func instrument(next http.Handler, m interface {
	ObserveAPIRequest(method, pattern string, status int, d time.Duration)
}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		// ServeMux sets r.Pattern on this same request value while routing.
		pattern := r.Pattern
		if pattern == "" {
			pattern = "unmatched"
		}
		m.ObserveAPIRequest(r.Method, pattern, sw.status, time.Since(start))
	})
}

// statusWriter captures the response status. Unwrap lets
// http.ResponseController (used by the SSE handler for Flush and write
// deadlines) reach the real ResponseWriter.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	w.wroteHeader = true // an implicit 200 was sent
	return w.ResponseWriter.Write(p)
}

// FlushError flushes through http.ResponseController so a write error reaches
// the caller and a non-flushable writer yields http.ErrNotSupported.
func (w *statusWriter) FlushError() error {
	w.wroteHeader = true // a flush sends the headers (implicit 200)
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *statusWriter) Flush() { _ = w.FlushError() }

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// freshConns tracks connections in http.StateNew: accepted, but no request
// byte read yet. Such connections carry no work, so closing them on shutdown
// loses nothing.
type freshConns struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newFreshConns() *freshConns { return &freshConns{conns: make(map[net.Conn]struct{})} }

// track is an http.Server.ConnState hook.
func (f *freshConns) track(c net.Conn, s http.ConnState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s == http.StateNew {
		f.conns[c] = struct{}{}
		return
	}
	delete(f.conns, c) // active, idle, hijacked or closed: no longer fresh
}

func (f *freshConns) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.conns {
		_ = c.Close()
	}
	clear(f.conns)
}
