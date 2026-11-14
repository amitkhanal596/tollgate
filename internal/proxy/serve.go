package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server-side limits for client connections (docs/architecture.md section
// 4, "Listener and transport limits").
const (
	// readHeaderTimeout stops slowloris clients from holding a connection
	// (and a goroutine) open by trickling request headers.
	readHeaderTimeout = 5 * time.Second
	// idleTimeout closes keep-alive connections that sit unused. It is
	// longer than the usual 60 s idle timeout of a fronting load balancer,
	// so the LB closes first and never reuses a connection we just closed.
	idleTimeout = 120 * time.Second
	// maxHeaderBytes bounds the memory one request's headers can take
	// (header-flood protection); larger requests get 431.
	maxHeaderBytes = 1 << 20
)

// NewServer returns the traffic http.Server with the documented limits:
// ReadHeaderTimeout 5 s, IdleTimeout 120 s, MaxHeaderBytes 1 MiB.
//
// There is deliberately no ReadTimeout or WriteTimeout: they are absolute
// deadlines on the whole connection exchange, so they would kill streamed
// responses (SSE, long polls) and slow uploads mid-flight. The per-route
// request timeout bounds each proxied exchange instead.
func NewServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
		// BaseContext is left at its default (Background) on purpose:
		// deriving request contexts from a shutdown context would cancel
		// every in-flight request the moment shutdown starts, defeating
		// the drain.
	}
}

// Serve serves handler on ln, using NewServer's limits, until ctx is canceled, then shuts down
// gracefully: the listener is closed at once (new connections are refused),
// idle connections are closed, and in-flight requests get up to drain to
// finish.
//
// It returns nil after a clean drain, a non-nil error if the drain deadline
// passed (remaining connections are then force-closed so Serve cannot
// hang), or the server's error if it failed before ctx was canceled. Serve
// does not return before its serving goroutine has exited. A nil logger
// discards logs.
func Serve(ctx context.Context, ln net.Listener, handler http.Handler, drain time.Duration, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	srv := NewServer(handler)
	srv.ErrorLog = slog.NewLogLogger(logger.Handler(), slog.LevelWarn)

	// Buffered so the goroutine can always deliver its result and exit,
	// whichever branch below we take.
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		// Serve only returns on its own if the listener failed; Shutdown
		// has not been called, so this is never ErrServerClosed.
		return fmt.Errorf("proxy: serve %s: %w", ln.Addr(), err)
	case <-ctx.Done():
	}

	addr := ln.Addr().String()
	logger.Info("draining connections", "addr", addr, "drain", drain.String())
	drainCtx, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	shutdownErr := srv.Shutdown(drainCtx)
	if shutdownErr != nil {
		// Drain deadline passed with requests still running. Close forces
		// their connections shut, which also cancels their request
		// contexts, so the gateway's upstream calls are aborted too.
		//
		// We deliberately do NOT wait for those handlers to return: the
		// process is exiting, and waiting on handlers that already
		// outlived the deadline would defeat the deadline (and could hang
		// shutdown forever on a handler that ignores its context).
		_ = srv.Close()
	}
	// Shutdown closes the listener first, so Serve has returned (or is
	// about to) with ErrServerClosed; wait so no goroutine outlives us.
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Warn("serve returned unexpected error during shutdown", "addr", addr, "err", err)
	}
	if shutdownErr != nil {
		return fmt.Errorf("proxy: drain %s did not finish within %v: %w", addr, drain, shutdownErr)
	}
	logger.Info("drained", "addr", addr)
	return nil
}
