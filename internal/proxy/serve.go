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

// Server-side limits for client connections.
const (
	// readHeaderTimeout stops slowloris clients from holding a connection
	// (and a goroutine) open by trickling request headers.
	readHeaderTimeout = 10 * time.Second
	// idleTimeout closes keep-alive connections that sit unused.
	idleTimeout = 120 * time.Second
	// There is deliberately no ReadTimeout or WriteTimeout: they would cap
	// large uploads and streamed responses. Per-route request timeouts
	// bound proxied exchanges instead.
)

// Serve serves handler on ln until ctx is canceled, then shuts down
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
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		// BaseContext is left at its default (Background) on purpose:
		// deriving request contexts from ctx would cancel every in-flight
		// request the moment shutdown starts, defeating the drain.
	}

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
