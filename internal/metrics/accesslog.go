package metrics

import (
	"context"
	"log/slog"
	"time"
)

// AccessRecord is one request's log line. It deliberately has no header,
// query or credential fields.
type AccessRecord struct {
	RequestID, Method, Host, Path, Route, Upstream, Backend, ClientID, RemoteIP string
	Status                                                                      int
	Duration                                                                    time.Duration
	BytesOut                                                                    int64
	ConfigVersion                                                               int64
}

// LogAccess writes one flat structured line; 5xx is WARN, everything else INFO. A nil logger is a no-op.
func LogAccess(ctx context.Context, logger *slog.Logger, rec AccessRecord) {
	if logger == nil {
		return
	}
	level := slog.LevelInfo
	if rec.Status >= 500 {
		level = slog.LevelWarn
	}
	// LogAttrs avoids the []any boxing that logger.Info would do.
	logger.LogAttrs(ctx, level, "request",
		slog.String("request_id", rec.RequestID),
		slog.String("method", rec.Method),
		slog.String("host", rec.Host),
		slog.String("path", rec.Path),
		slog.String("route", rec.Route),
		slog.String("upstream", rec.Upstream),
		slog.String("backend", rec.Backend),
		slog.String("client_id", rec.ClientID),
		slog.String("remote_ip", rec.RemoteIP),
		slog.Int("status", rec.Status),
		slog.Float64("duration_ms", float64(rec.Duration)/float64(time.Millisecond)),
		slog.Int64("bytes_out", rec.BytesOut),
		slog.Int64("config_version", rec.ConfigVersion),
	)
}
