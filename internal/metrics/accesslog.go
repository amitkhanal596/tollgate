package metrics

import (
	"context"
	"log/slog"
	"time"
	"unicode/utf8"
)

// Limits for client-controlled access-log fields. The async log queue is
// bounded in lines, not bytes, so unbounded fields would let a client pin
// gigabytes of memory; longer values are cut on a rune boundary.
const (
	MaxLogPathBytes      = 2048
	MaxLogHostBytes      = 255
	MaxLogClientIDBytes  = 256 // = config.MaxClientIDLen, so a valid client ID is never cut (metrics imports no internal package)
	MaxLogRequestIDBytes = 128
	MaxLogMethodBytes    = 32
)

// truncateUTF8 returns s cut to at most max bytes without splitting a rune.
// Values within the limit are returned as is (no allocation).
func truncateUTF8(s string, max int) (string, bool) {
	if len(s) <= max {
		return s, false
	}
	n := max
	// Back up to the start of the rune that straddles the limit.
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n], true
}

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
	var t1, t2, t3, t4, t5 bool
	rec.Path, t1 = truncateUTF8(rec.Path, MaxLogPathBytes)
	rec.Host, t2 = truncateUTF8(rec.Host, MaxLogHostBytes)
	rec.ClientID, t3 = truncateUTF8(rec.ClientID, MaxLogClientIDBytes)
	rec.RequestID, t4 = truncateUTF8(rec.RequestID, MaxLogRequestIDBytes)
	rec.Method, t5 = truncateUTF8(rec.Method, MaxLogMethodBytes)
	truncated := t1 || t2 || t3 || t4 || t5
	// LogAttrs avoids the []any boxing that logger.Info would do.
	attrs := [14]slog.Attr{
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
	}
	n := 13
	if truncated {
		attrs[n] = slog.Bool("truncated", true)
		n++
	}
	logger.LogAttrs(ctx, level, "request", attrs[:n]...)
}
