package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// This file holds the two data-plane endpoints (architecture §6.4 rows 14
// and 15, §6.5, §6.6). handler.go authenticates the data-plane token and
// then calls serveStream / serveStatusReport.

// streamDeps is everything the stream endpoints need from the handler.
type streamDeps struct {
	Store        Store
	Hub          *Hub
	Nodes        *Nodes
	Heartbeat    time.Duration // already defaulted by NewHandler
	WriteTimeout time.Duration // per-write deadline, already defaulted
	StoreTimeout time.Duration // already defaulted
	Logger       *slog.Logger  // never nil
}

// serveStream handles GET /v1/config/stream AFTER the data-plane token has been verified by the caller.
// It parses and validates node / since / Last-Event-ID itself and writes all error responses itself.
//
// Sequence (architecture §6.5): validate parameters, Subscribe, THEN read
// Store.Head (so a version committed and published between the two steps is
// caught by one or the other), commit the SSE headers, register the node,
// send head if it is newer than since, then loop forwarding only versions
// greater than the last one sent.
//
// The handler goroutine is the only goroutine involved (no helper
// goroutines, so nothing can leak). It returns when the client goes away
// (r.Context), when the Hub closes the subscription (shutdown), or when a
// write or flush fails, which includes a write that exceeds WriteTimeout.
func serveStream(w http.ResponseWriter, r *http.Request, d streamDeps) {
	q := r.URL.Query()
	node := q.Get(QueryNode)
	if !validNodeID(node) {
		writeStreamError(w, http.StatusBadRequest, "invalid or missing node")
		return
	}
	var since int64
	if q.Has(QuerySince) {
		v, ok := parseVersion(q.Get(QuerySince))
		if !ok {
			writeStreamError(w, http.StatusBadRequest, "since must be a non-negative decimal integer")
			return
		}
		since = v
	}
	// A non-empty Last-Event-ID wins over since (standard SSE resume); since
	// is still validated above so a malformed request is never accepted.
	if leid := r.Header.Get(HeaderLastEventID); leid != "" {
		v, ok := parseVersion(leid)
		if !ok {
			writeStreamError(w, http.StatusBadRequest, "Last-Event-ID must be a non-negative decimal integer")
			return
		}
		since = v
	}

	// Subscribe FIRST, then read head (§11 row 5). cancel is deferred so the
	// subscription is released on every return path.
	sub, cancel := d.Hub.Subscribe()
	defer cancel()

	head, err := readHead(r.Context(), d)
	if err != nil {
		d.Logger.Error("config stream: read head", "node", node, "err", err)
		writeStreamError(w, http.StatusInternalServerError, "internal error")
		return
	}

	rc := http.NewResponseController(w)
	// Clearing the write deadline does two jobs: it removes any server-wide
	// WriteTimeout (which would kill this long-lived response), and it probes
	// that per-write deadlines work at all. Without them a peer that stops
	// reading could pin this goroutine forever, so refuse to stream.
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		d.Logger.Error("config stream: response writer does not support write deadlines; refusing to stream", "node", node, "err", err)
		writeStreamError(w, http.StatusInternalServerError, "internal error")
		return
	}
	// Flush cannot be probed without committing the response, so look for
	// it the way ResponseController does; a wrapper that hides it (missing
	// Unwrap) would otherwise buffer every event forever.
	if !canFlush(w) {
		d.Logger.Error("config stream: response writer does not support flushing; refusing to stream", "node", node)
		writeStreamError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Register before any stream byte is written, so a full registry can
	// still be answered with a plain 503 (the gateway backs off and retries)
	// instead of a stream that would be invisible in GET /v1/dataplanes.
	disconnect, ok := d.Nodes.TryConnect(node, since)
	if !ok {
		d.Logger.Warn("config stream: data-plane registry is full of connected nodes; refusing stream", "node", node)
		writeStreamError(w, http.StatusServiceUnavailable, "too many connected data planes")
		return
	}
	defer disconnect()

	h := w.Header()
	h.Set("Content-Type", ContentTypeEventStream)
	h.Set("Cache-Control", "no-store")
	h.Set(HeaderAccelBuffering, "no")
	w.WriteHeader(http.StatusOK)
	sw := &sseWriter{w: w, rc: rc, timeout: d.WriteTimeout}
	// On a clean exit (hub closed) the server still writes the end of the
	// chunked body; give that write a fresh, bounded deadline because the
	// last one set may already have passed during an idle period.
	defer func() { _ = rc.SetWriteDeadline(time.Now().Add(d.WriteTimeout)) }()

	// Flush the headers now so the client sees the stream open even when
	// there is nothing to send yet.
	if err := sw.flush(); err != nil {
		d.Logger.Debug("config stream: initial flush failed", "node", node, "err", err)
		return
	}

	d.Logger.Info("config stream: connected", "node", node, "since", since, "head", head.Version)

	lastSent := since
	switch {
	case head.Version > since:
		if err := sw.event(head); err != nil {
			d.Logger.Info("config stream: write failed", "node", node, "version", head.Version, "err", err)
			return
		}
		lastSent = head.Version
		d.Nodes.Offered(node, head.Version)
	case since > head.Version:
		// Database wiped or restored from an older backup (ADR 0005,
		// Consequences): stay silent until a version > since is published.
		d.Logger.Warn("config stream: node is ahead of the store head; waiting for a newer version",
			"node", node, "since", since, "head", head.Version)
	}

	// The first tick is one interval after connect, so a pending head always
	// goes out before the first heartbeat.
	ticker := time.NewTicker(d.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			d.Logger.Info("config stream: client gone", "node", node)
			return
		case cfg, ok := <-sub:
			if !ok {
				d.Logger.Info("config stream: hub closed", "node", node)
				return
			}
			// The primed slot or a version already sent as head can repeat
			// lastSent; anything <= lastSent is not news for this client.
			if cfg.Version <= lastSent {
				continue
			}
			if err := sw.event(cfg); err != nil {
				d.Logger.Info("config stream: write failed", "node", node, "version", cfg.Version, "err", err)
				return
			}
			lastSent = cfg.Version
			d.Nodes.Offered(node, cfg.Version)
		case <-ticker.C:
			if err := sw.write([]byte(HeartbeatComment)); err != nil {
				d.Logger.Info("config stream: heartbeat failed", "node", node, "err", err)
				return
			}
			d.Nodes.Seen(node)
		}
	}
}

// readHead reads Store.Head bounded by StoreTimeout and the request context.
func readHead(ctx context.Context, d streamDeps) (*config.Config, error) {
	ctx, cancel := context.WithTimeout(ctx, d.StoreTimeout)
	defer cancel()
	head, err := d.Store.Head(ctx)
	if err == nil && head == nil {
		// Store contract says Head never returns nil without an error;
		// treat a violation as an internal error rather than panicking.
		err = errors.New("store returned a nil head")
	}
	return head, err
}

// sseWriter performs bounded writes on the stream. Each write sets a fresh
// deadline, writes and flushes, so no single write can block longer than
// timeout. It is used only by the handler goroutine and holds no lock.
type sseWriter struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	timeout time.Duration
}

func (s *sseWriter) write(p []byte) error {
	if err := s.rc.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		return err
	}
	if _, err := s.w.Write(p); err != nil {
		return err
	}
	return s.rc.Flush()
}

func (s *sseWriter) flush() error {
	if err := s.rc.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		return err
	}
	return s.rc.Flush()
}

// event writes one config event, byte-exact per api.go:
// "id: N\nevent: config\ndata: <json>\n\n". json.Marshal never emits a
// newline, so data is always a single line.
func (s *sseWriter) event(cfg *config.Config) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	buf := make([]byte, 0, len(data)+64)
	buf = append(buf, "id: "...)
	buf = strconv.AppendInt(buf, cfg.Version, 10)
	buf = append(buf, "\nevent: "+EventConfig+"\ndata: "...)
	buf = append(buf, data...)
	buf = append(buf, "\n\n"...)
	return s.write(buf)
}

// canFlush reports whether http.ResponseController can flush w, following
// the same Unwrap chain ResponseController follows.
func canFlush(w http.ResponseWriter) bool {
	for {
		switch t := w.(type) {
		case interface{ FlushError() error }, http.Flusher:
			return true
		case interface{ Unwrap() http.ResponseWriter }:
			w = t.Unwrap()
		default:
			return false
		}
	}
}

// serveStatusReport handles POST /v1/dataplanes/{node}/status AFTER the data-plane token has been verified.
// It enforces the body limit, strict JSON decoding and field limits itself.
//
// Check order follows §6.3 rule 2: path (400), body size (413), decode (400),
// field limits (400). Nothing is recorded unless every check passes.
func serveStatusReport(w http.ResponseWriter, r *http.Request, d streamDeps) {
	node := r.PathValue("node")
	if !validNodeID(node) {
		writeStreamError(w, http.StatusBadRequest, "invalid node")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeStreamError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeStreamError(w, http.StatusBadRequest, "could not read request body")
		return
	}

	var rep StatusReport
	if err := decodeStrictJSON(body, &rep); err != nil {
		writeStreamError(w, http.StatusBadRequest, "invalid status report: "+err.Error())
		return
	}
	switch {
	case rep.Applied < 0:
		writeStreamError(w, http.StatusBadRequest, "applied must be >= 0")
		return
	case rep.LastRejected < 0:
		writeStreamError(w, http.StatusBadRequest, "last_rejected must be >= 0")
		return
	case len(rep.Error) > MaxReportErrorBytes:
		writeStreamError(w, http.StatusBadRequest, "error longer than "+strconv.Itoa(MaxReportErrorBytes)+" bytes")
		return
	}

	d.Nodes.Report(node, rep)
	w.WriteHeader(http.StatusNoContent)
}

// decodeStrictJSON decodes exactly one JSON value into v, rejecting unknown
// fields, an empty body and anything but whitespace after the value.
func decodeStrictJSON(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("empty body")
		}
		return err
	}
	// Token (not More) because More returns false for a stray '}' or ']'.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after JSON value")
	}
	return nil
}

// validNodeID reports whether s is 1..MaxNodeIDBytes bytes of
// [A-Za-z0-9._-] starting with a letter or digit (architecture §6.4).
func validNodeID(s string) bool {
	if len(s) == 0 || len(s) > MaxNodeIDBytes {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if alnum || (i > 0 && (c == '.' || c == '_' || c == '-')) {
			continue
		}
		return false
	}
	return true
}

// parseVersion parses a non-negative decimal int64. It accepts only ASCII
// digits, because strconv.ParseInt alone would also accept "+5" and "-0".
func parseVersion(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(s, 10, 64) // still needed: overflow
	return v, err == nil
}

// writeStreamError writes an ErrorBody JSON response. Used only by the
// data-plane endpoints in this file (handler.go has its own helper).
func writeStreamError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", ContentTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorBody{Error: msg})
}
