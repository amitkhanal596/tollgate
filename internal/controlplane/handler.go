package controlplane

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// api holds what every endpoint handler needs. It is immutable after
// NewHandler returns, so handlers share it without locking.
type api struct {
	store        Store
	hub          *Hub
	nodes        *Nodes
	storeTimeout time.Duration
	logger       *slog.Logger
}

// NewHandler builds the control-plane HTTP handler: the admin REST API, the
// config stream, and the ops endpoints (docs/architecture.md §6).
//
// It returns an error (never containing a token) if Store, Hub or Nodes is
// nil, if either token is shorter than MinTokenBytes, or if the tokens are
// equal. Only SHA-256 digests of the tokens are retained.
func NewHandler(opts Options) (http.Handler, error) {
	switch {
	case opts.Store == nil:
		return nil, errors.New("controlplane: Options.Store is nil")
	case opts.Hub == nil:
		return nil, errors.New("controlplane: Options.Hub is nil")
	case opts.Nodes == nil:
		return nil, errors.New("controlplane: Options.Nodes is nil")
	case len(opts.AdminToken) < MinTokenBytes:
		return nil, fmt.Errorf("controlplane: admin token must be at least %d bytes", MinTokenBytes)
	case len(opts.DataPlaneToken) < MinTokenBytes:
		return nil, fmt.Errorf("controlplane: data-plane token must be at least %d bytes", MinTokenBytes)
	case opts.AdminToken == opts.DataPlaneToken:
		return nil, errors.New("controlplane: admin and data-plane tokens must differ")
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	a := &api{
		store:        opts.Store,
		hub:          opts.Hub,
		nodes:        opts.Nodes,
		storeTimeout: orDefault(opts.StoreTimeout, DefaultStoreTimeout),
		logger:       logger,
	}
	deps := streamDeps{
		Store:        opts.Store,
		Hub:          opts.Hub,
		Nodes:        opts.Nodes,
		Heartbeat:    orDefault(opts.Heartbeat, DefaultHeartbeat),
		WriteTimeout: orDefault(opts.StreamWriteTimeout, DefaultStreamWriteTimeout),
		StoreTimeout: a.storeTimeout,
		Logger:       logger,
	}

	admin := requireToken(opts.AdminToken)
	dataPlane := requireToken(opts.DataPlaneToken)

	// Method patterns make ServeMux answer 404/405 itself, before any token
	// check (rule 1).
	mux := http.NewServeMux()
	a.routes(mux, admin)
	mux.Handle("GET /v1/config/stream", dataPlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveStream(w, r, deps)
	})))
	mux.Handle("POST /v1/dataplanes/{node}/status", dataPlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveStatusReport(w, r, deps)
	})))

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), ReadyzTimeout)
		defer cancel()
		if err := a.store.Ping(ctx); err != nil {
			a.logger.Warn("readyz: store not ready", "err", err) // detail stays in the log
			writeText(w, http.StatusServiceUnavailable, "not ready")
			return
		}
		writeText(w, http.StatusOK, "ok")
	})
	if opts.Metrics != nil {
		mux.Handle("GET /metrics", opts.Metrics)
	}
	return mux, nil
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// requireToken returns middleware that admits only requests carrying
// "Authorization: Bearer <token>". Both sides are SHA-256 hashed so the
// comparison is constant-time regardless of length, and the raw token is
// not retained.
func requireToken(token string) func(http.Handler) http.Handler {
	want := sha256.Sum256([]byte(token))
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, ok := bearerToken(r.Header.Get(HeaderAuthorization))
			sum := sha256.Sum256([]byte(got))
			if !ok || subtle.ConstantTimeCompare(sum[:], want[:]) != 1 {
				w.Header().Set(HeaderWWWAuthenticate, "Bearer")
				writeError(w, http.StatusUnauthorized, "unauthorized") // same body for every failure mode
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// bearerToken extracts the token from "Bearer <token>": the scheme is
// case-insensitive, followed by exactly one space.
func bearerToken(h string) (string, bool) {
	const scheme = "Bearer "
	if len(h) < len(scheme) || !strings.EqualFold(h[:len(scheme)], scheme) {
		return "", false
	}
	return h[len(scheme):], true
}

// ---- response helpers ---------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		// Only possible for a programming error; keep the generic body.
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.Header().Set("Content-Type", ContentTypeJSON)
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorBody{Error: msg})
}

func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body+"\n")
}

// setETag sets ETag: "<version>".
func setETag(w http.ResponseWriter, version int64) {
	w.Header().Set(HeaderETag, `"`+strconv.FormatInt(version, 10)+`"`)
}

// storeCtx bounds the Store calls of one request (rule 9).
func (a *api) storeCtx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), a.storeTimeout)
}

// writeStoreError maps a Store error to a status (rule 9). Anything that is
// not a typed error is logged with detail and answered with a generic 500,
// so SQL text, hosts and tokens never reach the client.
func (a *api) writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrVersionConflict):
		writeError(w, http.StatusPreconditionFailed, err.Error())
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, config.ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		a.logger.Error("store error", "method", r.Method, "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// ---- request parsing ----------------------------------------------------------

// parseDecimal parses a non-negative decimal int64 made only of digits (no
// sign, no spaces, no hex).
func parseDecimal(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil { // overflow
		return 0, false
	}
	return v, true
}

// parseIfMatch parses the optional If-Match header: exactly one entity tag,
// "N" or bare N (rule 7).
func parseIfMatch(h http.Header) (*int64, error) {
	vals := h.Values(HeaderIfMatch)
	switch len(vals) {
	case 0:
		return nil, nil
	case 1:
	default:
		return nil, errors.New("If-Match must be a single entity tag")
	}
	s := strings.TrimSpace(vals[0])
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	v, ok := parseDecimal(s)
	if !ok {
		return nil, errors.New(`If-Match must be "N" or N, N a non-negative decimal integer`)
	}
	return &v, nil
}

// writeRequest parses the preconditions and audit headers shared by every
// write (rules 7 and 8). On failure it has already written the 400.
func writeRequest(w http.ResponseWriter, r *http.Request) (UpdateRequest, bool) {
	ifMatch, err := parseIfMatch(r.Header)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return UpdateRequest{}, false
	}
	author := r.Header.Get(HeaderAuthor)
	if len(author) > MaxAuthorBytes {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("%s longer than %d bytes", HeaderAuthor, MaxAuthorBytes))
		return UpdateRequest{}, false
	}
	comment := r.Header.Get(HeaderComment)
	if len(comment) > MaxCommentBytes {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("%s longer than %d bytes", HeaderComment, MaxCommentBytes))
		return UpdateRequest{}, false
	}
	return UpdateRequest{IfMatch: ifMatch, Author: author, Comment: comment}, true
}

// errEmptyBody is returned by decodeBody when the body holds no JSON value.
var errEmptyBody = errors.New("request body is empty")

// decodeBody reads the capped body and strictly decodes one JSON value into
// v: unknown fields, a wrong type, or anything but whitespace after the value
// is an error (rule 4). It returns errEmptyBody for an empty body. On any
// other failure it has written the 400/413 and returns a non-nil error with
// written == true.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) (written bool, err error) {
	body := http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return false, errEmptyBody
		}
		return true, bodyError(w, err, "invalid JSON body: "+err.Error())
	}
	// A second Decode must hit EOF: anything else is trailing data.
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("unexpected data after the JSON value")
		}
		return true, bodyError(w, err, "invalid JSON body: unexpected data after the JSON value")
	}
	return false, nil
}

// bodyError writes 413 for an over-limit body and 400 otherwise.
func bodyError(w http.ResponseWriter, err error, msg string) error {
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return err
	}
	writeError(w, http.StatusBadRequest, msg)
	return err
}

// readJSON decodes a required body into v, answering 400 (empty body
// included) or 413 itself. It reports whether the handler may continue.
func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	written, err := decodeBody(w, r, v)
	if err == nil {
		return true
	}
	if !written {
		writeError(w, http.StatusBadRequest, err.Error())
	}
	return false
}
