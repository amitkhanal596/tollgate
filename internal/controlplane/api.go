package controlplane

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// This file holds the wire-level CONTRACT of the admin API and the config
// stream: names, limits, the handler Options and the JSON DTOs. The full
// HTTP contract (every endpoint, status code and header) is in
// docs/architecture.md §6; the handlers live in handler.go and stream.go.

// Environment variables read by cmd/controlplane. cmd/gateway also reads
// EnvDataPlaneToken when -control-plane is set.
const (
	// EnvAdminToken is the bearer token for every /v1/* endpoint except the
	// two data-plane endpoints, reads included.
	EnvAdminToken = "TOLLGATE_ADMIN_TOKEN"
	// EnvDataPlaneToken is the bearer token accepted ONLY on
	// GET /v1/config/stream and POST /v1/dataplanes/{node}/status.
	EnvDataPlaneToken = "TOLLGATE_DATAPLANE_TOKEN"
	// EnvDatabaseURL is the Postgres connection string (pgx format).
	EnvDatabaseURL = "TOLLGATE_DATABASE_URL"
)

// MinTokenBytes is the minimum length of each token. NewHandler (and
// cmd/controlplane at startup) refuse tokens that are shorter, or two
// tokens that are equal.
const MinTokenBytes = 32

// Header names used by the API. Standard headers are spelled in their
// canonical form so they can be compared with http.Header keys directly.
const (
	HeaderAuthorization   = "Authorization"
	HeaderWWWAuthenticate = "WWW-Authenticate"
	// HeaderETag carries the config version on every successful response
	// that is about config, as a quoted strong entity tag: ETag: "42".
	HeaderETag = "ETag"
	// HeaderIfMatch makes a write conditional on the head version. Accepted
	// forms: "42" (quoted) or 42 (bare), non-negative decimal. Anything else
	// (*, W/"42", lists) is a 400.
	HeaderIfMatch = "If-Match"
	// HeaderLastEventID is the standard SSE resume header. When present on
	// GET /v1/config/stream it overrides ?since.
	HeaderLastEventID = "Last-Event-ID"
	// HeaderAuthor and HeaderComment are optional audit labels stored with
	// every write (VersionInfo.Author / Comment).
	HeaderAuthor  = "X-Tollgate-Author"
	HeaderComment = "X-Tollgate-Comment"
	// HeaderAccelBuffering disables response buffering in nginx-style
	// proxies on the stream ("no").
	HeaderAccelBuffering = "X-Accel-Buffering"
)

// Content types.
const (
	ContentTypeJSON        = "application/json"
	ContentTypeEventStream = "text/event-stream"
)

// Query parameter names.
const (
	QuerySince  = "since"  // stream: applied version, default 0
	QueryNode   = "node"   // stream: required data-plane ID
	QueryLimit  = "limit"  // version list page size
	QueryBefore = "before" // version list cursor: versions < before
)

// SSE framing. One config event on the wire is exactly:
//
//	"id: " + decimal(version) + "\n" +
//	"event: " + EventConfig + "\n" +
//	"data: " + json.Marshal(cfg) + "\n" +
//	"\n"
//
// json.Marshal never emits a newline, so data is always one line.
const (
	// EventConfig is the only event type the control plane sends.
	EventConfig = "config"
	// HeartbeatComment is written verbatim every Options.Heartbeat. It is an
	// SSE comment (dispatches nothing) that keeps idle-timeout middleboxes
	// and the client's idle detector satisfied.
	HeartbeatComment = ": heartbeat\n\n"
)

// Limits and defaults.
const (
	// MaxBodyBytes caps every request body (http.MaxBytesReader); a larger
	// body is a 413.
	MaxBodyBytes = 1 << 20
	// MaxAuthorBytes and MaxCommentBytes cap the audit headers; longer is a
	// 400.
	MaxAuthorBytes  = 128
	MaxCommentBytes = 1024
	// MaxReportErrorBytes caps StatusReport.Error; longer is a 400. The
	// gateway truncates before sending.
	MaxReportErrorBytes = 1024
	// MaxNodeIDBytes caps node IDs. A valid node ID is 1..MaxNodeIDBytes
	// bytes of [A-Za-z0-9._-] whose first byte is a letter or digit
	// (Kubernetes pod names and host names fit).
	MaxNodeIDBytes = 128
	// DefaultHeartbeat is the stream heartbeat interval. The gateway's idle
	// timeout (45 s) is three heartbeats.
	DefaultHeartbeat = 15 * time.Second
	// DefaultStreamWriteTimeout bounds each write+flush on a stream (via
	// http.ResponseController.SetWriteDeadline), so a peer that stopped
	// reading cannot pin a handler goroutine forever.
	DefaultStreamWriteTimeout = 10 * time.Second
	// DefaultStoreTimeout bounds the Store calls of one non-stream request.
	DefaultStoreTimeout = 10 * time.Second
	// ReadyzTimeout bounds Store.Ping in /readyz.
	ReadyzTimeout = 2 * time.Second
)

// Options configures the handler returned by
//
//	func NewHandler(opts Options) (http.Handler, error)
//
// (handler.go, Phase 4 builder). NewHandler returns an error if Store, Hub
// or Nodes is nil, if either token is shorter than MinTokenBytes, or if the
// two tokens are equal. It stores only the SHA-256 digests of the tokens.
// Zero durations take the Default* values above.
type Options struct {
	Store Store
	Hub   *Hub
	Nodes *Nodes

	// AdminToken and DataPlaneToken are the raw bearer tokens.
	AdminToken     string
	DataPlaneToken string

	// Heartbeat is the stream heartbeat interval (tests use milliseconds).
	Heartbeat time.Duration
	// StreamWriteTimeout bounds each stream write+flush.
	StreamWriteTimeout time.Duration
	// StoreTimeout bounds Store calls in non-stream handlers.
	StoreTimeout time.Duration

	// Metrics, if non-nil, is mounted at GET /metrics (cmd/controlplane
	// passes promhttp.HandlerFor(its own registry)). Nil: /metrics is 404.
	Metrics http.Handler

	// Logger receives structured logs. Nil discards them.
	Logger *slog.Logger
}

// ErrorBody is the body of every non-2xx JSON response from /v1/*:
// {"error": "..."}. 5xx bodies never contain internal details; they say
// "internal error" and the detail is logged.
type ErrorBody struct {
	Error string `json:"error"`
}

// RollbackRequest is the optional body of POST /v1/config/rollback. An
// empty body, {} and {"to_version": 0} all mean "head - 1".
type RollbackRequest struct {
	ToVersion int64 `json:"to_version"`
}

// StatusReport is the body of POST /v1/dataplanes/{node}/status, sent by
// config.RemoteSource after every Applied/Rejected (best effort). It always
// describes the gateway process's whole current state, so the latest report
// wins. config.RemoteSource has its own unexported copy of this shape
// (config may not import controlplane); the JSON tags are the contract.
type StatusReport struct {
	// Applied is the version currently live on the gateway, 0 if none.
	Applied int64 `json:"applied"`
	// LastRejected is the highest version this gateway process failed to
	// build, 0 if none. It is not cleared by a later Applied.
	LastRejected int64 `json:"last_rejected"`
	// Error is the build error of LastRejected, "" if none.
	Error string `json:"error,omitempty"`
}

// VersionItem is one entry of GET /v1/config/versions (the wire form of
// VersionInfo).
type VersionItem struct {
	Version        int64     `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	Author         string    `json:"author"`
	Comment        string    `json:"comment"`
	RolledBackFrom *int64    `json:"rolled_back_from,omitempty"`
}

// VersionList is the body of GET /v1/config/versions.
type VersionList struct {
	// Versions are in descending version order; never null.
	Versions []VersionItem `json:"versions"`
	// NextBefore, when non-zero, is the value to pass as ?before= to get
	// the next (older) page: the smallest version in this page, set only
	// if that version is > 1. Omitted on the last page.
	NextBefore int64 `json:"next_before,omitempty"`
}

// RouteList is the body of GET /v1/routes: routes sorted by name; never
// null.
type RouteList struct {
	Routes []config.Route `json:"routes"`
}

// UpstreamList is the body of GET /v1/upstreams: upstreams sorted by name;
// never null.
type UpstreamList struct {
	Upstreams []config.Upstream `json:"upstreams"`
}

// DataPlaneList is the body of GET /v1/dataplanes (Nodes.List); never
// null.
type DataPlaneList struct {
	DataPlanes []NodeStatus `json:"dataplanes"`
}
