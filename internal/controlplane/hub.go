package controlplane

import (
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// This file is the Phase 4 CONTRACT for Hub and Nodes. The method bodies
// are placeholders: the builder replaces each panic with an implementation
// and adds the struct fields, keeping every signature and doc comment.

// Hub fans committed configs out to SSE stream handlers (ADR 0005,
// architecture §11 row 5). It is in-memory, so it serves one control-plane
// replica; multi-replica fan-out (Postgres LISTEN/NOTIFY) is future work.
//
// The Hub owns no goroutine. All methods are safe for concurrent use and
// never block on subscribers: they hold the hub mutex only for O(number of
// subscribers) non-blocking channel operations.
//
// Ordering rules:
//
//   - The hub remembers the highest-version config ever accepted ("max").
//     Publish ignores any config whose Version is <= max. This makes the hub
//     order-safe when two API requests commit v5 and v6 but call Publish
//     in the opposite order: v6 is delivered, the late v5 is dropped.
//   - Every subscriber has a capacity-1 slot. Because only strictly higher
//     versions are accepted, replacing the slot's content (drain if full,
//     then send, both under the hub mutex) always leaves the highest
//     version in the slot. A slow subscriber may skip versions but always
//     ends up with the newest one, which is enough because events are full
//     snapshots.
//
// Values are shared *config.Config pointers: the hub and every subscriber
// treat them as immutable.
//
// Planned fields (builder): mu sync.Mutex; max *config.Config;
// subs map[*subscription]struct{} (or map[chan *config.Config]struct{});
// closed bool.
type Hub struct{}

// NewHub returns an open Hub with no subscribers and no max.
func NewHub() *Hub {
	panic("controlplane: NewHub not implemented (Phase 4)")
}

// Publish offers cfg to every current subscriber.
//
// It is a no-op if cfg is nil, if the hub is closed, or if cfg.Version is
// <= the highest version previously accepted. Otherwise cfg becomes the new
// max (used to prime future subscribers) and replaces the content of every
// subscriber's slot. It never blocks.
//
// The caller must not modify cfg afterwards.
func (h *Hub) Publish(cfg *config.Config) {
	panic("controlplane: Hub.Publish not implemented (Phase 4)")
}

// Subscribe registers a subscriber and returns its receive channel and a
// cancel function.
//
//   - The channel has capacity 1 and holds at most one pending config, the
//     highest version published since the subscriber last received.
//   - If the hub has a max, the slot is primed with it before Subscribe
//     returns, so a subscriber never misses the newest config even if it
//     subscribed just after the publish. (The stream handler still reads
//     Store.Head after subscribing, because the hub knows nothing about
//     versions committed before this process started.)
//   - cancel removes the subscription and closes the channel. It is
//     idempotent and safe to call concurrently with Publish and Close, and
//     after Close.
//   - The channel is closed exactly once: by cancel or by Close, whichever
//     comes first. A closed channel means "stop streaming".
//   - After Close, Subscribe returns an already-closed channel and a no-op
//     cancel.
func (h *Hub) Subscribe() (ch <-chan *config.Config, cancel func()) {
	panic("controlplane: Hub.Subscribe not implemented (Phase 4)")
}

// Close closes every subscriber channel and makes later Publish calls
// no-ops and later Subscribe calls return closed channels. It is idempotent.
//
// cmd/controlplane registers it with http.Server.RegisterOnShutdown, so
// every SSE handler returns when Shutdown begins; otherwise Shutdown would
// wait forever for stream connections that never go idle.
func (h *Hub) Close() {
	panic("controlplane: Hub.Close not implemented (Phase 4)")
}

// DefaultNodeTTL is how long a disconnected data plane stays listed in
// GET /v1/dataplanes after it was last seen.
const DefaultNodeTTL = time.Hour

// NodeStatus is one data plane's entry in GET /v1/dataplanes. It is an
// informational, in-memory view (lost on control-plane restart); each
// gateway's tollgate_config_version metric stays the authoritative source
// for what it runs (ADR 0005).
type NodeStatus struct {
	// Node is the ID the gateway sent as ?node= or in the status path.
	Node string `json:"node"`
	// Connected is true while at least one stream connection with this
	// node ID is open (a reconnect can briefly overlap the old connection,
	// so Nodes counts connections).
	Connected bool `json:"connected"`
	// ConnectedAt is when the most recent stream connection for this node
	// was opened; nil if the node has only ever sent status reports.
	ConnectedAt *time.Time `json:"connected_at,omitempty"`
	// LastSeen is the last time there was evidence the node was alive:
	// connect, disconnect, a successful event or heartbeat write, or a
	// status report.
	LastSeen time.Time `json:"last_seen"`
	// LastOfferedVersion is the highest version sent to the node on a
	// stream (0 if none).
	LastOfferedVersion int64 `json:"last_offered_version"`
	// AppliedVersion is the version the node last said it is running:
	// set from since/Last-Event-ID at connect time and overwritten by
	// every status report.
	AppliedVersion int64 `json:"applied_version"`
	// RejectedVersion is the highest version the node reported it failed
	// to build (0 if none), from status reports.
	RejectedVersion int64 `json:"rejected_version"`
	// LastError is the build error of RejectedVersion, "" if none.
	LastError string `json:"last_error,omitempty"`
}

// Nodes is the in-memory registry of data planes behind GET /v1/dataplanes.
// All methods are safe for concurrent use and never block beyond a short
// mutex hold. Node IDs are validated by the HTTP layer before they reach
// Nodes.
//
// Eviction is lazy (no goroutine): List drops entries that are not
// connected and whose LastSeen is older than the TTL.
//
// Planned fields (builder): mu sync.Mutex; now func() time.Time;
// ttl time.Duration; nodes map[string]*nodeEntry (NodeStatus plus an open
// connection count).
type Nodes struct{}

// NewNodes returns an empty registry. A nil now uses time.Now; ttl <= 0
// uses DefaultNodeTTL.
func NewNodes(now func() time.Time, ttl time.Duration) *Nodes {
	panic("controlplane: NewNodes not implemented (Phase 4)")
}

// Connect records that a stream connection for node opened, with the
// version the node says it has applied (since, after Last-Event-ID
// override). It creates the entry if needed, increments the connection
// count, sets ConnectedAt and LastSeen to now, and sets AppliedVersion to
// since (overwrite, not max: a restarted gateway legitimately goes back to
// 0).
//
// The returned disconnect function decrements the connection count and
// updates LastSeen; it is idempotent. The stream handler defers it.
func (n *Nodes) Connect(node string, since int64) (disconnect func()) {
	panic("controlplane: Nodes.Connect not implemented (Phase 4)")
}

// Offered records that version was written to node's stream successfully:
// LastOfferedVersion = max(LastOfferedVersion, version), LastSeen = now.
// A call for an unknown node creates the entry.
func (n *Nodes) Offered(node string, version int64) {
	panic("controlplane: Nodes.Offered not implemented (Phase 4)")
}

// Seen updates LastSeen (after a successful heartbeat write). A call for
// an unknown node creates the entry.
func (n *Nodes) Seen(node string) {
	panic("controlplane: Nodes.Seen not implemented (Phase 4)")
}

// Report records a status report from POST /v1/dataplanes/{node}/status.
// It overwrites AppliedVersion, RejectedVersion and LastError with the
// report's Applied, LastRejected and Error (the latest report describes the
// node's whole current state) and sets LastSeen. It creates the entry if
// needed (a gateway may report an LKG-applied version before its stream
// connects), leaving ConnectedAt nil.
func (n *Nodes) Report(node string, r StatusReport) {
	panic("controlplane: Nodes.Report not implemented (Phase 4)")
}

// List first evicts expired entries (not connected and LastSeen older than
// the TTL), then returns a copy of every remaining entry sorted by Node.
// It returns an empty non-nil slice when there are none.
func (n *Nodes) List() []NodeStatus {
	panic("controlplane: Nodes.List not implemented (Phase 4)")
}
