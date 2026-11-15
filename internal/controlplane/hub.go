package controlplane

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// Lock ordering: Hub.mu and Nodes.mu are independent leaf locks. Neither type
// calls the other, and no method calls out (no I/O, no callbacks, no blocking
// channel operation) while holding its lock, so the two can never be held
// together and there is no ordering to get wrong.

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
// Why a mutex and not atomics: Publish must do "compare with max, raise
// max, replace every slot" as one step, Subscribe must do "register, prime
// with max" as one step (otherwise a publish between the two is lost), and
// close(ch) must never race a send on ch. No single atomic word covers that,
// and the path is not hot (one Publish per admin write, one Subscribe per
// stream connection), so a plain mutex is the clear choice.
type Hub struct {
	mu     sync.Mutex
	max    *config.Config             // highest version ever accepted; nil before the first
	subs   map[*subscription]struct{} // open subscriptions; nil after Close
	closed bool
}

// subscription is one subscriber's slot. ch is closed exactly once, by
// cancel or Close, and only while holding Hub.mu; done records that so the
// other path skips it.
type subscription struct {
	ch   chan *config.Config
	done bool // guarded by Hub.mu
}

// offer replaces the slot's content with cfg. The caller holds Hub.mu, so it
// is the only sender: after the non-blocking drain the buffer has room (a
// concurrent receiver can only make more room), so the send cannot block.
// The default branch is defence in depth, never expected to run.
func (s *subscription) offer(cfg *config.Config) {
	select {
	case <-s.ch:
	default:
	}
	select {
	case s.ch <- cfg:
	default:
	}
}

// NewHub returns an open Hub with no subscribers and no max.
func NewHub() *Hub {
	return &Hub{subs: make(map[*subscription]struct{})}
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
	if cfg == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	// Dropping <= max is what makes out-of-order publishes safe (§11 row 5):
	// every value ever placed in a slot is strictly newer than all earlier
	// ones, so "replace" is the same as "highest version wins".
	if h.closed || (h.max != nil && cfg.Version <= h.max.Version) {
		return
	}
	h.max = cfg
	for s := range h.subs {
		s.offer(cfg)
	}
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
	s := &subscription{ch: make(chan *config.Config, 1)}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		close(s.ch)
		return s.ch, func() {}
	}
	h.subs[s] = struct{}{}
	// Priming under the same lock as registration: a concurrent Publish
	// either ran before (its cfg is max, primed here) or runs after (it sees
	// s in subs). Nothing can fall between.
	if h.max != nil {
		s.offer(h.max)
	}
	return s.ch, func() { h.cancel(s) }
}

// cancel removes s and closes its channel unless Close already did.
func (h *Hub) cancel(s *subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s.done {
		return
	}
	s.done = true
	delete(h.subs, s)
	close(s.ch)
}

// Close closes every subscriber channel and makes later Publish calls
// no-ops and later Subscribe calls return closed channels. It is idempotent.
//
// cmd/controlplane registers it with http.Server.RegisterOnShutdown, so
// every SSE handler returns when Shutdown begins; otherwise Shutdown would
// wait forever for stream connections that never go idle.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for s := range h.subs {
		s.done = true
		close(s.ch)
	}
	h.subs = nil
	h.max = nil // nothing can read it any more; let the config be collected
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
// Why a mutex: every method is a read-modify-write of several fields of one
// entry (and List also deletes), the registry is touched a few times per
// stream connection and heartbeat, never per proxied request, so a single
// short-held mutex is simpler than per-field atomics and plenty fast.
type Nodes struct {
	mu    sync.Mutex
	now   func() time.Time
	ttl   time.Duration
	nodes map[string]*nodeEntry
}

// nodeEntry is a NodeStatus plus the number of open stream connections.
// status.Connected is derived from conns when copied out.
type nodeEntry struct {
	status NodeStatus
	conns  int
}

// NewNodes returns an empty registry. A nil now uses time.Now; ttl <= 0
// uses DefaultNodeTTL.
func NewNodes(now func() time.Time, ttl time.Duration) *Nodes {
	if now == nil {
		now = time.Now
	}
	if ttl <= 0 {
		ttl = DefaultNodeTTL
	}
	return &Nodes{now: now, ttl: ttl, nodes: make(map[string]*nodeEntry)}
}

// entry returns node's entry, creating it if needed. Caller holds n.mu.
func (n *Nodes) entry(node string) *nodeEntry {
	e, ok := n.nodes[node]
	if !ok {
		e = &nodeEntry{status: NodeStatus{Node: node}}
		n.nodes[node] = e
	}
	return e
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
	n.mu.Lock()
	now := n.now()
	e := n.entry(node)
	e.conns++
	// A fresh pointer per connection: List hands out copies, so the value
	// behind an old pointer is never mutated.
	connectedAt := now
	e.status.ConnectedAt = &connectedAt
	e.status.LastSeen = now
	e.status.AppliedVersion = since
	n.mu.Unlock()

	var once sync.Once
	return func() {
		// Once makes a double disconnect harmless; holding e directly is
		// safe because List never evicts an entry while conns > 0.
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			e.conns--
			e.status.LastSeen = n.now()
		})
	}
}

// Offered records that version was written to node's stream successfully:
// LastOfferedVersion = max(LastOfferedVersion, version), LastSeen = now.
// A call for an unknown node creates the entry.
func (n *Nodes) Offered(node string, version int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	e := n.entry(node)
	e.status.LastOfferedVersion = max(e.status.LastOfferedVersion, version)
	e.status.LastSeen = n.now()
}

// Seen updates LastSeen (after a successful heartbeat write). A call for
// an unknown node creates the entry.
func (n *Nodes) Seen(node string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.entry(node).status.LastSeen = n.now()
}

// Report records a status report from POST /v1/dataplanes/{node}/status.
// It overwrites AppliedVersion, RejectedVersion and LastError with the
// report's Applied, LastRejected and Error (the latest report describes the
// node's whole current state) and sets LastSeen. It creates the entry if
// needed (a gateway may report an LKG-applied version before its stream
// connects), leaving ConnectedAt nil.
func (n *Nodes) Report(node string, r StatusReport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	e := n.entry(node)
	e.status.AppliedVersion = r.Applied
	e.status.RejectedVersion = r.LastRejected
	e.status.LastError = r.Error
	e.status.LastSeen = n.now()
}

// List first evicts expired entries (not connected and LastSeen older than
// the TTL), then returns a copy of every remaining entry sorted by Node.
// It returns an empty non-nil slice when there are none.
func (n *Nodes) List() []NodeStatus {
	n.mu.Lock()
	now := n.now()
	out := make([]NodeStatus, 0, len(n.nodes))
	for id, e := range n.nodes {
		if e.conns == 0 && now.Sub(e.status.LastSeen) > n.ttl {
			delete(n.nodes, id) // deleting during range is allowed in Go
			continue
		}
		ns := e.status
		ns.Connected = e.conns > 0
		if ns.ConnectedAt != nil {
			// Deep copy so a caller writing through the pointer cannot
			// reach the registry.
			t := *ns.ConnectedAt
			ns.ConnectedAt = &t
		}
		out = append(out, ns)
	}
	n.mu.Unlock()

	// Sorting happens outside the lock: out is private to this call.
	slices.SortFunc(out, func(a, b NodeStatus) int { return strings.Compare(a.Node, b.Node) })
	return out
}
