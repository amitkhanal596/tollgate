package controlplane

import (
	"container/list"
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
// architecture §11 row 5). It is in-memory, so it only sees writes made
// through this replica's handlers; cmd/controlplane's head poller publishes
// versions committed elsewhere (another replica) within one poll interval.
// Push-based fan-out (Postgres LISTEN/NOTIFY) is future work.
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

// Max returns the highest version published to the hub, 0 if none (or after
// Close). cmd/controlplane's head poller compares Store.Head against it, so
// it only publishes versions this hub has not seen. It is called once per
// poll interval, not on a hot path, so it shares the hub mutex.
func (h *Hub) Max() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.max == nil {
		return 0
	}
	return h.max.Version
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

// DefaultMaxNodes caps the Nodes registry used by NewNodes. Node IDs come
// from (token-holding) clients, so without a cap a buggy or compromised
// data plane could grow the map, and GET /v1/dataplanes, without bound.
// 4096 is far above any realistic fleet behind one control plane.
const DefaultMaxNodes = 4096

// Nodes is the in-memory registry of data planes behind GET /v1/dataplanes.
// All methods are safe for concurrent use and never block beyond a short
// mutex hold. Node IDs are validated by the HTTP layer before they reach
// Nodes.
//
// Expiry is lazy (no goroutine): List drops entries that are not connected
// and whose LastSeen is older than the TTL.
//
// The registry holds at most max entries. Admitting a new node ID at the cap
// evicts the least-recently-seen DISCONNECTED entry; if every entry is
// connected, the new node is refused (TryConnect returns ok == false, the
// stream handler answers 503, and Report/Offered/Seen drop the update).
// Connected entries are never evicted: they are live gateways, and their
// disconnect closures still point at them.
//
// Eviction is O(1): disconnected entries are kept on a list ordered by when
// they were last seen (each touch moves the entry to the back), so the
// victim is always the front element. No method scans the registry except
// List, which is O(n) anyway because it copies everything out.
//
// Why a mutex: every method is a read-modify-write of several fields of one
// entry plus the map and the idle list together, the registry is touched a
// few times per stream connection and heartbeat, never per proxied request,
// so a single short-held mutex is simpler than per-field atomics and plenty
// fast.
type Nodes struct {
	mu    sync.Mutex
	now   func() time.Time
	ttl   time.Duration
	max   int
	nodes map[string]*nodeEntry
	// idle holds every entry with conns == 0, least recently seen at the
	// front. Values are *nodeEntry.
	idle *list.List
}

// nodeEntry is a NodeStatus plus the number of open stream connections.
// status.Connected is derived from conns when copied out.
type nodeEntry struct {
	status NodeStatus
	conns  int
	// idleElem is the entry's element in Nodes.idle; non-nil exactly when
	// conns == 0.
	idleElem *list.Element
}

// NewNodes returns an empty registry capped at DefaultMaxNodes. A nil now
// uses time.Now; ttl <= 0 uses DefaultNodeTTL.
func NewNodes(now func() time.Time, ttl time.Duration) *Nodes {
	return NewNodesWithLimit(now, ttl, DefaultMaxNodes)
}

// NewNodesWithLimit is NewNodes with an explicit registry cap; max <= 0 uses
// DefaultMaxNodes.
func NewNodesWithLimit(now func() time.Time, ttl time.Duration, max int) *Nodes {
	if now == nil {
		now = time.Now
	}
	if ttl <= 0 {
		ttl = DefaultNodeTTL
	}
	if max <= 0 {
		max = DefaultMaxNodes
	}
	return &Nodes{now: now, ttl: ttl, max: max, nodes: make(map[string]*nodeEntry), idle: list.New()}
}

// entry returns node's entry, admitting it if it is new. At the cap it
// evicts the least-recently-seen disconnected entry first; if there is none
// (every entry is connected) it returns false and changes nothing. Caller
// holds n.mu.
func (n *Nodes) entry(node string) (*nodeEntry, bool) {
	if e, ok := n.nodes[node]; ok {
		return e, true
	}
	if len(n.nodes) >= n.max {
		victim := n.idle.Front()
		if victim == nil {
			return nil, false
		}
		n.remove(victim.Value.(*nodeEntry))
	}
	e := &nodeEntry{status: NodeStatus{Node: node}}
	e.idleElem = n.idle.PushBack(e) // a new entry has no connection yet
	n.nodes[node] = e
	return e, true
}

// remove deletes a disconnected entry from the map and the idle list.
// Caller holds n.mu.
func (n *Nodes) remove(e *nodeEntry) {
	n.idle.Remove(e.idleElem)
	e.idleElem = nil
	delete(n.nodes, e.status.Node)
}

// touch sets LastSeen to now and, for a disconnected entry, moves it to the
// back of the idle list, keeping the list in last-seen order. Caller holds
// n.mu.
func (n *Nodes) touch(e *nodeEntry, now time.Time) {
	e.status.LastSeen = now
	if e.idleElem != nil {
		n.idle.MoveToBack(e.idleElem)
	}
}

// Connect is TryConnect for callers that cannot act on a refusal: when the
// registry is full of connected nodes the connection is simply not recorded
// and the returned disconnect is a no-op. The stream handler uses
// TryConnect so it can answer 503 instead.
func (n *Nodes) Connect(node string, since int64) (disconnect func()) {
	if d, ok := n.TryConnect(node, since); ok {
		return d
	}
	return func() {}
}

// TryConnect records that a stream connection for node opened, with the
// version the node says it has applied (since, after Last-Event-ID
// override). It admits the entry if needed (see Nodes for the cap),
// increments the connection count, sets ConnectedAt and LastSeen to now, and
// sets AppliedVersion to since (overwrite, not max: a restarted gateway
// legitimately goes back to 0).
//
// ok is false, disconnect nil and nothing changed when node is new, the
// registry is at its cap and every entry is connected.
//
// The returned disconnect function decrements the connection count and
// updates LastSeen; it is idempotent. The stream handler defers it.
func (n *Nodes) TryConnect(node string, since int64) (disconnect func(), ok bool) {
	n.mu.Lock()
	e, ok := n.entry(node)
	if !ok {
		n.mu.Unlock()
		return nil, false
	}
	now := n.now()
	e.conns++
	if e.idleElem != nil { // connected entries are never eviction candidates
		n.idle.Remove(e.idleElem)
		e.idleElem = nil
	}
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
		// safe because nothing evicts or expires an entry while conns > 0.
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			e.conns--
			e.status.LastSeen = n.now()
			if e.conns == 0 {
				e.idleElem = n.idle.PushBack(e) // just seen: the newest idle entry
			}
		})
	}, true
}

// Offered records that version was written to node's stream successfully:
// LastOfferedVersion = max(LastOfferedVersion, version), LastSeen = now.
// A call for an unknown node creates the entry if it can be admitted, and is
// dropped otherwise.
func (n *Nodes) Offered(node string, version int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	e, ok := n.entry(node)
	if !ok {
		return
	}
	e.status.LastOfferedVersion = max(e.status.LastOfferedVersion, version)
	n.touch(e, n.now())
}

// Seen updates LastSeen (after a successful heartbeat write). A call for
// an unknown node creates the entry if it can be admitted, and is dropped
// otherwise.
func (n *Nodes) Seen(node string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e, ok := n.entry(node); ok {
		n.touch(e, n.now())
	}
}

// Report records a status report from POST /v1/dataplanes/{node}/status.
// It overwrites AppliedVersion, RejectedVersion and LastError with the
// report's Applied, LastRejected and Error (the latest report describes the
// node's whole current state) and sets LastSeen. It creates the entry if
// needed (a gateway may report an LKG-applied version before its stream
// connects), leaving ConnectedAt nil. A report for a new node that cannot
// be admitted (registry full of connected nodes) is dropped silently: the
// status endpoint is best effort and the gateway's own metrics stay
// authoritative.
func (n *Nodes) Report(node string, r StatusReport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	e, ok := n.entry(node)
	if !ok {
		return
	}
	e.status.AppliedVersion = r.Applied
	e.status.RejectedVersion = r.LastRejected
	e.status.LastError = r.Error
	n.touch(e, n.now())
}

// List first evicts expired entries (not connected and LastSeen older than
// the TTL), then returns a copy of every remaining entry sorted by Node.
// It returns an empty non-nil slice when there are none.
func (n *Nodes) List() []NodeStatus {
	n.mu.Lock()
	now := n.now()
	out := make([]NodeStatus, 0, len(n.nodes))
	for _, e := range n.nodes {
		if e.conns == 0 && now.Sub(e.status.LastSeen) > n.ttl {
			n.remove(e) // deleting during range is allowed in Go
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
