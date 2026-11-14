// Package balancer chooses which backend instance of an upstream serves a
// request.
//
// It defines two things:
//
//   - Backend: one upstream instance (a base URL) plus its mutable runtime
//     state (health, passive-ejection deadline, in-flight count). Backend is
//     a concrete struct with atomic fields, not an interface; see ADR 0002
//     for why.
//   - Balancer: the selection policy. Round-robin and least-connections are
//     both implemented and chosen per upstream (ADR 0003).
//
// Dependency rule: this package imports only the standard library and no
// other internal package (docs/architecture.md).
//
// Planned constructors (Phase 2). Both take an injected clock so tests can
// control passive-ejection expiry without sleeping:
//
//	func NewRoundRobin(backends []*Backend, now func() time.Time) Balancer
//	func NewLeastConn(backends []*Backend, now func() time.Time) Balancer
//
// The backends slice passed to a constructor is owned by the Balancer and
// must not be modified afterwards.
package balancer

import (
	"errors"
	"net/url"
	"sync/atomic"
)

// ErrNoHealthyBackends is returned by Balancer.Next when the backend set is
// empty or every backend is currently unavailable (actively marked down or
// passively ejected). The proxy maps it to 503 Service Unavailable.
var ErrNoHealthyBackends = errors.New("balancer: no healthy backends")

// Backend is one upstream instance plus its runtime state.
//
// A *Backend is shared by the balancer (reads health, updates in-flight),
// the active health checker (writes activeDown), the passive health tracker
// (writes ejection state) and the proxy (forwards to URL). All mutable
// fields are atomics so none of those readers or writers take a lock.
//
// Identity and lifetime: Backends are created and handed out by a
// long-lived Registry (Phase 2) keyed by ID. When a new config snapshot is
// built, the builder asks the Registry for each backend by ID and gets the
// SAME pointer back if that backend already existed. That is how health,
// ejection and in-flight counts survive a config swap (ADR 0002). Backends
// that disappear from the config are dropped from the Registry; requests
// still in flight on an old snapshot keep their pointer until they finish,
// after which the garbage collector reclaims it.
//
// Never copy a Backend by value (it contains atomics; go vet's copylocks
// check flags this). Always use *Backend.
//
// Method set to be implemented in Phase 2 (signatures are the contract the
// tests will be written against):
//
//	func NewBackend(id string, u *url.URL) *Backend
//	// Available reports !activeDown && now >= ejectedUntil. Ejection expires
//	// lazily here, so no timer goroutine is needed to re-admit a backend.
//	func (b *Backend) Available(now time.Time) bool
//	func (b *Backend) InFlight() int64
//	// SetActiveHealthy is called by the active checker when the rise/fall
//	// threshold is crossed. It reports whether the state changed.
//	func (b *Backend) SetActiveHealthy(up bool) (changed bool)
//	func (b *Backend) ActiveHealthy() bool
//	// RecordFailure increments the consecutive-failure counter and returns
//	// the new value; RecordSuccess resets it to zero.
//	func (b *Backend) RecordFailure() int32
//	func (b *Backend) RecordSuccess()
//	func (b *Backend) EjectUntil(t time.Time)
//	func (b *Backend) EjectedUntil() time.Time // zero Time if never ejected
type Backend struct {
	// ID uniquely identifies the backend process-wide. Format:
	// "<upstream name>|<base URL>", e.g. "users|http://10.0.0.7:8080".
	// The same address used by two upstreams is two Backends, because the
	// upstreams may have different health-check settings.
	// Immutable after construction.
	ID string

	// URL is the base URL requests are forwarded to (scheme + host:port).
	// Immutable after construction; never mutate the pointed-to value.
	URL *url.URL

	// activeDown is true after the active checker has seen
	// UnhealthyThreshold consecutive probe failures, and false again after
	// HealthyThreshold consecutive successes. It is "down" rather than "up"
	// so the zero value means healthy: a newly discovered backend takes
	// traffic immediately instead of waiting one probe interval.
	activeDown atomic.Bool

	// ejectedUntilUnixNano is the passive-ejection deadline as Unix
	// nanoseconds; 0 means never ejected. Stored as an int64 so it can be
	// read and written atomically.
	ejectedUntilUnixNano atomic.Int64

	// consecutiveFailures counts passive failures (transport errors and
	// 5xx) since the last successful response. Only a success resets it.
	consecutiveFailures atomic.Int32

	// inFlight is the number of requests currently proxied to this backend.
	// Incremented by Balancer.Next, decremented by Balancer.Done.
	inFlight atomic.Int64
}

// Balancer chooses a backend for each request from a fixed set.
//
// A Balancer is built per upstream every time a config snapshot is built
// and is never mutated afterwards. Changing the backend set (config push or
// Consul update) produces a new Balancer inside a new snapshot. All mutable
// state a Balancer depends on lives in the shared *Backend records, so a
// Balancer itself holds at most a cursor (round-robin).
//
// Implementations must be safe for concurrent use, must not block, and must
// not do I/O.
type Balancer interface {
	// Next returns an available backend and increments its in-flight count.
	// "Available" means Backend.Available(now) is true, with now taken from
	// the clock injected at construction. If no backend is available, Next
	// returns (nil, ErrNoHealthyBackends) and increments nothing.
	//
	// On success the caller MUST call Done(b) exactly once when the attempt
	// to b has finished (response fully copied, or the attempt failed),
	// normally with defer. A missing Done permanently inflates b's in-flight
	// count and skews least-connections.
	Next() (*Backend, error)

	// Done reports that a request previously given b by Next has finished
	// and decrements b's in-flight count. Because the count lives on the
	// shared *Backend, Done stays correct even if a newer snapshot has
	// replaced this Balancer or removed b in the meantime. Calling Done more
	// often than Next returned b is a bug.
	Done(b *Backend)
}
