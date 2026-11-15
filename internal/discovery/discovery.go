// Package discovery turns a service name into a live set of healthy
// instances (ADR 0007). The Consul implementation (Phase 5) uses blocking
// queries against /v1/health/service/<name>?passing=true.
//
// Dependency rule: this package imports only the standard library and the
// Consul API client, and no other internal package (docs/architecture.md).
// The proxy maps config.ConsulService to a Query and Endpoints to backend
// URLs.
//
// Constructor:
//
//	func NewConsul(client *consulapi.Client, opts ConsulOptions) *Consul // implements Resolver
package discovery

import "context"

// Query identifies the instances to watch.
type Query struct {
	// Service is the Consul service name.
	Service string
	// Tag, if non-empty, keeps only instances carrying this tag.
	Tag string
}

// Endpoint is one healthy service instance.
type Endpoint struct {
	// ID is stable for the life of the instance ("<node>/<service ID>").
	ID string
	// Address is "host:port". The service address is used, falling back to
	// the node address when the service registered without one.
	Address string
}

// Resolver watches the healthy instances of services.
//
// Watch semantics mirror config.ConfigSource:
//
//  1. Watch may be called many times, once per Query; each call has its own
//     goroutine and channel, and is stopped by canceling its ctx. The
//     gateway cancels a watch when no upstream references the service.
//  2. The error return is only for invalid input (empty Service). Consul
//     being unreachable is not an error: the watcher retries with
//     exponential backoff and sends nothing, so the consumer keeps the last
//     known endpoint set. It never sends an empty set because of an error.
//  3. The first value is the result of the first successful query. It may
//     be an empty slice, which means Consul reports zero passing instances.
//  4. A value is sent only when the set actually changed. Endpoints in a
//     value are sorted by ID so sets can be compared cheaply.
//  5. The channel has capacity 1 and is "latest wins": an unreceived older
//     set is replaced by the newer one.
//  6. Each slice sent is immutable: neither side modifies it after sending.
//  7. When ctx is canceled the watcher goroutine exits and closes the
//     channel. Only the watcher closes it.
type Resolver interface {
	Watch(ctx context.Context, q Query) (<-chan []Endpoint, error)
}
