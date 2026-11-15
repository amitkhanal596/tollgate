package config

import (
	"context"
	"errors"
)

// ErrAlreadyWatching is returned by Watch when it is called a second time on
// the same source. Sources are single-subscriber.
var ErrAlreadyWatching = errors.New("config: source is already being watched")

// ConfigSource delivers successive validated configurations to the gateway.
//
// Implementations (planned):
//   - FileSource (Phase 1): reads a YAML file and re-reads it when its
//     content hash changes (polled every couple of seconds; fsnotify is not
//     an allowed dependency, and hashing also handles Kubernetes ConfigMap
//     symlink swaps where mtime is unreliable).
//   - RemoteSource (Phase 4): subscribes to the control plane's SSE stream
//     (ADR 0005) and keeps a copy of the last APPLIED config on local disk
//     (it implements Acknowledger, see ack.go).
//   - Static/fake sources in tests.
//
// Semantics of Watch, which every implementation must follow:
//
//  1. Single subscriber. Watch may be called at most once per source; a
//     second call returns (nil, ErrAlreadyWatching).
//
//  2. Synchronous error only for "cannot start at all". Watch returns a
//     non-nil error only if the source can never produce a config, e.g.
//     the YAML file is missing or invalid at startup (fail fast). Transient
//     problems (control plane unreachable, file temporarily unreadable) are
//     NOT returned: the source logs them and keeps retrying.
//
//  3. Initial config. The first value sent is the current config as soon as
//     the source has one. FileSource sends it immediately. RemoteSource
//     sends the on-disk copy of the last APPLIED config if present (written
//     only after the gateway acknowledged it with Applied), then the control
//     plane's latest version once connected. Until the first value arrives
//     the gateway is not ready (503, /readyz fails).
//
//  4. Only valid configs. Every value has been Normalized and Validated. An
//     invalid update is logged and skipped; nothing is sent, so the gateway
//     keeps serving its last-known-good config.
//
//  5. Ordering and coalescing ("latest wins"). Values are sent in the order
//     the source observed them. The channel has capacity 1: if the consumer
//     has not yet received the previous value, the source replaces it with
//     the newer one. Intermediate configs may therefore be skipped, but the
//     latest one is never lost. RemoteSource additionally guarantees
//     strictly increasing Version and drops anything <= the last version it
//     delivered. The consumer applies every value it receives and does not
//     compare versions itself.
//
//  6. Ownership. Each *Config sent is immutable: the source never touches it
//     after sending and the consumer must not modify it.
//
//  7. Shutdown. When ctx is canceled the source stops all of its goroutines,
//     then closes the channel. The source (the sender) is the only party
//     that closes the channel, and it closes it only after ctx is done; a
//     closed channel therefore always means "shut down", never "error".
type ConfigSource interface {
	Watch(ctx context.Context) (<-chan *Config, error)
}
