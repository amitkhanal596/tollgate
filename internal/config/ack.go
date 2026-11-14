package config

// Acknowledger is an optional interface a ConfigSource may implement to learn
// whether each config it delivered was actually put into service
// (docs/architecture.md §5, ADR 0005 "Acknowledgement").
//
// The consumer (the proxy.Gateway reconcile loop) discovers it with a type
// assertion on the ConfigSource:
//
//	ack, _ := src.(config.Acknowledger) // nil if not implemented
//
// FileSource does not implement it. RemoteSource does: Applied is what
// advances its resume cursor and its on-disk last-known-good copy, so the LKG
// file only ever holds a config this gateway actually ran.
//
// Contract for the CALLER (the reconcile loop):
//
//  1. Exactly one call per received config. For every *Config received from
//     the Watch channel, the loop attempts a snapshot build and then calls
//     exactly one of Applied(cfg.Version) or Rejected(cfg.Version, err).
//     Configs that were replaced in the capacity-1 channel before the loop
//     received them are never seen and are never acknowledged.
//  2. Only config-triggered builds. Rebuilds caused by other inputs (Consul
//     discovery updates, health changes) are NOT acknowledged, even if they
//     fail.
//  3. Applied(v) is called only AFTER the new snapshot has been stored (the
//     atomic swap is done and new requests see version v). Rejected(v, err)
//     means the build failed and the previous snapshot is still live; err is
//     never nil.
//  4. Single goroutine, in order. Both methods are called only from the
//     reconcile goroutine, never concurrently with each other, and in the
//     order the configs were received (so with strictly increasing versions
//     for RemoteSource).
//  5. Calls may arrive after the source has shut down: the loop can be in
//     the middle of a build when ctx is canceled and the source closes its
//     channel, and it still reports the outcome.
//
// Contract for the IMPLEMENTATION (the source):
//
//  6. Must not block. Each method returns promptly: it may take a mutex for
//     a short critical section and do a non-blocking channel send, but it
//     must not perform network or disk I/O or wait on anything the reconcile
//     loop could be holding up. Slow work (writing the LKG file, reporting
//     status to the control plane) is handed to the source's own goroutine.
//  7. Goroutine safety. The methods run on the reconcile goroutine while the
//     source's own goroutines are running, so shared state must be
//     synchronised.
//  8. Safe after shutdown. Calls after ctx is canceled, or after the Watch
//     channel was closed, must not panic (for example by sending on a closed
//     channel) and must not start new goroutines. They may record state or
//     do nothing.
//  9. Unknown versions are tolerated. A version the source did not deliver,
//     or no longer remembers, is logged and ignored.
type Acknowledger interface {
	// Applied reports that the config with this version was built and is
	// now live.
	Applied(version int64)

	// Rejected reports that building the config with this version failed
	// with err (non-nil); the previously live config is still serving.
	Rejected(version int64, err error)
}
