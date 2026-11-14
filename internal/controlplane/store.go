package controlplane

import (
	"context"
	"errors"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// Sentinel errors returned by every Store implementation. Callers test them
// with errors.Is; implementations wrap them with detail
// (fmt.Errorf("%w: version %d", ErrNotFound, v)).
var (
	// ErrNotFound: the requested version does not exist (Get, Rollback), or
	// a mutate function reported a missing route/upstream. HTTP 404.
	ErrNotFound = errors.New("controlplane: not found")

	// ErrVersionConflict: UpdateRequest.IfMatch was set and did not equal
	// the head version at the moment the write lock was held. Nothing was
	// written. HTTP 412. The message names both versions, e.g.
	// "controlplane: version conflict: if-match 41, head is 42".
	ErrVersionConflict = errors.New("controlplane: version conflict")
)

// Validation failures are not a separate sentinel: Update and Rollback
// return an error for which errors.Is(err, config.ErrInvalid) is true
// (config.Validate already wraps it in every per-problem error). HTTP 400.
// Every other error (database unreachable, context canceled, ...) is an
// internal error, HTTP 500.

// List page sizes. List clamps limit into [1, MaxListLimit], treating
// limit < 1 as DefaultListLimit.
const (
	DefaultListLimit = 50
	MaxListLimit     = 500
)

// VersionInfo is the metadata of one stored version (no body).
type VersionInfo struct {
	// Version is the config version, >= 1.
	Version int64
	// CreatedAt is the commit-side timestamp (Postgres now() of the write
	// transaction; time.Now for MemStore), in UTC.
	CreatedAt time.Time
	// Author is the free-text X-Tollgate-Author label of the write, "" if
	// none. It is not an authenticated identity (ADR 0008).
	Author string
	// Comment is the free-text X-Tollgate-Comment of the write, "" if none.
	Comment string
	// RolledBackFrom is the version whose body this version copies, or nil
	// if this version was not created by Rollback.
	RolledBackFrom *int64
}

// UpdateRequest carries the preconditions and audit metadata of a write.
// The zero value means "unconditional write, no author, no comment".
type UpdateRequest struct {
	// IfMatch, when non-nil, makes the write conditional: it succeeds only
	// if the head version (read while holding the write lock) equals
	// *IfMatch; otherwise ErrVersionConflict. *IfMatch == 0 means "only if
	// the store is empty". Nil means unconditional.
	IfMatch *int64
	// Author and Comment are stored verbatim with the new version. Length
	// limits are enforced by the HTTP layer, not the Store.
	Author  string
	Comment string
}

// Store persists versioned configs (ADR 0008). There are two
// implementations, PostgresStore and MemStore, and both must pass the same
// conformance test suite (storeConformance(t, newStore func(t) Store)), so
// the API can be tested against MemStore with confidence.
//
// Guarantees under concurrent use (all implementations):
//
//  1. Linearizable writes. Successful Update and Rollback calls take effect
//     one at a time, in increasing version order, each applied to the
//     committed result of all earlier ones. There are no lost updates: a
//     mutate function always sees the latest head, never a stale one.
//  2. Gapless versions. The committed versions are exactly 1..head. A call
//     that fails (validation, conflict, mutate error, canceled context)
//     consumes no version.
//  3. A failed call writes nothing, with one unavoidable exception: if the
//     context is canceled or the connection drops after COMMIT was sent,
//     the call returns an error even though the version may exist. Callers
//     that need exactly-once semantics use IfMatch, so a retry of an
//     already-committed write fails with ErrVersionConflict.
//  4. Reads (Head, Get, List) never block on, or block, writers, and never
//     observe an uncommitted version. After Update or Rollback returns
//     version v, every later Head returns a version >= v.
//  5. Fresh values. Every returned *config.Config is a new value owned by
//     the caller; the Store keeps no reference to it, and modifying it does
//     not affect the Store. (Once the caller passes it to Hub.Publish it
//     becomes shared and must be treated as immutable.)
//  6. Writers wait for each other (the write lock), bounded by ctx. A
//     canceled ctx returns ctx.Err() (possibly wrapped) without writing.
type Store interface {
	// Head returns the latest committed config. If no version exists yet it
	// returns an empty config (&config.Config{} with Version 0), not an
	// error: version 0 means "nothing has been written", and it is never
	// streamed to data planes.
	Head(ctx context.Context) (*config.Config, error)

	// Get returns the config stored as version, or an error wrapping
	// ErrNotFound if no such version exists (including version <= 0).
	// Stored bodies are decoded leniently and not re-validated (ADR 0008).
	Get(ctx context.Context, version int64) (*config.Config, error)

	// List returns version metadata in strictly descending version order,
	// at most limit entries (clamped, see DefaultListLimit), restricted to
	// versions < before when before > 0 (before <= 0 means "from head").
	// An empty store, or a before that is <= 1, returns an empty non-nil
	// slice and no error.
	List(ctx context.Context, limit int, before int64) ([]VersionInfo, error)

	// Update is the single write path for API edits. In ONE transaction it:
	//
	//   1. takes the write lock (Postgres: pg_advisory_xact_lock),
	//   2. reads head (version 0 / empty config if none),
	//   3. if req.IfMatch != nil and *req.IfMatch != head.Version, returns
	//      ErrVersionConflict,
	//   4. deep-copies head and calls mutate(copy) exactly once; a non-nil
	//      error from mutate aborts the write and is returned UNCHANGED, so
	//      mutate may return errors wrapping ErrNotFound or config.ErrInvalid
	//      to get 404 or 400 from the HTTP layer,
	//   5. sets copy.Version = head.Version + 1 (overwriting anything mutate
	//      set), then copy.Normalize() and copy.Validate(); a validation
	//      error (wrapping config.ErrInvalid) aborts the write,
	//   6. inserts the copy as a new row with req.Author, req.Comment and a
	//      NULL rolled_back_from, commits, and returns the copy.
	//
	// mutate runs while the write lock is held, so it must be fast, pure
	// CPU work. It must not do I/O and must not call the Store (a nested
	// write would wait for the lock its caller holds). It must not retain
	// the *Config it is given.
	//
	// The returned config is the one that was stored. Update does not
	// publish it; the caller passes it to Hub.Publish after Update returns.
	Update(ctx context.Context, req UpdateRequest, mutate func(*config.Config) error) (*config.Config, error)

	// Rollback creates a NEW version whose body is a copy of version to.
	// In one transaction, under the same write lock as Update, it:
	//
	//   1. reads head; checks req.IfMatch exactly like Update,
	//   2. resolves to == 0 to head.Version - 1,
	//   3. returns an error wrapping ErrNotFound if the target is < 1 or
	//      > head.Version (so rolling back with head <= 1 and to == 0 is
	//      ErrNotFound: version 0 is not a stored row); to == head is allowed
	//      and re-publishes head as a new version,
	//   4. copies the target body, sets Version = head.Version + 1, runs
	//      Normalize and Validate with the CURRENT rules (an old body that is
	//      invalid today returns an error wrapping config.ErrInvalid),
	//   5. inserts it with rolled_back_from = target, commits, and returns it.
	//
	// Callers validate to >= 0 before calling; a negative to is
	// ErrNotFound.
	Rollback(ctx context.Context, to int64, req UpdateRequest) (*config.Config, error)

	// Ping reports whether the store can serve requests: for Postgres, the
	// database answers and the schema is at the newest embedded migration
	// (ADR 0008, Migrations). MemStore always returns nil. Used by /readyz.
	Ping(ctx context.Context) error
}
