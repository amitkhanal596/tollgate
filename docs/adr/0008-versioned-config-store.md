# ADR 0008: Store configs as full-document snapshots in one append-only Postgres table

Status: Accepted (Phase 4 design). Builds on ADR 0001 (split) and ADR 0005 (SSE push, rollback as a new version).

## Context

The control plane is the single writer of gateway configuration. It must:

- accept small edits (one route, one upstream) and whole-document replacements through the admin API (architecture.md §6),
- validate the **whole** resulting document with the same `config.Validate` the data plane uses, because most rules are cross-object (a route's upstream must exist, `(host, path_prefix)` pairs must be unique, a route using `jwt` needs `auth.jwt`),
- give every accepted change a version number that is **gapless and strictly increasing**, because the data plane's only ordering rule is "apply if newer" (ADR 0005),
- serve "the head", "version N" and "the history" cheaply, and roll back to an earlier version,
- stay correct when two admins (or a CI job and an admin) write at the same time.

Write volume is tiny (a few writes per day, maybe bursts of tens during a migration). Read volume is also small: each gateway reads head once per (re)connect. Configs are small (kilobytes to low megabytes).

Constraints: Postgres via pgx is the only database dependency, and there is no migration library on the allowed-dependency list.

## Options considered

### A. Data model

1. **Full-document snapshot per version, one append-only table** (`config_versions(version, body jsonb, ...)`). (chosen)
   Every write reads head, applies the change in Go, validates the full document and inserts the whole document as a new row.
   - Good: one row is exactly what the SSE stream sends, what `GET /v1/config/versions/{n}` returns and what rollback copies. No reassembly code, so no reassembly bugs.
   - Good: validation is "decode, `Validate`", the same code path as the data plane.
   - Good: rollback is "copy row N into a new row". The history is an audit log by construction (rows are never updated).
   - Good: adding a config field needs **no** SQL migration; the field just appears in the JSON.
   - Cost: storage grows by one full document per write, even for a one-line change. At 10 KB per version and 10 000 versions that is about 100 MB, which is fine for v1 (see Retention).
   - Cost: no relational queries over routes ("which versions referenced upstream X?") without JSONB operators. Postgres has them (`body->'routes'`), which is good enough for ad-hoc debugging.
2. **Normalised tables (`routes`, `upstreams`, `api_keys`, ...) with a version column** (or `valid_from`/`valid_to` temporal rows).
   - Good: small writes; per-object queries and constraints (foreign keys from routes to upstreams) in SQL.
   - Cost: producing version N means joining several tables "as of N" and reassembling a `config.Config`, on every stream connect. That is the code the full-snapshot model does not need.
   - Cost: the SQL schema duplicates the Go struct. Every new config field needs a migration, and the two can drift.
   - Cost: cross-object validation still needs the whole document, so we would assemble it in Go anyway, then also have to keep SQL constraints consistent with `Validate`.
   - Cost: rollback copies rows across several tables inside one transaction.
3. **Event-sourced diffs** (each row is an operation: `put_route`, `delete_upstream`, ...; head = replay).
   - Good: tiny rows; the log records intent ("deleted route X"), not just the result.
   - Cost: reading head or version N means replaying from the start, or maintaining periodic snapshots, which is option 1 plus extra machinery.
   - Cost: a bug in the replay (reducer) silently corrupts every later version, including ones already streamed to gateways. Full snapshots cannot drift like that.
   - Cost: rollback must compute an inverse or replay to N, then emit a "replace all" event anyway.

Column type for the body: `JSONB` (chosen) over `JSON`/`BYTEA`/`TEXT`. JSONB rejects malformed JSON at insert, supports `body->'routes'` queries and a `CHECK` on `body->>'version'`. It does not preserve key order or whitespace, which does not matter: the control plane always decodes into `config.Config` and re-encodes with `encoding/json`.

### B. Version assignment and write serialisation

1. **`pg_advisory_xact_lock(K)` at the start of every write transaction, then `version = head + 1`.** (chosen)
   - Writers queue on one lock and run strictly one after another. `mutate` runs exactly once, no retry loop.
   - The lock is transaction-scoped: COMMIT, ROLLBACK, or a dropped connection releases it. It cannot leak.
   - It works on an **empty** table, which matters (see option 4).
   - Versions are gapless: the number is computed under the lock and consumed only if the transaction commits.
2. **SERIALIZABLE isolation, retry on `40001`.** Correct, but every caller needs a retry loop that re-runs `mutate`, and the failure mode under contention is "spurious retries" instead of "wait in line". More code for no benefit at our write rate.
3. **Optimistic insert, rely on the primary key.** Read head, insert `head + 1`; a concurrent writer loses with `23505 unique_violation`, and the loser retries. Correct and lock-free, but again a retry loop around `mutate`, and the retry must distinguish "my version was taken" from other unique violations.
4. **`SELECT ... FOR UPDATE` on the head row.** Under READ COMMITTED the second writer blocks, and when the first commits it is handed the **old** head row (re-checked, still matching), computes the same `head + 1` and fails on the primary key: a spurious 500 unless we add a retry loop (option 3 in disguise). It also cannot serialise the very first write, because an empty table has no row to lock.
5. **A Postgres sequence (`BIGSERIAL`).** `nextval` is not transactional: a write that fails validation or rolls back burns a number, so versions get gaps. Gaps are harmless to the data plane but break "rollback default = head-1" and "history is 1..head". It also does not serialise the read-modify-write of the document, so we would still need option 1. Rejected (ADR 0005, architecture §11 row 16).

Lock keys (bigint, distinct, documented so nothing else in the database reuses them):

| Purpose | Key | Mnemonic |
|---|---|---|
| Config writes (Update, Rollback) | `0x746f6c6c67617465` | ASCII `"tollgate"` |
| Schema migrations at startup | `0x746f6c6c6d696772` | ASCII `"tollmigr"` |

### C. Concurrency control for API clients

1. **Document-level optimistic concurrency: `ETag: "<version>"` on every response, `If-Match: "<version>"` on writes.** (chosen) The store compares `If-Match` with head **under the write lock**; a mismatch returns `ErrVersionConflict` (HTTP 412) and writes nothing. Without `If-Match` a write is unconditional ("last writer wins", but never a lost update, because each write is applied to the latest head, not to what the client read).
2. **Per-object versions** (each route has its own revision). Finer-grained: editing route A would not conflict with a concurrent edit of route B. Costs a revision field per object in the model or a side table. Rejected for v1: writes are rare, and a 412 just means "re-read and retry".
3. **No concurrency control.** Two admins doing read-modify-write of the same route silently overwrite each other. Rejected.

`If-Match: "0"` is meaningful: it succeeds only while the database is empty, which makes "seed once" scripts safe.

## Decision

- **Model:** A1, full-document snapshots in one append-only table. Rows are never updated or deleted by the application.
- **Serialisation:** B1, `pg_advisory_xact_lock(0x746f6c6c67617465)` + `version = head + 1`, inside one READ COMMITTED transaction. No sequence anywhere.
- **Client concurrency:** C1, `ETag`/`If-Match` on the config version.
- **Rollback** (`Store.Rollback`) is a write like any other: under the same lock, copy the body of the target version, set `version = head + 1`, re-run `Normalize` + `Validate` **with today's rules** (a version that was valid under older rules but is invalid now is refused with 400 rather than streamed to gateways that would reject it), insert with `rolled_back_from = target`. `to = 0` means `head - 1`. A target outside `1..head` is `ErrNotFound` (so "roll back" when head is 1 is a 404: version 0, the empty config, is not a stored row and cannot be restored). `to = head` is allowed and re-publishes head as a new version; this is the supported way to make gateways retry a version they rejected (see architecture §5, RemoteSource).
- **Empty database:** `Head` returns an empty `config.Config` with `Version: 0`. Version 0 is never stored and never streamed (`head (0) > since (>= 0)` is false), so gateways connected to an empty control plane receive only heartbeats and stay not-ready until the first write creates version 1.
- **Every successful write creates a version,** even if the resulting document equals head (for example, a PUT of an unchanged route). This keeps "2xx write = new ETag" true without exceptions. Detecting no-op writes is listed as future work.
- **Reads** (`Head`, `Get`, `List`) take no lock. Each is a single statement, so it sees a consistent committed snapshot and never blocks or is blocked by writers.
- **Decoding stored bodies is lenient** (`json.Unmarshal`, unknown fields ignored) and does not re-validate. The database is trusted, and lenient reads let a newer schema remove a field without bricking reads of old rows. Strict decoding applies to untrusted input only (admin API bodies, the stream on the data plane).
- **Retention:** unbounded in v1. Pruning (for example "keep the newest 1 000 versions plus anything referenced by `rolled_back_from`") is future work; the foreign key is declared `ON DELETE SET NULL` so it will not block that.

### Schema

`internal/controlplane/migrations/0001_config_versions.sql`:

```sql
CREATE TABLE config_versions (
    version          BIGINT      PRIMARY KEY CHECK (version > 0),
    body             JSONB       NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    author           TEXT        NOT NULL DEFAULT '',
    comment          TEXT        NOT NULL DEFAULT '',
    rolled_back_from BIGINT      REFERENCES config_versions (version) ON DELETE SET NULL,
    CONSTRAINT config_versions_body_version CHECK ((body ->> 'version')::BIGINT = version),
    CONSTRAINT config_versions_rollback_older CHECK (rolled_back_from IS NULL OR rolled_back_from < version)
);
```

The primary-key index serves every query: head is `ORDER BY version DESC LIMIT 1` (a backward index scan), history is `WHERE version < $before ORDER BY version DESC LIMIT $n`.

Queries the Postgres store uses (exact text is the builder's choice; the shape is binding):

```sql
-- every write, in one transaction:
SELECT pg_advisory_xact_lock($1);   -- $1 = int64(0x746f6c6c67617465), bound from a Go constant
SELECT version, body FROM config_versions ORDER BY version DESC LIMIT 1;
INSERT INTO config_versions (version, body, author, comment, rolled_back_from)
VALUES ($1, $2, $3, $4, $5);                                   -- $5 NULL except for rollbacks
-- rollback additionally:
SELECT body FROM config_versions WHERE version = $1;
-- reads:
SELECT version, body FROM config_versions WHERE version = $1;
SELECT version, created_at, author, comment, rolled_back_from
  FROM config_versions WHERE ($1 = 0 OR version < $1) ORDER BY version DESC LIMIT $2;
```

### Migrations

No migration library (not an allowed dependency). Instead:

- SQL files live in `internal/controlplane/migrations/NNNN_name.sql` and are compiled in with `//go:embed migrations/*.sql`. `NNNN` is the migration version; files are applied in ascending order. Files are **forward-only and immutable** once released: a change is always a new file.
- `controlplane.Migrate(ctx, pool) error` runs at control-plane startup, before the listener opens, in **one transaction**:
  1. `SELECT pg_advisory_xact_lock($1)` with `$1 = int64(0x746f6c6c6d696772)` (bound from a Go constant, never a hand-typed decimal), so two control planes starting together cannot both migrate.
  2. `CREATE TABLE IF NOT EXISTS schema_migrations (version INT PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`.
  3. Read the applied rows. For each embedded file in order: if recorded, its SHA-256 must equal the recorded one (else fail: someone edited a released migration); if not recorded, `Exec` it and insert its row. If the database records a version the binary does not embed, fail ("database schema is newer than this binary").
  4. COMMIT. Postgres DDL is transactional, so a failure leaves the schema untouched.
- Idempotent: a second run finds every file recorded and changes nothing.
- Each file is executed with one argument-less `Exec`, which pgx sends over the simple query protocol, so a file may contain several statements. (The Postgres integration test pins this.)
- A failure is fatal: the control plane exits non-zero rather than serving against an unknown schema.
- `Store.Ping` (used by `/readyz`) checks that the database answers and that `max(schema_migrations.version)` equals the newest embedded migration.

## Consequences

- Good: the stored row, the streamed event and the rollback source are the same bytes-equivalent document. There is no reassembly or replay code to get wrong.
- Good: writes are trivially correct under concurrency: one lock, one read, one insert. The proof is short: "the version is computed while holding a lock every writer must take, and only consumed on commit".
- Good: the history is an append-only audit log with author, comment and rollback lineage.
- Good: config schema changes need no SQL migration.
- Cost: write throughput is bounded by one transaction at a time. At admin-API write rates this is irrelevant (a write is a few milliseconds).
- Cost: optimistic concurrency is coarse. Any write changes the ETag, so an `If-Match` PUT on route A fails if someone just changed route B. Clients re-read and retry.
- Cost: storage grows with every write and is never reclaimed in v1.
- Cost: `author` is a free-text label from a request header, not an authenticated identity (there is a single admin token). Do not use it for access decisions.
- Edge: a context error returned **after** COMMIT was sent leaves the outcome unknown (the version may exist). Clients that need exactly-once use `If-Match`: a retry of a write that did commit gets 412 instead of creating a duplicate.
- Edge: a database wipe restarts versions at 1; see ADR 0005, Consequences, for recovery.
