package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore is the pgxpool-backed Store (ADR 0008): one append-only
// table of full-document snapshots, writers serialised by an advisory lock.
type PostgresStore struct {
	pool *pgxpool.Pool
}

var _ Store = (*PostgresStore)(nil)

// NewPostgresStore runs Migrate and returns a store on pool. The caller owns
// the pool and closes it.
func NewPostgresStore(ctx context.Context, pool *pgxpool.Pool) (*PostgresStore, error) {
	if err := Migrate(ctx, pool); err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

// rollbackQuietly aborts tx after a failure. It uses its own short context
// because the caller's may already be canceled, which is exactly when the
// rollback matters (it releases the advisory lock). After a successful
// Commit it is a harmless no-op.
func rollbackQuietly(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// decodeStoredBody decodes a stored JSONB body leniently (unknown fields ignored,
// no re-validation; ADR 0008).
func decodeStoredBody(body []byte, version int64) (*config.Config, error) {
	c := new(config.Config)
	if err := json.Unmarshal(body, c); err != nil {
		return nil, fmt.Errorf("decode stored config version %d: %w", version, err)
	}
	return c, nil
}

// Head implements Store.
func (s *PostgresStore) Head(ctx context.Context) (*config.Config, error) {
	c, _, err := headOf(ctx, s.pool)
	return c, err
}

// queryRower is satisfied by *pgxpool.Pool and pgx.Tx.
type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// headOf reads the newest version; an empty table is version 0 / empty config.
func headOf(ctx context.Context, q queryRower) (*config.Config, int64, error) {
	var (
		v    int64
		body []byte
	)
	err := q.QueryRow(ctx, `SELECT version, body FROM config_versions ORDER BY version DESC LIMIT 1`).Scan(&v, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return &config.Config{}, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("read head: %w", err)
	}
	c, err := decodeStoredBody(body, v)
	if err != nil {
		return nil, 0, err
	}
	return c, v, nil
}

// Get implements Store.
func (s *PostgresStore) Get(ctx context.Context, version int64) (*config.Config, error) {
	if version < 1 {
		return nil, fmt.Errorf("%w: version %d", ErrNotFound, version)
	}
	var body []byte
	err := s.pool.QueryRow(ctx, `SELECT body FROM config_versions WHERE version = $1`, version).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: version %d", ErrNotFound, version)
	}
	if err != nil {
		return nil, fmt.Errorf("get version %d: %w", version, err)
	}
	return decodeStoredBody(body, version)
}

// List implements Store.
func (s *PostgresStore) List(ctx context.Context, limit int, before int64) ([]VersionInfo, error) {
	limit = clampLimit(limit)
	rows, err := s.pool.Query(ctx, `SELECT version, created_at, author, comment, rolled_back_from
		FROM config_versions WHERE ($1::bigint <= 0 OR version < $1) ORDER BY version DESC LIMIT $2`, before, limit)
	if err != nil {
		return nil, fmt.Errorf("list versions: %w", err)
	}
	defer rows.Close()
	out := []VersionInfo{} // non-nil even when empty
	for rows.Next() {
		var vi VersionInfo
		if err := rows.Scan(&vi.Version, &vi.CreatedAt, &vi.Author, &vi.Comment, &vi.RolledBackFrom); err != nil {
			return nil, fmt.Errorf("list versions: scan: %w", err)
		}
		vi.CreatedAt = vi.CreatedAt.UTC()
		out = append(out, vi)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list versions: %w", err)
	}
	return out, nil
}

// Update implements Store.
func (s *PostgresStore) Update(ctx context.Context, req UpdateRequest, mutate func(*config.Config) error) (*config.Config, error) {
	return s.write(ctx, req, func(_ pgx.Tx, head *config.Config, _ int64) (*config.Config, *int64, error) {
		// head was freshly decoded inside this transaction, so it is already our private copy.
		if err := mutate(head); err != nil {
			return nil, nil, err // returned unchanged, per the Store contract
		}
		return head, nil, nil
	})
}

// Rollback implements Store.
func (s *PostgresStore) Rollback(ctx context.Context, to int64, req UpdateRequest) (*config.Config, error) {
	return s.write(ctx, req, func(tx pgx.Tx, _ *config.Config, headVersion int64) (*config.Config, *int64, error) {
		target := to
		if target == 0 {
			target = headVersion - 1
		}
		if target < 1 || target > headVersion {
			return nil, nil, fmt.Errorf("%w: rollback target %d (head is %d)", ErrNotFound, to, headVersion)
		}
		var body []byte
		if err := tx.QueryRow(ctx, `SELECT body FROM config_versions WHERE version = $1`, target).Scan(&body); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, nil, fmt.Errorf("%w: version %d", ErrNotFound, target)
			}
			return nil, nil, fmt.Errorf("read rollback target %d: %w", target, err)
		}
		c, err := decodeStoredBody(body, target)
		if err != nil {
			return nil, nil, err
		}
		return c, &target, nil
	})
}

// write is the single write path. In one transaction: take the write lock,
// read head, check IfMatch, build the new document via build, stamp
// version = head+1, Normalize + Validate, INSERT, COMMIT.
func (s *PostgresStore) write(ctx context.Context, req UpdateRequest,
	build func(tx pgx.Tx, head *config.Config, headVersion int64) (*config.Config, *int64, error)) (*config.Config, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin write: %w", err)
	}
	defer rollbackQuietly(tx)

	// Held until COMMIT/ROLLBACK/disconnect, so it cannot leak.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKeyConfigWrites); err != nil {
		return nil, fmt.Errorf("take write lock: %w", err)
	}
	head, headVersion, err := headOf(ctx, tx)
	if err != nil {
		return nil, err
	}
	if req.IfMatch != nil && *req.IfMatch != headVersion {
		return nil, fmt.Errorf("%w: if-match %d, head is %d", ErrVersionConflict, *req.IfMatch, headVersion)
	}

	next, rolledFrom, err := build(tx, head, headVersion)
	if err != nil {
		return nil, err
	}
	next.Version = headVersion + 1
	next.Normalize()
	if err := next.Validate(); err != nil {
		return nil, err
	}

	body, err := json.Marshal(next)
	if err != nil {
		return nil, fmt.Errorf("encode config version %d: %w", next.Version, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO config_versions (version, body, author, comment, rolled_back_from)
		VALUES ($1, $2, $3, $4, $5)`, next.Version, body, req.Author, req.Comment, rolledFrom); err != nil {
		return nil, fmt.Errorf("insert config version %d: %w", next.Version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit config version %d: %w", next.Version, err)
	}
	return next, nil
}

// Ping implements Store: the database answers and the schema is at the
// newest embedded migration.
func (s *PostgresStore) Ping(ctx context.Context) error {
	want, err := latestMigrationVersion()
	if err != nil {
		return err
	}
	var got *int
	if err := s.pool.QueryRow(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&got); err != nil {
		return fmt.Errorf("ping: read schema version: %w", err)
	}
	if got == nil || *got != want {
		have := 0
		if got != nil {
			have = *got
		}
		return fmt.Errorf("ping: schema at version %d, want %d", have, want)
	}
	return nil
}
