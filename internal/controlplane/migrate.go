package controlplane

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Advisory-lock keys (ADR 0008). They are bigint constants bound as
// parameters, never hand-typed decimals. Nothing else in the database may
// reuse them.
const (
	// lockKeyConfigWrites serialises Update and Rollback: ASCII "tollgate".
	lockKeyConfigWrites int64 = 0x746f6c6c67617465
	// lockKeyMigrations serialises Migrate across processes: ASCII "tollmigr".
	lockKeyMigrations int64 = 0x746f6c6c6d696772
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migration is one embedded NNNN_name.sql file.
type migration struct {
	version int
	name    string
	sql     string
	sum     string // hex SHA-256 of the file contents
}

// loadMigrations returns the embedded migrations in ascending version order.
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	var out []migration
	for _, e := range entries {
		file := e.Name()
		base, ok := strings.CutSuffix(file, ".sql")
		if !ok {
			continue
		}
		num, name, ok := strings.Cut(base, "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: want NNNN_name.sql", file)
		}
		v, err := strconv.Atoi(num)
		if err != nil || v < 1 {
			return nil, fmt.Errorf("migration %q: bad version %q", file, num)
		}
		body, err := migrationFiles.ReadFile("migrations/" + file)
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", file, err)
		}
		sum := sha256.Sum256(body)
		out = append(out, migration{version: v, name: name, sql: string(body), sum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	for i := 1; i < len(out); i++ {
		if out[i].version == out[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %d", out[i].version)
		}
	}
	return out, nil
}

// latestMigrationVersion is the newest embedded migration version.
func latestMigrationVersion() (int, error) {
	ms, err := loadMigrations()
	if err != nil {
		return 0, err
	}
	if len(ms) == 0 {
		return 0, fmt.Errorf("no embedded migrations")
	}
	return ms[len(ms)-1].version, nil
}

// Migrate applies the embedded SQL migrations idempotently, in one
// transaction under the migration advisory lock, so two control planes
// starting together cannot both migrate (ADR 0008). Postgres DDL is
// transactional, so a failure leaves the schema untouched. Released
// migrations are immutable: a changed checksum, or a database that is newer
// than this binary, is an error.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	ms, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate: begin: %w", err)
	}
	defer rollbackQuietly(tx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKeyMigrations); err != nil {
		return fmt.Errorf("migrate: take advisory lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INT PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("migrate: create schema_migrations: %w", err)
	}

	rows, err := tx.Query(ctx, `SELECT version, sha256 FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("migrate: read applied migrations: %w", err)
	}
	applied := map[int]string{}
	for rows.Next() {
		var v int
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			rows.Close()
			return fmt.Errorf("migrate: scan applied migration: %w", err)
		}
		applied[v] = sum
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("migrate: read applied migrations: %w", err)
	}

	known := map[int]bool{}
	for _, m := range ms {
		known[m.version] = true
	}
	for v := range applied {
		if !known[v] {
			return fmt.Errorf("migrate: database schema is newer than this binary (has migration %d)", v)
		}
	}

	for _, m := range ms {
		if sum, ok := applied[m.version]; ok {
			if sum != m.sum {
				return fmt.Errorf("migrate: migration %04d_%s was modified after release (checksum mismatch)", m.version, m.name)
			}
			continue
		}
		// No arguments: pgx uses the simple protocol, so a file may hold several statements.
		if _, err := tx.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("migrate: apply %04d_%s: %w", m.version, m.name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name, sha256) VALUES ($1, $2, $3)`,
			m.version, m.name, m.sum); err != nil {
			return fmt.Errorf("migrate: record %04d_%s: %w", m.version, m.name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate: commit: %w", err)
	}
	return nil
}
