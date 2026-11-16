package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// freshSchemaPool returns a pool whose search_path is a brand-new schema that
// has NOT been migrated (dropped on cleanup). It skips, without touching any
// database, unless TOLLGATE_TEST_DATABASE_URL is set.
func freshSchemaPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TOLLGATE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TOLLGATE_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	schema := fmt.Sprintf("tgmig_%d_%d", time.Now().UnixNano(), pgSchemaSeq.Add(1))
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dcancel()
		_, _ = admin.Exec(dctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	return pool
}

func migrationRows(t *testing.T, pool *pgxpool.Pool) (total, distinct int) {
	t.Helper()
	err := pool.QueryRow(testCtx(t), `SELECT count(*), count(DISTINCT version) FROM schema_migrations`).Scan(&total, &distinct)
	if err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	return total, distinct
}

func TestFixRound4_MigrateIdempotent(t *testing.T) {
	defer failOnPanic(t)
	pool := freshSchemaPool(t)
	ms, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		if err := Migrate(testCtx(t), pool); err != nil {
			t.Fatalf("Migrate call %d: %v", i, err)
		}
	}
	if total, _ := migrationRows(t, pool); total != len(ms) {
		t.Fatalf("schema_migrations has %d rows, want %d", total, len(ms))
	}
	// The data survives a re-run: Migrate never re-applies (and so never drops) a table.
	ps := &PostgresStore{pool: pool}
	mustUpdate(t, ps, UpdateRequest{}, mutSeed)
	if err := Migrate(testCtx(t), pool); err != nil {
		t.Fatalf("Migrate on a populated database: %v", err)
	}
	if h := mustHead(t, ps); h.Version != 1 {
		t.Fatalf("head version %d after re-Migrate, want 1", h.Version)
	}
}

func TestFixRound4_MigrateConcurrent(t *testing.T) {
	defer failOnPanic(t)
	pool := freshSchemaPool(t)
	ms, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = Migrate(testCtx(t), pool)
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent Migrate #%d: %v", i, err)
		}
	}
	if total, distinct := migrationRows(t, pool); total != len(ms) || distinct != len(ms) {
		t.Fatalf("schema_migrations rows = %d (%d distinct), want %d", total, distinct, len(ms))
	}
}

func TestFixRound4_MigrateTamperedChecksum(t *testing.T) {
	defer failOnPanic(t)
	pool := freshSchemaPool(t)
	if err := Migrate(testCtx(t), pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(testCtx(t), `UPDATE schema_migrations SET sha256 = 'deadbeef' WHERE version = 1`); err != nil {
		t.Fatal(err)
	}
	err := Migrate(testCtx(t), pool)
	if err == nil {
		t.Fatal("Migrate accepted a modified released migration")
	}
	msg := err.Error()
	if !strings.Contains(msg, "checksum") || !strings.Contains(msg, "0001") {
		t.Fatalf("error %q should name the migration (0001) and say checksum", msg)
	}
}

func TestFixRound4_SchemaNewerThanBinary(t *testing.T) {
	defer failOnPanic(t)
	pool := freshSchemaPool(t)
	if err := Migrate(testCtx(t), pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(testCtx(t),
		`INSERT INTO schema_migrations (version, name, sha256) VALUES (9999, 'from_the_future', 'x')`); err != nil {
		t.Fatal(err)
	}
	err := Migrate(testCtx(t), pool)
	if err == nil {
		t.Fatal("Migrate accepted a database newer than this binary")
	}
	if !strings.Contains(err.Error(), "newer") {
		t.Fatalf("error %q should say the database is newer than the binary", err)
	}
	if err := (&PostgresStore{pool: pool}).Ping(testCtx(t)); err == nil {
		t.Fatal("Ping succeeded against a database newer than this binary")
	}
	if _, err := NewPostgresStore(testCtx(t), pool); err == nil {
		t.Fatal("NewPostgresStore succeeded against a database newer than this binary")
	}
}

// ---- rollback to a version that is invalid under today's rules ------------------

// invalidOldBody is a stored body that was fine once but fails Validate now:
// its route points at an upstream that does not exist.
const invalidOldBody = `{"version":1,"routes":[{"name":"r","path_prefix":"/r","upstream":"gone"}],"upstreams":[{"name":"web","backends":["http://127.0.0.1:9001"]}]}`

func TestFixRound4_RollbackToInvalidVersion_MemStore(t *testing.T) {
	defer failOnPanic(t)
	s := NewMemStore()
	// MemStore has no seam for storing a body that Validate rejects, so plant
	// the old version directly (same package), as a database would hold it.
	var old config.Config
	if err := json.Unmarshal([]byte(invalidOldBody), &old); err != nil {
		t.Fatal(err)
	}
	s.versions = append(s.versions, memVersion{cfg: &old, info: VersionInfo{Version: 1}})
	mustUpdate(t, s, UpdateRequest{}, func(c *config.Config) error { *c = config.Config{}; return mutSeed(c) }) // v2, valid

	_, err := s.Rollback(testCtx(t), 1, UpdateRequest{})
	if !errors.Is(err, config.ErrInvalid) {
		t.Fatalf("Rollback to an invalid old version: err = %v, want errors.Is(config.ErrInvalid)", err)
	}
	if h := mustHead(t, s); h.Version != 2 {
		t.Fatalf("head version %d after rejected rollback, want 2", h.Version)
	}
	if l, _ := s.List(testCtx(t), 10, 0); len(l) != 2 {
		t.Fatalf("%d versions after rejected rollback, want 2", len(l))
	}
}

func TestFixRound4_RollbackToInvalidVersion_PostgresStore(t *testing.T) {
	defer failOnPanic(t)
	pool := freshSchemaPool(t)
	ps, err := NewPostgresStore(testCtx(t), pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(testCtx(t), `INSERT INTO config_versions (version, body) VALUES (1, $1::jsonb)`, invalidOldBody); err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, ps, UpdateRequest{}, func(c *config.Config) error { *c = config.Config{}; return mutSeed(c) }) // v2

	_, err = ps.Rollback(testCtx(t), 1, UpdateRequest{})
	if !errors.Is(err, config.ErrInvalid) {
		t.Fatalf("Rollback to an invalid old version: err = %v, want errors.Is(config.ErrInvalid)", err)
	}
	if h := mustHead(t, ps); h.Version != 2 {
		t.Fatalf("head version %d after rejected rollback, want 2", h.Version)
	}
	if l, _ := ps.List(testCtx(t), 10, 0); len(l) != 2 {
		t.Fatalf("%d versions after rejected rollback, want 2", len(l))
	}
}
