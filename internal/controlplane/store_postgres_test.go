package controlplane

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var pgSchemaSeq atomic.Int64

// postgresStoreFactory returns a factory that gives each caller a PostgresStore
// in its own freshly created schema (dropped on cleanup). It skips, without
// touching any database, unless TOLLGATE_TEST_DATABASE_URL is set.
func postgresStoreFactory(t *testing.T) func(*testing.T) Store {
	url := os.Getenv("TOLLGATE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TOLLGATE_TEST_DATABASE_URL not set")
	}
	return func(t *testing.T) Store {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		admin, err := pgxpool.New(ctx, url)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		schema := fmt.Sprintf("tgtest_%d_%d", time.Now().UnixNano(), pgSchemaSeq.Add(1))
		if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
			admin.Close()
			t.Fatalf("create schema: %v", err)
		}
		cfg, err := pgxpool.ParseConfig(url)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.RuntimeParams["search_path"] = schema
		cfg.MaxConns = 24 // 16 concurrent writers + readers
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
		store, err := NewPostgresStore(ctx, pool)
		if err != nil {
			t.Fatalf("NewPostgresStore: %v", err)
		}
		return store
	}
}
