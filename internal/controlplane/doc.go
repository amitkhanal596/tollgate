// Package controlplane is Tollgate's control plane: the admin REST API, the
// versioned config store, the in-memory fan-out hub and the SSE config
// stream that data planes subscribe to (ADR 0001, 0005, 0008;
// docs/architecture.md §6).
//
// Files and responsibilities (Phase 4):
//
//	doc.go        this overview
//	store.go      Store interface, VersionInfo, UpdateRequest, sentinel errors
//	memstore.go   MemStore: in-memory Store used by API tests        (builder)
//	postgres.go   PostgresStore: pgxpool-backed Store               (core-builder)
//	migrate.go    Migrate: embedded SQL migrations under an advisory lock (builder)
//	migrations/   NNNN_name.sql, embedded with //go:embed           (ADR 0008)
//	hub.go        Hub (publish/subscribe, latest-wins) and Nodes (data-plane registry)
//	api.go        constants (env vars, headers, limits), Options, JSON DTOs
//	handler.go    NewHandler: routes, token middleware, CRUD handlers (builder)
//	stream.go     the SSE stream handler                            (core-builder)
//
// Dependency rule: controlplane imports internal/config and, in postgres.go
// and migrate.go only, pgx. It never imports internal/proxy; the JSON shape
// of config.Config is the only contract between the planes.
//
// Concurrency model: the Store serialises writers (one Postgres advisory
// lock). The Hub has no goroutine of its own: Publish and Subscribe are
// mutex-protected, non-blocking operations, and each SSE handler goroutine
// owns its subscription. Hub.Close (registered with
// http.Server.RegisterOnShutdown) ends every stream so Shutdown can finish.
package controlplane
