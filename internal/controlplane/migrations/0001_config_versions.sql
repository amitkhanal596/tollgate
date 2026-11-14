-- 0001: append-only table of full config snapshots (ADR 0008).
-- Released migrations are immutable: Migrate refuses to start if this file's
-- SHA-256 differs from the one recorded in schema_migrations.
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
