-- 01_create_schemas_tables.sql
-- Creates schemas, tables, constraints and indexes for the HN ETL pipeline.
-- Safe to re-run (IF NOT EXISTS everywhere).
-- v2: dropped stg.hn_stories_raw.children_count (field does not exist in the API)
--     and clean.hn_stories.is_title_truncated (no signal in the data).

BEGIN;

CREATE SCHEMA IF NOT EXISTS etl;    -- pipeline metadata
CREATE SCHEMA IF NOT EXISTS stg;    -- staging (raw, as received)
CREATE SCHEMA IF NOT EXISTS clean;  -- typed, validated, deduplicated
CREATE SCHEMA IF NOT EXISTS dw;     -- analysis-ready model

-- ---------------------------------------------------------------
-- etl: pipeline metadata
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl.pipeline_runs (
    run_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_type          TEXT        NOT NULL CHECK (run_type IN ('full', 'incremental')),
    range_start       TIMESTAMPTZ,
    range_end         TIMESTAMPTZ,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    status            TEXT        NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
    records_extracted INTEGER     NOT NULL DEFAULT 0 CHECK (records_extracted >= 0),
    records_loaded    INTEGER     NOT NULL DEFAULT 0 CHECK (records_loaded >= 0),
    records_rejected  INTEGER     NOT NULL DEFAULT 0 CHECK (records_rejected >= 0),
    error_message     TEXT,
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE TABLE IF NOT EXISTS etl.batch_log (
    batch_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id            BIGINT      NOT NULL REFERENCES etl.pipeline_runs (run_id),
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
    records_extracted INTEGER     NOT NULL DEFAULT 0 CHECK (records_extracted >= 0),
    records_loaded    INTEGER     NOT NULL DEFAULT 0 CHECK (records_loaded >= 0),
    records_rejected  INTEGER     NOT NULL DEFAULT 0 CHECK (records_rejected >= 0),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    error_message     TEXT,
    CHECK (window_end > window_start),
    UNIQUE (run_id, window_start)
);

CREATE TABLE IF NOT EXISTS etl.checkpoint (
    pipeline_name           TEXT PRIMARY KEY,
    last_completed_window_end TIMESTAMPTZ,
    max_updated_at          TIMESTAMPTZ,
    last_run_id             BIGINT REFERENCES etl.pipeline_runs (run_id),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl.rejected_records (
    reject_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id       BIGINT      NOT NULL REFERENCES etl.pipeline_runs (run_id),
    batch_id     BIGINT      REFERENCES etl.batch_log (batch_id),
    object_id    TEXT,                -- may itself be the problem, so nullable text
    raw_payload  JSONB       NOT NULL,
    reason       TEXT        NOT NULL,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------
-- stg: staging. Everything is text, no data constraints, append-only.
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg.hn_stories_raw (
    stg_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id         BIGINT NOT NULL REFERENCES etl.pipeline_runs (run_id),
    batch_id       BIGINT REFERENCES etl.batch_log (batch_id),
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    object_id      TEXT,
    title          TEXT,
    url            TEXT,
    author         TEXT,
    points         TEXT,
    num_comments   TEXT,
    created_at     TEXT,
    created_at_i   TEXT,
    updated_at     TEXT,
    tags           TEXT,
    row_hash       TEXT,
    payload        JSONB          -- full original record as received
);

CREATE INDEX IF NOT EXISTS idx_stg_batch  ON stg.hn_stories_raw (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_object ON stg.hn_stories_raw (object_id);

-- ---------------------------------------------------------------
-- clean: typed, validated, one row per story
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.hn_stories (
    object_id          BIGINT PRIMARY KEY,          -- dedup key
    title              TEXT,
    url                TEXT,
    author             TEXT,
    points             INTEGER     CHECK (points >= 0),
    num_comments       INTEGER     CHECK (num_comments >= 0),
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ,
    story_type         TEXT        NOT NULL CHECK (story_type IN ('regular', 'show_hn', 'ask_hn')),
    row_hash           TEXT        NOT NULL,        -- change detection
    first_run_id       BIGINT      NOT NULL REFERENCES etl.pipeline_runs (run_id),
    last_run_id        BIGINT      NOT NULL REFERENCES etl.pipeline_runs (run_id),
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_clean_created ON clean.hn_stories (created_at);
CREATE INDEX IF NOT EXISTS idx_clean_updated ON clean.hn_stories (updated_at);

-- ---------------------------------------------------------------
-- dw: transformed, analysis-ready
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_author (
    author_key  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    author_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.dim_domain (
    domain_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    domain     TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.fact_story (
    story_id       BIGINT PRIMARY KEY,
    author_key     BIGINT REFERENCES dw.dim_author (author_key),
    domain_key     BIGINT REFERENCES dw.dim_domain (domain_key),  -- NULL for text posts
    created_at     TIMESTAMPTZ NOT NULL,
    hour_of_day    SMALLINT    NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    story_type     TEXT        NOT NULL
                    CHECK (story_type IN ('regular', 'show_hn', 'ask_hn')),
    points         INTEGER     CHECK (points >= 0),
    num_comments   INTEGER     CHECK (num_comments >= 0),
    has_discussion BOOLEAN     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_author ON dw.fact_story (author_key);
CREATE INDEX IF NOT EXISTS idx_fact_domain ON dw.fact_story (domain_key);

-- ---------------------------------------------------------------
-- v1 -> v2 migration (no-op on a fresh database)
-- ---------------------------------------------------------------
ALTER TABLE stg.hn_stories_raw DROP COLUMN IF EXISTS children_count;
ALTER TABLE clean.hn_stories   DROP COLUMN IF EXISTS is_title_truncated;

COMMIT;