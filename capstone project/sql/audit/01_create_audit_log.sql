-- ============================================================================
-- PulseTrack — Audit / metadata log (Q3's "Metadata"/"Audit" cross-cutting
-- concern, populated by every ingestion/transform/load step in Q4).
-- Column set matches Q6's explicit requirement: batch ID, pipeline ID, run
-- ID, start time, end time, status, source/processed/inserted/updated/
-- rejected/error counts. Pattern follows miniproject2's
-- gold.etl_audit_log/etl_audit_step_log, generalised beyond one warehouse.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_run_log (
    run_log_key      BIGINT GENERATED ALWAYS AS IDENTITY,
    batch_id         VARCHAR(50)  NOT NULL,   -- one per DAG run, e.g. 2026-01-15T00:00:00
    pipeline_id      VARCHAR(100) NOT NULL,   -- e.g. 'pulsetrack_medallion_pipeline'
    run_id           VARCHAR(100) NOT NULL,   -- Airflow run_id (or a generated UUID outside Airflow)
    step_name        VARCHAR(100) NOT NULL,   -- e.g. 'extract_conversions', 'silver_conversions', 'load_fact_conversion'
    layer            VARCHAR(20)  NOT NULL,   -- bronze | silver | gold | quality
    target_table     VARCHAR(100),
    status           VARCHAR(20)  NOT NULL,   -- running | success | failed
    start_time       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time         TIMESTAMP,
    source_count     BIGINT,
    processed_count  BIGINT,
    inserted_count   BIGINT,
    updated_count    BIGINT,
    rejected_count   BIGINT,
    error_count      BIGINT,
    notes            TEXT,

    CONSTRAINT pk_pipeline_run_log PRIMARY KEY (run_log_key)
);

CREATE INDEX IF NOT EXISTS ix_pipeline_run_log_batch ON audit.pipeline_run_log (batch_id);
CREATE INDEX IF NOT EXISTS ix_pipeline_run_log_step ON audit.pipeline_run_log (step_name, start_time);

-- Quarantine: rows that failed a business rule, a referential check, or
-- couldn't be parsed at all. Never silently dropped (Q3's "Error handling").
CREATE TABLE IF NOT EXISTS audit.quarantine (
    quarantine_key   BIGINT GENERATED ALWAYS AS IDENTITY,
    batch_id         VARCHAR(50)  NOT NULL,
    source_table     VARCHAR(100) NOT NULL,
    source_pk        TEXT,               -- the natural key value, when identifiable
    reason           VARCHAR(200) NOT NULL,
    raw_payload      TEXT,               -- the offending row/line, verbatim
    quarantined_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_quarantine PRIMARY KEY (quarantine_key)
);

CREATE INDEX IF NOT EXISTS ix_quarantine_source_table ON audit.quarantine (source_table, batch_id);

-- Watermark control table for incremental relational extraction (Q1 §7 /
-- Week 08's watermark pattern). One row per source table; the extractor
-- computes GREATEST(MAX(created_at), MAX(updated_at)) over what it JUST
-- pulled (parsed in Python — see python/parsing.py) and advances this value
-- only after a successful load, so a failed run never advances the
-- watermark past data that didn't actually land (Q5's recovery requirement).
CREATE TABLE IF NOT EXISTS audit.extraction_watermark (
    source_table    TEXT PRIMARY KEY,
    last_watermark  TIMESTAMP,   -- NULL = no successful extraction yet -> full load
    last_batch_id   TEXT,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);
