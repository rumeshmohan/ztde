-- ============================================================================
-- PulseTrack — Bronze layer (Q4)
-- One table per source table/collection, EXACT source shape (everything
-- TEXT for the 11 relational tables, matching pulsetrack_raw's own "raw dump,
-- untyped" design), plus the audit columns every Bronze table gets in this
-- course's house style: load_timestamp, source_system, batch_id.
-- No cleansing, no typing, no filtering here — that is Silver's job.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;
SET search_path TO bronze;

-- ---------------------------------------------------------------- relational source (pulsetrack_raw, via Python/psycopg extraction)

CREATE TABLE IF NOT EXISTS account_managers_raw (
    am_id TEXT, am_name TEXT, team TEXT, region TEXT, hire_date TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS advertisers_raw (
    advertiser_id TEXT, advertiser_name TEXT, vertical TEXT, account_manager_id TEXT,
    status TEXT, signup_date TEXT, billing_country TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS partners_raw (
    partner_id TEXT, partner_name TEXT, channel TEXT, media_buyer TEXT,
    join_date TEXT, payment_terms TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS offers_raw (
    offer_id TEXT, offer_name TEXT, advertiser_id TEXT, vertical TEXT,
    default_payout TEXT, payout_type TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS campaigns_raw (
    campaign_id TEXT, campaign_name TEXT, offer_id TEXT, advertiser_id TEXT,
    price_type TEXT, start_date TEXT, end_date TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS creatives_raw (
    creative_id TEXT, campaign_id TEXT, creative_name TEXT, format TEXT,
    size TEXT, landing_url TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS impressions_raw (
    impression_id TEXT, campaign_id TEXT, creative_id TEXT, partner_id TEXT,
    impression_time TEXT, placement_raw TEXT, country TEXT, device TEXT, bot_score TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clicks_raw (
    click_id TEXT, impression_id TEXT, campaign_id TEXT, creative_id TEXT, partner_id TEXT,
    click_time TEXT, ip TEXT, user_agent TEXT, country TEXT, device TEXT,
    is_unique TEXT, referrer_raw TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conversions_raw (
    conversion_id TEXT, click_id TEXT, campaign_id TEXT, advertiser_name TEXT,
    account_manager TEXT, campaign_name TEXT, partner_id TEXT, partner_name TEXT,
    offer_name TEXT, price_type TEXT, conversion_time TEXT, revenue TEXT, payout TEXT,
    currency TEXT, status TEXT, country TEXT, device TEXT, sub_id TEXT, is_test TEXT,
    legacy_flag TEXT, pixel_fired TEXT, internal_note TEXT, reserved_1 TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conversion_touchpoints_raw (
    touchpoint_id TEXT, conversion_id TEXT, click_id TEXT, touch_position TEXT, weight TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS revenue_adjustments_raw (
    adjustment_id TEXT, conversion_id TEXT, delta_amount TEXT, reason TEXT, adjusted_at TEXT,
    created_at TEXT, updated_at TEXT,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'pulsetrack_raw',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------- MongoDB source (Q4: parsed directly from the .jsonl export files by Python)
-- Kept as raw JSONB, untouched — Silver does the flattening/typing. This is
-- "JSON ingestion" (Q3/Q4): Postgres's native JSONB is the landing format,
-- no separate document store required to demonstrate it.

CREATE TABLE IF NOT EXISTS mongo_advertiser_accounts (
    mongo_id TEXT,              -- the $oid
    advertiser_id TEXT,          -- promoted for indexing/joins; still also inside raw_doc
    raw_doc JSONB NOT NULL,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'mongodb',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mongo_partner_profiles (
    mongo_id TEXT,
    partner_id TEXT,
    raw_doc JSONB NOT NULL,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'mongodb',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------- Kafka-style stream (Q4: parsed line-by-line by Python)
-- One row per envelope line (or per malformed line, flagged). Dedup by
-- event_id and schema-v2 handling are Silver's job — Bronze preserves
-- exactly what was on the wire, including duplicates and malformed lines,
-- per the medallion principle of "raw preservation with ingestion metadata."

CREATE TABLE IF NOT EXISTS stream_events (
    stream_event_key BIGINT GENERATED ALWAYS AS IDENTITY,
    topic TEXT,
    partition INT,
    kafka_offset BIGINT,
    envelope_ts BIGINT,          -- epoch millis, from the envelope
    event_key TEXT,               -- the conversion_id in the envelope key
    event_id TEXT,                -- from the JSON payload, NULL if malformed
    event_type TEXT,              -- conversion.created | conversion.status_changed | adjustment.applied | NULL if malformed
    schema_version TEXT,          -- '1.0' (implicit) | '2.0'
    payload JSONB,                 -- parsed payload, NULL if malformed
    raw_line TEXT NOT NULL,        -- verbatim source line, always kept
    is_malformed BOOLEAN NOT NULL DEFAULT FALSE,
    batch_id TEXT NOT NULL, source_system TEXT NOT NULL DEFAULT 'kafka_stream',
    load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_stream_events PRIMARY KEY (stream_event_key)
);

CREATE INDEX IF NOT EXISTS ix_stream_events_event_id ON stream_events (event_id);
CREATE INDEX IF NOT EXISTS ix_stream_events_key ON stream_events (event_key);
