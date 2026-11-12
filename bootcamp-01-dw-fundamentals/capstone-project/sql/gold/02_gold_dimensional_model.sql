-- ============================================================================
-- PulseTrack — Gold layer dimensional model  (Capstone Q2 deliverable)
-- ============================================================================
-- Implements the star schema designed in
-- ../02-source-forensics-and-edw-design.md (Part B). This is the SINGLE
-- target model every later question (Q4 ETL, Q7 Data Mart, Q8 OLAP, Q9
-- analytics) builds on. Do not alter grain/keys here without updating that
-- document's change log (and Q1's, if a KPI definition is affected).
--
-- House conventions followed (see Week 07/08/10 of the bootcamp):
--   * dim_*/fact_* naming; surrogate keys suffixed _key (GENERATED ALWAYS AS
--     IDENTITY); natural/business keys suffixed _id (UNIQUE constrained).
--   * Named constraints: pk_/fk_/uq_.
--   * load_timestamp on every table (Gold-layer audit column).
--   * Every dimension gets a -1 "Unknown" member row (seeded here) so a fact
--     row is never rejected just because a reference didn't resolve.
-- ============================================================================

DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;
SET search_path TO gold;

-- ============================================================================
-- DIMENSIONS
-- ============================================================================

-- dim_date — conformed across every fact. Static, built once by a date-spine
-- generator (Q4), not sourced from any raw table.
CREATE TABLE dim_date (
    date_key        INT         NOT NULL,   -- YYYYMMDD
    full_date       DATE        NOT NULL,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    day             SMALLINT    NOT NULL,
    day_of_week     SMALLINT    NOT NULL,   -- 1=Monday .. 7=Sunday
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN     NOT NULL,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key),
    CONSTRAINT uq_dim_date_full_date UNIQUE (full_date)
);

-- dim_account_manager — Type 1. AM's own attributes have no stated historical
-- requirement; only the advertiser->AM ASSIGNMENT is versioned (in dim_advertiser).
CREATE TABLE dim_account_manager (
    am_key          INT GENERATED ALWAYS AS IDENTITY,
    am_id           VARCHAR(10)  NOT NULL,      -- e.g. AM007
    am_name         VARCHAR(200),                -- 1 deliberate null in source (AM013)
    team            VARCHAR(100),
    region          VARCHAR(100),
    hire_date       DATE,
    load_timestamp  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_account_manager PRIMARY KEY (am_key),
    CONSTRAINT uq_dim_account_manager_am_id UNIQUE (am_id)
);

INSERT INTO dim_account_manager (am_key, am_id, am_name, team, region, hire_date)
OVERRIDING SYSTEM VALUE
VALUES (-1, 'UNKNOWN', 'Unknown Account Manager', NULL, NULL, NULL);

-- dim_advertiser — Type 2 HYBRID SCD. A new version row is created ONLY when
-- account_manager_key changes (the one stated historical requirement, Q1 §4 /
-- Q2 A.4). All other attributes are Type 1: overwritten across every version
-- row of the advertiser (current AND historical) by design — see
-- 02-source-forensics-and-edw-design.md B.3 for the justification.
--
-- effective_to convention (reconciled against Week 09/10's gold_dim_layer,
-- pulled into the course repo after this table was first drafted): the
-- CURRENT version's effective_to is a NOT NULL sentinel (DATE '9999-12-31'),
-- never NULL. This matches the house pattern (which uses a
-- TIMESTAMP '9999-12-31 23:59:59' sentinel) for the same reason it does
-- there: every downstream "which version was valid at time T" join can then
-- be a plain `WHERE t BETWEEN effective_from AND effective_to`, with no
-- `OR effective_to IS NULL` branch to remember (and forget) in Q4's ETL
-- resolution, Q6's reconciliation checks, and Q8/Q9's semantic layer alike.
-- Kept as DATE rather than the house's TIMESTAMP, though: the only source of
-- historical truth here, MongoDB account_manager_history[].from/to, is
-- genuinely date-only (Q1 §4's documented transition-day ambiguity is a
-- direct consequence of that) — a TIMESTAMP column would imply a precision
-- the source data does not have.
--
-- Unlike the house pattern, a partial UNIQUE index (not just a plain index)
-- enforces "at most one current version per advertiser_id" — see the
-- comment on uq_dim_advertiser_current_per_id below for why this is kept as
-- a deliberate, justified deviation rather than dropped to match.
CREATE TABLE dim_advertiser (
    advertiser_key      INT GENERATED ALWAYS AS IDENTITY,
    advertiser_id       VARCHAR(10)  NOT NULL,   -- e.g. ADV0001 — repeats across versions
    advertiser_name     VARCHAR(200) NOT NULL,
    vertical            VARCHAR(100),
    status              VARCHAR(20),
    billing_country     VARCHAR(10),
    account_manager_key INT          NOT NULL,   -- the versioned attribute
    effective_from      DATE         NOT NULL,
    effective_to        DATE         NOT NULL DEFAULT DATE '9999-12-31',  -- sentinel = current
    is_current          BOOLEAN      NOT NULL DEFAULT TRUE,
    version_number      SMALLINT     NOT NULL DEFAULT 1,
    load_timestamp      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_advertiser PRIMARY KEY (advertiser_key),
    CONSTRAINT fk_dim_advertiser_am FOREIGN KEY (account_manager_key)
        REFERENCES dim_account_manager (am_key),
    CONSTRAINT uq_dim_advertiser_id_version UNIQUE (advertiser_id, version_number)
);

-- Enforce exactly one CURRENT version per advertiser_id (a partial unique
-- index, not a plain UNIQUE constraint, because only is_current=TRUE rows
-- must be unique on advertiser_id — historical versions legitimately repeat
-- it). The house's Week 09/10 dim_customer/dim_account/etc. deliberately
-- omit this ("no unique constraint on customer_id — several versions... are
-- the point of Type 2"), relying on ETL logic alone to prevent two
-- simultaneously-current rows. Kept here anyway: that reasoning addresses a
-- different invariant (multiple versions are fine and expected) from the one
-- this index actually protects (at most one of them may be marked current at
-- once) — a retried/concurrent load without this guard can silently double
-- an advertiser's contribution to every downstream KPI until someone notices.
CREATE UNIQUE INDEX uq_dim_advertiser_current_per_id
    ON dim_advertiser (advertiser_id)
    WHERE is_current;

INSERT INTO dim_advertiser
    (advertiser_key, advertiser_id, advertiser_name, vertical, status, billing_country,
     account_manager_key, effective_from, effective_to, is_current, version_number)
OVERRIDING SYSTEM VALUE
VALUES (-1, 'UNKNOWN', 'Unknown Advertiser', NULL, NULL, NULL,
        -1, DATE '1900-01-01', DATE '9999-12-31', TRUE, 1);

-- dim_campaign — Type 1. Offer attributes are DENORMALISED here rather than
-- snowflaked into a separate dim_offer: Q1 has no Offer Performance KPI area,
-- so a separate offer dimension would exist to serve no query (B.3).
-- advertiser_id is kept as a plain business-key column, NOT a surrogate FK to
-- dim_advertiser: dim_advertiser.advertiser_id is not globally unique (it
-- repeats across SCD2 versions), so no single advertiser_key correctly
-- represents "this campaign's advertiser" independent of AM history. Consumers
-- needing current advertiser attributes join on advertiser_id WHERE is_current;
-- consumers needing the AM-at-event-time reach it via fact_conversion.advertiser_key
-- directly instead (never via dim_campaign).
CREATE TABLE dim_campaign (
    campaign_key    INT GENERATED ALWAYS AS IDENTITY,
    campaign_id     VARCHAR(12)  NOT NULL,       -- e.g. CMP00001
    campaign_name   VARCHAR(200),
    advertiser_id   VARCHAR(10)  NOT NULL,       -- resolved per A.4/B.3 precedence rule
    offer_id        VARCHAR(10),
    offer_name      VARCHAR(200),
    default_payout  NUMERIC(12,2),
    payout_type     VARCHAR(20),
    price_type      VARCHAR(20),                  -- canonicalised CPA/CPC/CPM/Fixed
    start_date      DATE,
    end_date        DATE,
    load_timestamp  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_campaign PRIMARY KEY (campaign_key),
    CONSTRAINT uq_dim_campaign_id UNIQUE (campaign_id)
);

INSERT INTO dim_campaign
    (campaign_key, campaign_id, campaign_name, advertiser_id, offer_id, offer_name,
     default_payout, payout_type, price_type, start_date, end_date)
OVERRIDING SYSTEM VALUE
VALUES (-1, 'UNKNOWN', 'Unknown Campaign', 'UNKNOWN', NULL, NULL,
        NULL, NULL, NULL, NULL, NULL);

-- dim_creative — Type 1. Child of campaign; no historical requirement stated.
CREATE TABLE dim_creative (
    creative_key    INT GENERATED ALWAYS AS IDENTITY,
    creative_id     VARCHAR(12)  NOT NULL,       -- e.g. CRE00001
    campaign_id     VARCHAR(12)  NOT NULL,
    creative_name   VARCHAR(200),
    format          VARCHAR(50),
    size            VARCHAR(20),
    landing_url     VARCHAR(500),
    load_timestamp  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_creative PRIMARY KEY (creative_key),
    CONSTRAINT uq_dim_creative_id UNIQUE (creative_id)
);

INSERT INTO dim_creative (creative_key, creative_id, campaign_id, creative_name, format, size, landing_url)
OVERRIDING SYSTEM VALUE
VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'Unknown Creative', NULL, NULL, NULL);

-- dim_partner — Type 1. Resolved per the A.5 precedence rule: partners_raw
-- first, MongoDB partner_profiles for the 40 prefixes missing from
-- partners_raw, else routed to the -1 Unknown member. channels[] from
-- MongoDB is multi-valued; kept as an array reference column plus a single
-- primary_channel for grouping (B.3) — a deliberate simplification, since no
-- Q1 KPI requires channel-level partner performance.
CREATE TABLE dim_partner (
    partner_key     INT GENERATED ALWAYS AS IDENTITY,
    partner_id      VARCHAR(10)  NOT NULL,       -- canonical 5-digit prefix
    partner_name    VARCHAR(200),
    channels        TEXT[],                       -- full MongoDB channels[] list
    primary_channel VARCHAR(50),                   -- channels[1], for grouping
    media_buyer     VARCHAR(200),
    join_date       DATE,
    payment_terms   VARCHAR(50),
    source_system   VARCHAR(20)  NOT NULL,        -- 'relational' | 'mongo' | 'unknown' — traceability (Q9)
    load_timestamp  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_partner PRIMARY KEY (partner_key),
    CONSTRAINT uq_dim_partner_id UNIQUE (partner_id)
);

INSERT INTO dim_partner
    (partner_key, partner_id, partner_name, channels, primary_channel, media_buyer,
     join_date, payment_terms, source_system)
OVERRIDING SYSTEM VALUE
VALUES (-1, 'UNKNOWN', 'Unknown Partner', NULL, NULL, NULL, NULL, NULL, 'unknown');

-- ============================================================================
-- FACTS
-- ============================================================================

-- fact_impression — grain: ONE IMPRESSION SERVED (one row per impression_id).
-- Factless; impression_count=1 is a convention measure so it sums like every
-- other fact. country/device kept as degenerate attributes — no Q1 KPI groups
-- by geography/device, so no dim_geography/dim_device is built (B.4).
CREATE TABLE fact_impression (
    impression_key   BIGINT GENERATED ALWAYS AS IDENTITY,
    impression_id    VARCHAR(20)  NOT NULL,       -- degenerate — kept for traceability
    date_key         INT          NOT NULL,
    campaign_key     INT          NOT NULL,
    creative_key     INT          NOT NULL,        -- -1 Unknown when blank (~5% of campaigns have no creative)
    partner_key      INT          NOT NULL,        -- -1 Unknown when unresolvable
    country          VARCHAR(100),
    device           VARCHAR(50),
    impression_time  TIMESTAMP    NOT NULL,
    impression_count SMALLINT     NOT NULL DEFAULT 1,
    load_timestamp   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_fact_impression PRIMARY KEY (impression_key),
    CONSTRAINT uq_fact_impression_id UNIQUE (impression_id),
    CONSTRAINT fk_fact_impression_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_impression_campaign FOREIGN KEY (campaign_key) REFERENCES dim_campaign (campaign_key),
    CONSTRAINT fk_fact_impression_creative FOREIGN KEY (creative_key) REFERENCES dim_creative (creative_key),
    CONSTRAINT fk_fact_impression_partner FOREIGN KEY (partner_key) REFERENCES dim_partner (partner_key)
);

-- fact_click — grain: ONE CLICK EVENT (one row per click_id). Factless;
-- click_count=1 convention measure. impression_id is a DEGENERATE reference
-- (not a FK to fact_impression) — it is lineage/traceability only, never
-- joined for rollups, and ~15% are legitimately null ("direct" clicks).
-- is_unique is deliberately NOT modelled: no KPI requires it (Q1 §6).
CREATE TABLE fact_click (
    click_key        BIGINT GENERATED ALWAYS AS IDENTITY,
    click_id         VARCHAR(20)  NOT NULL,
    impression_id    VARCHAR(20),                  -- nullable degenerate reference, NOT a FK
    date_key         INT          NOT NULL,
    campaign_key     INT          NOT NULL,
    creative_key     INT          NOT NULL,
    partner_key      INT          NOT NULL,
    country          VARCHAR(100),
    device           VARCHAR(50),
    click_time       TIMESTAMP    NOT NULL,
    click_count      SMALLINT     NOT NULL DEFAULT 1,
    load_timestamp   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_fact_click PRIMARY KEY (click_key),
    CONSTRAINT uq_fact_click_id UNIQUE (click_id),
    CONSTRAINT fk_fact_click_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_click_campaign FOREIGN KEY (campaign_key) REFERENCES dim_campaign (campaign_key),
    CONSTRAINT fk_fact_click_creative FOREIGN KEY (creative_key) REFERENCES dim_creative (creative_key),
    CONSTRAINT fk_fact_click_partner FOREIGN KEY (partner_key) REFERENCES dim_partner (partner_key)
);

-- fact_conversion — grain: ONE DEDUPLICATED, CURRENT-STATE BUSINESS CONVERSION
-- (one row per conversion_id, the MAX(updated_at) row; is_test rows never
-- loaded here at all). A REVISABLE transaction fact: adjustment_delta_total
-- is updated in place via targeted UPDATE as revenue_adjustments_raw/stream
-- adjustments arrive (Q1 §7 — measured lag up to 60 days, no safe cutoff, so
-- this row must stay patchable indefinitely, never treated as closed).
-- adjusted_revenue is a GENERATED (computed) column: Q4/Q5's incremental
-- pipeline only ever needs to update adjustment_delta_total, and
-- adjusted_revenue recomputes automatically — it can never drift out of sync.
-- advertiser_key/account_manager_key are resolved AT LOAD TIME to the
-- dim_advertiser version whose effective range contains conversion_time
-- (the historical join, not "current advertiser") — see B.4.
CREATE TABLE fact_conversion (
    conversion_key          BIGINT GENERATED ALWAYS AS IDENTITY,
    conversion_id           VARCHAR(15)  NOT NULL,   -- degenerate + the fact's own natural key
    date_key                INT          NOT NULL,
    advertiser_key          INT          NOT NULL,   -- historically-resolved dim_advertiser version
    account_manager_key     INT          NOT NULL,   -- historically-resolved, direct (no snowflake hop)
    campaign_key            INT          NOT NULL,
    creative_key            INT          NOT NULL,   -- resolved via the linked click; -1 if no click_id (~10%)
    partner_key             INT          NOT NULL,
    click_id                VARCHAR(20),              -- nullable degenerate reference
    status                  VARCHAR(20)  NOT NULL,    -- canonicalised approved/pending/reversed
    sub_id                  VARCHAR(50),
    country                 VARCHAR(100),
    device                  VARCHAR(50),
    conversion_time         TIMESTAMP    NOT NULL,
    revenue                 NUMERIC(12,2) NOT NULL,
    payout                  NUMERIC(12,2) NOT NULL,
    adjustment_delta_total  NUMERIC(12,2) NOT NULL DEFAULT 0,
    adjusted_revenue        NUMERIC(12,2) GENERATED ALWAYS AS (revenue + adjustment_delta_total) STORED,
    source_updated_at       TIMESTAMP    NOT NULL,    -- carried watermark value (conversions_raw.updated_at)
    load_timestamp          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_fact_conversion PRIMARY KEY (conversion_key),
    CONSTRAINT uq_fact_conversion_id UNIQUE (conversion_id),
    CONSTRAINT fk_fact_conversion_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_conversion_advertiser FOREIGN KEY (advertiser_key) REFERENCES dim_advertiser (advertiser_key),
    CONSTRAINT fk_fact_conversion_am FOREIGN KEY (account_manager_key) REFERENCES dim_account_manager (am_key),
    CONSTRAINT fk_fact_conversion_campaign FOREIGN KEY (campaign_key) REFERENCES dim_campaign (campaign_key),
    CONSTRAINT fk_fact_conversion_creative FOREIGN KEY (creative_key) REFERENCES dim_creative (creative_key),
    CONSTRAINT fk_fact_conversion_partner FOREIGN KEY (partner_key) REFERENCES dim_partner (partner_key)
);

-- bridge_conversion_touchpoint — grain: ONE (CONVERSION, CLICK) TOUCH.
-- Deliberate departure from textbook Kimball: a weighting bridge normally
-- connects a FACT to a DIMENSION group; here it connects fact_conversion to
-- fact_click directly, because multi-touch attribution is genuinely a
-- fact-to-fact relationship in this business (there is no dimension standing
-- in for "the click") — justified in B.4. weight is semi-additive: sums to
-- ≈1 PER CONVERSION only, never additive across conversions.
CREATE TABLE bridge_conversion_touchpoint (
    touchpoint_key   BIGINT GENERATED ALWAYS AS IDENTITY,
    touchpoint_id    VARCHAR(20)  NOT NULL,
    conversion_key   BIGINT       NOT NULL,
    click_key        BIGINT       NOT NULL,
    touch_position   VARCHAR(10)  NOT NULL,   -- first / mid / last
    weight           NUMERIC(6,4) NOT NULL,
    load_timestamp   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_bridge_conversion_touchpoint PRIMARY KEY (touchpoint_key),
    CONSTRAINT uq_bridge_touchpoint_id UNIQUE (touchpoint_id),
    CONSTRAINT fk_bridge_touchpoint_conversion FOREIGN KEY (conversion_key) REFERENCES fact_conversion (conversion_key),
    CONSTRAINT fk_bridge_touchpoint_click FOREIGN KEY (click_key) REFERENCES fact_click (click_key)
);

-- fact_revenue_adjustment — grain: ONE ADJUSTMENT TRANSACTION (one row per
-- adjustment_id). The 6.0% of adjustments whose conversion_id doesn't resolve
-- (A.2) are NOT loaded here — quarantined for the Q6 audit mechanism instead
-- of guessing an attachment. conversion_key is another justified fact-to-fact
-- FK: an adjustment inherently corrects a specific conversion event.
CREATE TABLE fact_revenue_adjustment (
    adjustment_key   BIGINT GENERATED ALWAYS AS IDENTITY,
    adjustment_id    VARCHAR(15)  NOT NULL,
    date_key         INT          NOT NULL,
    conversion_key   BIGINT       NOT NULL,
    delta_amount     NUMERIC(12,2) NOT NULL,   -- may be negative
    reason           VARCHAR(200),
    adjusted_at      TIMESTAMP    NOT NULL,
    load_timestamp   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_fact_revenue_adjustment PRIMARY KEY (adjustment_key),
    CONSTRAINT uq_fact_revenue_adjustment_id UNIQUE (adjustment_id),
    CONSTRAINT fk_fact_revenue_adjustment_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_revenue_adjustment_conversion FOREIGN KEY (conversion_key) REFERENCES fact_conversion (conversion_key)
);

-- ============================================================================
-- Indexes supporting the conformed join paths (B.2 bus matrix) and the KPI
-- rollups in B.5. Kept minimal here — Q7/Q8 add mart/OLAP-specific indexes.
-- ============================================================================
CREATE INDEX ix_fact_conversion_date ON fact_conversion (date_key);
CREATE INDEX ix_fact_conversion_advertiser ON fact_conversion (advertiser_key);
CREATE INDEX ix_fact_conversion_am ON fact_conversion (account_manager_key);
CREATE INDEX ix_fact_conversion_campaign ON fact_conversion (campaign_key);
CREATE INDEX ix_fact_conversion_partner ON fact_conversion (partner_key);
CREATE INDEX ix_fact_impression_date ON fact_impression (date_key);
CREATE INDEX ix_fact_impression_campaign ON fact_impression (campaign_key);
CREATE INDEX ix_fact_click_date ON fact_click (date_key);
CREATE INDEX ix_fact_click_campaign ON fact_click (campaign_key);
CREATE INDEX ix_bridge_touchpoint_conversion ON bridge_conversion_touchpoint (conversion_key);
CREATE INDEX ix_bridge_touchpoint_click ON bridge_conversion_touchpoint (click_key);
CREATE INDEX ix_fact_revenue_adjustment_conversion ON fact_revenue_adjustment (conversion_key);
