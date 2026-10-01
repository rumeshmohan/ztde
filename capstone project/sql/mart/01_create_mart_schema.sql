-- ============================================================================
-- PulseTrack — Data Mart (Q7)
-- Business-oriented, pre-aggregated layer built FROM the Gold EDW (Q2) —
-- never from Bronze/Silver. Grain, dimension and fact choices are all
-- driven directly by Q1's KPI spec (§3.1-3.6), not by Gold's structure.
--
-- WHY A MART, NOT DIRECT GOLD ACCESS (required justification):
--   1. Grain mismatch: Gold's finest fact (fact_conversion) is one row per
--      conversion; every BI question in Q1 §5 is asked at Advertiser/
--      Campaign/Partner x Day/Month/Quarter grain. Answering them straight
--      off Gold means every dashboard query re-aggregates ~86K conversion
--      rows (plus a 165K-row bridge join for anything partner-attributed)
--      from scratch, every time.
--   2. Correctness is easy to get wrong ad hoc: Q1 §3.4 already showed
--      attribution percentage must be SUM-then-divide, never averaged; Q2
--      A.3 showed naive joins fan out. A mart bakes the correct aggregation
--      ONCE, centrally, instead of trusting every analyst to re-derive it.
--   3. Multi-touch attribution has genuinely different additive semantics
--      from direct performance (one conversion's credit split across
--      multiple partners) — Gold models this correctly as a bridge, which
--      is exactly the kind of construct a BI tool should never have to
--      query directly.
--   4. Decoupling: Gold's schema (SCD2 dim_advertiser, degenerate columns,
--      -1 Unknown members) can evolve for warehouse reasons without
--      breaking every downstream report, as long as the mart's contract
--      stays stable.
--
-- TWO FACTS, NOT ONE (a deliberate grain split, same reasoning as Q2's
-- bridge table): direct funnel performance and multi-touch attribution
-- are NOT the same additive population — a conversion's revenue appears
-- ONCE in direct performance but is SPLIT FRACTIONALLY across N partners
-- in attribution. Merging them into one fact would force one of the two
-- to be either double-counted or approximated.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- ---------------------------------------------------------------- dimensions
-- Deliberately Type-1/flat (current state only), keyed by the STABLE
-- natural id, not Gold's surrogate keys — a business user browsing "by
-- advertiser" wants today's 60 advertisers, not Gold's 74 SCD2 version
-- rows. Historical accuracy is NOT lost: fact rows below still aggregate
-- using the AM that was historically correct at conversion time (resolved
-- already in Gold) — only the DIMENSION's browsing/filtering experience is
-- simplified to current-state, a deliberate, stated design choice.

CREATE TABLE mart.dim_advertiser (
    advertiser_id               VARCHAR(10) PRIMARY KEY,
    advertiser_name             VARCHAR(200),
    vertical                    VARCHAR(100),
    status                      VARCHAR(20),
    billing_country             VARCHAR(10),
    current_account_manager_id  VARCHAR(10),
    current_account_manager_name VARCHAR(200)
);

CREATE TABLE mart.dim_campaign (
    campaign_id     VARCHAR(12) PRIMARY KEY,
    campaign_name   VARCHAR(200),
    advertiser_id   VARCHAR(10),
    advertiser_name VARCHAR(200),
    offer_name      VARCHAR(200),
    price_type      VARCHAR(20),
    start_date      DATE,
    end_date        DATE
);

CREATE TABLE mart.dim_creative (
    creative_id     VARCHAR(12) PRIMARY KEY,
    creative_name   VARCHAR(200),
    campaign_id     VARCHAR(12),
    campaign_name   VARCHAR(200),
    format          VARCHAR(50),
    size            VARCHAR(20)
);

CREATE TABLE mart.dim_partner (
    partner_id      VARCHAR(10) PRIMARY KEY,
    partner_name    VARCHAR(200),
    primary_channel VARCHAR(50),
    media_buyer     VARCHAR(200)
);

CREATE TABLE mart.dim_account_manager (
    am_id   VARCHAR(10) PRIMARY KEY,
    am_name VARCHAR(200),
    team    VARCHAR(100),
    region  VARCHAR(100)
);

-- dim_date is conformed and reused directly from Gold (gold.dim_date) —
-- no reason to duplicate a static calendar spine.

-- Every dimension gets an 'UNKNOWN' row matching Gold's -1 convention, so a
-- fact row that resolved to Gold's -1 Unknown member still joins cleanly here.
INSERT INTO mart.dim_advertiser VALUES ('UNKNOWN', 'Unknown Advertiser', NULL, NULL, NULL, NULL, NULL);
INSERT INTO mart.dim_campaign VALUES ('UNKNOWN', 'Unknown Campaign', 'UNKNOWN', NULL, NULL, NULL, NULL, NULL);
INSERT INTO mart.dim_creative VALUES ('UNKNOWN', 'Unknown Creative', 'UNKNOWN', NULL, NULL, NULL);
INSERT INTO mart.dim_partner VALUES ('UNKNOWN', 'Unknown Partner', NULL, NULL);
INSERT INTO mart.dim_account_manager VALUES ('UNKNOWN', 'Unknown Account Manager', NULL, NULL);

-- ---------------------------------------------------------------- facts

-- mart.fact_performance_daily — grain: one row per
-- (date, advertiser, campaign, creative, partner, account_manager)
-- combination that actually occurred. account_manager is NULL for rows
-- whose measures come only from impressions/clicks (Q2's bus matrix: AM is
-- not resolvable for those without a conversion event) — NOT the same as
-- 'UNKNOWN' (a real gap in Gold's history), a plain absence of the concept.
CREATE TABLE mart.fact_performance_daily (
    date_key            INT NOT NULL,
    advertiser_id       VARCHAR(10) NOT NULL,
    campaign_id         VARCHAR(12) NOT NULL,
    creative_id         VARCHAR(12) NOT NULL,
    partner_id          VARCHAR(10) NOT NULL,
    account_manager_id  VARCHAR(10),   -- NULL = not applicable (impression/click-only rows)

    impressions             BIGINT NOT NULL DEFAULT 0,
    clicks                  BIGINT NOT NULL DEFAULT 0,
    conversions             BIGINT NOT NULL DEFAULT 0,
    revenue                 NUMERIC(14,2) NOT NULL DEFAULT 0,
    payout                  NUMERIC(14,2) NOT NULL DEFAULT 0,
    adjustment_delta_total  NUMERIC(14,2) NOT NULL DEFAULT 0,
    adjusted_revenue        NUMERIC(14,2) NOT NULL DEFAULT 0,

    -- UNIQUE, not PRIMARY KEY: Postgres PKs reject NULL in any column, but
    -- account_manager_id is deliberately NULLable (impression/click-only
    -- rows). A UNIQUE constraint treats NULLs as distinct from each other,
    -- which is exactly the semantics wanted here (caught by actually
    -- running the load — the first version of this table used PRIMARY KEY
    -- and rejected every impression/click row on the very first insert).
    CONSTRAINT uq_fact_performance_daily UNIQUE
        (date_key, advertiser_id, campaign_id, creative_id, partner_id, account_manager_id)
);

CREATE INDEX ix_perf_daily_date ON mart.fact_performance_daily (date_key);
CREATE INDEX ix_perf_daily_advertiser ON mart.fact_performance_daily (advertiser_id);
CREATE INDEX ix_perf_daily_campaign ON mart.fact_performance_daily (campaign_id);
CREATE INDEX ix_perf_daily_partner ON mart.fact_performance_daily (partner_id);
CREATE INDEX ix_perf_daily_am ON mart.fact_performance_daily (account_manager_id);

-- mart.fact_attribution_daily — grain: one row per (date, partner, campaign)
-- — the multi-touch attribution population, kept separate from direct
-- performance for the additive reason stated at the top of this file.
CREATE TABLE mart.fact_attribution_daily (
    date_key            INT NOT NULL,
    partner_id          VARCHAR(10) NOT NULL,
    campaign_id         VARCHAR(12) NOT NULL,

    attributed_conversions  NUMERIC(14,4) NOT NULL DEFAULT 0,  -- SUM(touchpoint weight) — fractional
    attributed_revenue      NUMERIC(14,2) NOT NULL DEFAULT 0,  -- SUM(weight * conversion revenue)
    touch_count             BIGINT NOT NULL DEFAULT 0,          -- raw touchpoint count, for context

    CONSTRAINT pk_fact_attribution_daily PRIMARY KEY (date_key, partner_id, campaign_id)
);

CREATE INDEX ix_attr_daily_date ON mart.fact_attribution_daily (date_key);
CREATE INDEX ix_attr_daily_partner ON mart.fact_attribution_daily (partner_id);
