-- ============================================================================
-- PulseTrack — Silver: reference/snowflake-chain entities
-- account_managers -> advertisers (+ AM history from Mongo) -> partners
-- (+ Mongo enrichment) -> offers -> campaigns (conflict-resolved) -> creatives
-- ============================================================================

-- ---------------------------------------------------------------- account_managers
DROP TABLE IF EXISTS silver.account_managers;
CREATE TABLE silver.account_managers AS
SELECT
    am_id,
    NULLIF(TRIM(am_name), '') AS am_name,
    NULLIF(TRIM(team), '')    AS team,
    NULLIF(TRIM(region), '')  AS region,
    silver.parse_flexible_date(hire_date) AS hire_date
FROM bronze.account_managers_raw;

ALTER TABLE silver.account_managers ADD CONSTRAINT pk_silver_account_managers PRIMARY KEY (am_id);

-- ---------------------------------------------------------------- advertisers (Type 1 attributes only — AM assignment is versioned separately below)
DROP TABLE IF EXISTS silver.advertisers;
CREATE TABLE silver.advertisers AS
SELECT
    advertiser_id,
    NULLIF(TRIM(advertiser_name), '') AS advertiser_name,
    NULLIF(TRIM(vertical), '')        AS vertical,
    NULLIF(LOWER(TRIM(status)), '')   AS status,
    silver.canonical_country(billing_country) AS billing_country,
    silver.parse_flexible_date(signup_date)   AS signup_date
FROM bronze.advertisers_raw;

ALTER TABLE silver.advertisers ADD CONSTRAINT pk_silver_advertisers PRIMARY KEY (advertiser_id);

-- ---------------------------------------------------------------- advertiser_am_history
-- Exploded DIRECTLY from MongoDB's account_manager_history[] — the only
-- source of AM-assignment history (Q1 §4 / Q2 A.4). This is the Type-2
-- source of truth for dim_advertiser's Gold load. `to` is date-only and NULL
-- for the current (open-ended) entry; parse_flexible_date leaves it NULL
-- and the Gold load applies the DATE '9999-12-31' sentinel (Q2 change log).
DROP TABLE IF EXISTS silver.advertiser_am_history;
CREATE TABLE silver.advertiser_am_history AS
SELECT
    m.advertiser_id,
    h->>'am_id'   AS am_id,
    h->>'am_name' AS am_name,
    (silver.parse_flexible_date(h->>'from'))::DATE AS effective_from,
    (silver.parse_flexible_date(h->>'to'))::DATE   AS effective_to_raw   -- NULL = current; sentinel applied at Gold load
FROM bronze.mongo_advertiser_accounts m,
     jsonb_array_elements(m.raw_doc->'account_manager_history') h;

CREATE INDEX ix_silver_am_history_advertiser ON silver.advertiser_am_history (advertiser_id);

-- ---------------------------------------------------------------- partners (merged: partners_raw primary, MongoDB fills the 40 missing prefixes — Q2 A.5)
DROP TABLE IF EXISTS silver.partners;
CREATE TABLE silver.partners AS
SELECT
    COALESCE(p.partner_id, m.partner_id) AS partner_id,
    CASE WHEN p.partner_id IS NOT NULL THEN NULLIF(TRIM(p.partner_name), '')
         ELSE NULLIF(TRIM(m.raw_doc->>'name'), '') END AS partner_name,
    CASE WHEN p.partner_id IS NOT NULL THEN NULLIF(LOWER(TRIM(p.channel)), '')
         ELSE LOWER(m.raw_doc->'channels'->>0) END AS primary_channel,
    CASE WHEN m.raw_doc IS NOT NULL THEN
        ARRAY(SELECT jsonb_array_elements_text(m.raw_doc->'channels'))
    END AS channels,
    NULLIF(TRIM(p.media_buyer), '') AS media_buyer,
    COALESCE(silver.parse_flexible_date(p.join_date), silver.parse_mongo_date(m.raw_doc->'joinedAt')) AS join_date,
    COALESCE(NULLIF(TRIM(p.payment_terms), ''), NULLIF(TRIM(m.raw_doc->'payment'->>'terms'), '')) AS payment_terms,
    CASE WHEN p.partner_id IS NOT NULL THEN 'relational' ELSE 'mongodb' END AS source_system
FROM bronze.partners_raw p
FULL OUTER JOIN bronze.mongo_partner_profiles m ON m.partner_id = p.partner_id;

ALTER TABLE silver.partners ADD CONSTRAINT pk_silver_partners PRIMARY KEY (partner_id);

-- ---------------------------------------------------------------- offers
DROP TABLE IF EXISTS silver.offers;
CREATE TABLE silver.offers AS
SELECT
    offer_id,
    NULLIF(TRIM(offer_name), '') AS offer_name,
    advertiser_id,
    NULLIF(TRIM(vertical), '') AS vertical,
    silver.parse_money(default_payout) AS default_payout,
    NULLIF(LOWER(TRIM(payout_type)), '') AS payout_type
FROM bronze.offers_raw;

ALTER TABLE silver.offers ADD CONSTRAINT pk_silver_offers PRIMARY KEY (offer_id);

-- ---------------------------------------------------------------- campaigns
-- Conflict resolution (Q2 A.2/B.3): where campaigns_raw.advertiser_id
-- disagrees with its offer's advertiser_id, THE OFFER'S ADVERTISER WINS.
-- Conflicting rows are still loaded (with the corrected advertiser_id) —
-- never dropped — and separately logged to audit.quarantine as a corrected
-- record, not a rejected one.
DROP TABLE IF EXISTS silver.campaigns;
CREATE TABLE silver.campaigns AS
SELECT
    c.campaign_id,
    NULLIF(TRIM(c.campaign_name), '') AS campaign_name,
    c.offer_id,
    COALESCE(o.advertiser_id, c.advertiser_id) AS advertiser_id,   -- offer's advertiser wins on conflict
    (c.advertiser_id IS DISTINCT FROM o.advertiser_id) AS advertiser_id_was_corrected,
    NULLIF(UPPER(TRIM(c.price_type)), '') AS price_type,
    silver.parse_flexible_date(c.start_date) AS start_date,
    silver.parse_flexible_date(c.end_date)   AS end_date
FROM bronze.campaigns_raw c
LEFT JOIN silver.offers o ON o.offer_id = c.offer_id;

ALTER TABLE silver.campaigns ADD CONSTRAINT pk_silver_campaigns PRIMARY KEY (campaign_id);

-- Idempotent (Q5/Q6 pattern): NOT EXISTS guard — this table is DROP+rebuilt
-- every run (a full Silver recompute), which would otherwise re-log the
-- same 12 corrections on every single rerun (caught by Q6's reconciliation
-- run: the count doubled from 12 to 24 on a no-op second pass before this
-- guard was added).
INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'silver_load', 'campaigns_raw', c.campaign_id,
       'advertiser_id conflicted with offer''s advertiser — corrected to offer''s advertiser, row still loaded',
       c.campaign_id || ' offer_id=' || c.offer_id
FROM silver.campaigns c
WHERE c.advertiser_id_was_corrected
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'campaigns_raw' AND q.source_pk = c.campaign_id);

-- ---------------------------------------------------------------- creatives
DROP TABLE IF EXISTS silver.creatives;
CREATE TABLE silver.creatives AS
SELECT
    creative_id,
    campaign_id,
    NULLIF(TRIM(creative_name), '') AS creative_name,
    NULLIF(LOWER(TRIM(format)), '') AS format,
    NULLIF(TRIM(size), '') AS size,
    NULLIF(TRIM(landing_url), '') AS landing_url
FROM bronze.creatives_raw;

ALTER TABLE silver.creatives ADD CONSTRAINT pk_silver_creatives PRIMARY KEY (creative_id);
