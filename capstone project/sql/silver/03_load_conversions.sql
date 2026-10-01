-- ============================================================================
-- PulseTrack — Silver: conversions (the dirty, unnormalised centrepiece)
-- Grain here = the deduplicated business conversion (Q1 §2 / Q2 A.2):
-- ROW_NUMBER() PARTITION BY conversion_id ORDER BY updated_at DESC — the
-- Week 08 dedup idiom, translated through silver.parse_flexible_date since
-- updated_at is messy multi-format text here rather than already a
-- TIMESTAMP column. is_test rows are filtered out entirely (never reach
-- Silver, let alone Gold). advertiser_id is resolved via campaign_id ->
-- silver.campaigns, NOT via the embedded (redundant, inconsistent-casing)
-- advertiser_name — reconciling by id is the whole point (Q2 A.3).
-- ============================================================================

DROP TABLE IF EXISTS silver.conversions;
CREATE TABLE silver.conversions AS
WITH deduped AS (
    SELECT
        c.*,
        silver.parse_flexible_date(c.updated_at) AS updated_at_parsed,
        silver.parse_flexible_date(c.created_at) AS created_at_parsed,
        ROW_NUMBER() OVER (
            PARTITION BY c.conversion_id
            ORDER BY silver.parse_flexible_date(c.updated_at) DESC NULLS LAST,
                     silver.parse_flexible_date(c.created_at) DESC NULLS LAST
        ) AS rn
    FROM bronze.conversions_raw c
    WHERE NOT silver.is_truthy(c.is_test)
)
SELECT
    d.conversion_id,
    NULLIF(TRIM(d.click_id), '') AS click_id,
    d.campaign_id,
    cmp.advertiser_id,                                   -- resolved via campaign, not the embedded name
    NULLIF(TRIM(d.account_manager), '') AS embedded_account_manager,  -- kept for the Q6 reconciliation check, not a join key
    silver.canonical_partner_prefix(d.partner_id) AS partner_prefix,
    silver.parse_flexible_date(d.conversion_time) AS conversion_time,
    silver.parse_money(d.revenue) AS revenue,
    silver.parse_money(d.payout)  AS payout,
    silver.canonical_conversion_status(d.status) AS status,
    silver.canonical_country(d.country) AS country,
    silver.canonical_device(d.device) AS device,
    NULLIF(TRIM(d.sub_id), '') AS sub_id,
    d.updated_at_parsed AS source_updated_at
FROM deduped d
LEFT JOIN silver.campaigns cmp ON cmp.campaign_id = d.campaign_id
WHERE d.rn = 1;

ALTER TABLE silver.conversions ADD CONSTRAINT pk_silver_conversions PRIMARY KEY (conversion_id);
CREATE INDEX ix_silver_conversions_campaign ON silver.conversions (campaign_id);
CREATE INDEX ix_silver_conversions_advertiser ON silver.conversions (advertiser_id);
