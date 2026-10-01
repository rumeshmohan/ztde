-- ============================================================================
-- PulseTrack — Silver: funnel events (impressions, clicks)
-- Factless events; partner_id resolved via the canonical 5-digit prefix
-- (Q2 A.5). Junk columns (placement_raw/bot_score on impressions;
-- ip/user_agent/referrer_raw on clicks) are dropped here — Silver is where
-- junk stops, per Q3's security decision (PII never reaches Gold).
-- ============================================================================

DROP TABLE IF EXISTS silver.impressions;
CREATE TABLE silver.impressions AS
SELECT
    impression_id,
    campaign_id,
    NULLIF(TRIM(creative_id), '') AS creative_id,
    silver.canonical_partner_prefix(partner_id) AS partner_prefix,
    silver.parse_flexible_date(impression_time) AS impression_time,
    silver.canonical_country(country) AS country,
    silver.canonical_device(device) AS device
FROM bronze.impressions_raw;

ALTER TABLE silver.impressions ADD CONSTRAINT pk_silver_impressions PRIMARY KEY (impression_id);

DROP TABLE IF EXISTS silver.clicks;
CREATE TABLE silver.clicks AS
SELECT
    click_id,
    NULLIF(TRIM(impression_id), '') AS impression_id,   -- NULL = "direct" click, a real business state (~15%)
    campaign_id,
    NULLIF(TRIM(creative_id), '') AS creative_id,
    silver.canonical_partner_prefix(partner_id) AS partner_prefix,
    silver.parse_flexible_date(click_time) AS click_time,
    silver.canonical_country(country) AS country,
    silver.canonical_device(device) AS device
FROM bronze.clicks_raw;

ALTER TABLE silver.clicks ADD CONSTRAINT pk_silver_clicks PRIMARY KEY (click_id);
