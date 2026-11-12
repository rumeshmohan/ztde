-- ============================================================================
-- PulseTrack — Data Mart population, FROM Gold ONLY (Q7)
-- Idempotent (same discipline as Q5/Q6): TRUNCATE + rebuild. The mart is a
-- derived, disposable layer — safe to fully recompute from Gold every run,
-- unlike Gold itself (which must preserve surrogate keys for historical
-- accuracy). No such constraint applies here, so the simplest correct
-- pattern is used rather than an incremental one.
-- ============================================================================

-- ---------------------------------------------------------------- dimensions (current state only, from Gold)

TRUNCATE mart.dim_advertiser, mart.dim_campaign, mart.dim_creative, mart.dim_partner, mart.dim_account_manager;

INSERT INTO mart.dim_advertiser VALUES ('UNKNOWN', 'Unknown Advertiser', NULL, NULL, NULL, NULL, NULL);
INSERT INTO mart.dim_advertiser
    (advertiser_id, advertiser_name, vertical, status, billing_country,
     current_account_manager_id, current_account_manager_name)
SELECT da.advertiser_id, da.advertiser_name, da.vertical, da.status, da.billing_country,
       am.am_id, am.am_name
FROM gold.dim_advertiser da
JOIN gold.dim_account_manager am ON am.am_key = da.account_manager_key
WHERE da.is_current AND da.advertiser_key <> -1;

INSERT INTO mart.dim_campaign VALUES ('UNKNOWN', 'Unknown Campaign', 'UNKNOWN', NULL, NULL, NULL, NULL, NULL);
INSERT INTO mart.dim_campaign (campaign_id, campaign_name, advertiser_id, advertiser_name, offer_name, price_type, start_date, end_date)
SELECT c.campaign_id, c.campaign_name, c.advertiser_id,
       COALESCE(a.advertiser_name, 'Unknown Advertiser'),
       c.offer_name, c.price_type, c.start_date, c.end_date
FROM gold.dim_campaign c
LEFT JOIN mart.dim_advertiser a ON a.advertiser_id = c.advertiser_id
WHERE c.campaign_key <> -1;

INSERT INTO mart.dim_creative VALUES ('UNKNOWN', 'Unknown Creative', 'UNKNOWN', NULL, NULL, NULL);
INSERT INTO mart.dim_creative (creative_id, creative_name, campaign_id, campaign_name, format, size)
SELECT cr.creative_id, cr.creative_name, cr.campaign_id,
       COALESCE(c.campaign_name, 'Unknown Campaign'), cr.format, cr.size
FROM gold.dim_creative cr
LEFT JOIN mart.dim_campaign c ON c.campaign_id = cr.campaign_id
WHERE cr.creative_key <> -1;

INSERT INTO mart.dim_partner VALUES ('UNKNOWN', 'Unknown Partner', NULL, NULL);
INSERT INTO mart.dim_partner (partner_id, partner_name, primary_channel, media_buyer)
SELECT partner_id, partner_name, primary_channel, media_buyer
FROM gold.dim_partner WHERE partner_key <> -1;

INSERT INTO mart.dim_account_manager VALUES ('UNKNOWN', 'Unknown Account Manager', NULL, NULL);
INSERT INTO mart.dim_account_manager (am_id, am_name, team, region)
SELECT am_id, am_name, team, region FROM gold.dim_account_manager WHERE am_key <> -1;

-- ---------------------------------------------------------------- fact_performance_daily
-- Built as three aggregates at the SAME grain (impressions, clicks,
-- conversions), UNION ALL'd and re-summed — the standard, correct way to
-- combine measures from facts with different native grains without a
-- fan-out join between them (impressions/clicks JOINED directly to
-- conversions would multiply rows; aggregating each side first, THEN
-- combining, is the fix — same principle as Q2 A.3's fanout warning).
-- account_manager_id is NULL for impression/click-sourced rows (Q2's bus
-- matrix: AM isn't resolvable without a conversion event) — deliberately
-- distinct from resolving to 'UNKNOWN', which would wrongly imply a data gap.

TRUNCATE mart.fact_performance_daily;

INSERT INTO mart.fact_performance_daily
    (date_key, advertiser_id, campaign_id, creative_id, partner_id, account_manager_id,
     impressions, clicks, conversions, revenue, payout, adjustment_delta_total, adjusted_revenue)
SELECT
    date_key, advertiser_id, campaign_id, creative_id, partner_id, account_manager_id,
    SUM(impressions), SUM(clicks), SUM(conversions),
    SUM(revenue), SUM(payout), SUM(adjustment_delta_total), SUM(adjusted_revenue)
FROM (
    -- impressions
    SELECT
        fi.date_key,
        c.advertiser_id, c.campaign_id, cr.creative_id, p.partner_id,
        NULL::VARCHAR(10) AS account_manager_id,
        fi.impression_count AS impressions, 0 AS clicks, 0 AS conversions,
        0::NUMERIC AS revenue, 0::NUMERIC AS payout, 0::NUMERIC AS adjustment_delta_total, 0::NUMERIC AS adjusted_revenue
    FROM gold.fact_impression fi
    JOIN gold.dim_campaign c ON c.campaign_key = fi.campaign_key
    JOIN gold.dim_creative cr ON cr.creative_key = fi.creative_key
    JOIN gold.dim_partner p ON p.partner_key = fi.partner_key

    UNION ALL

    -- clicks
    SELECT
        fc.date_key,
        c.advertiser_id, c.campaign_id, cr.creative_id, p.partner_id,
        NULL,
        0, fc.click_count, 0,
        0, 0, 0, 0
    FROM gold.fact_click fc
    JOIN gold.dim_campaign c ON c.campaign_key = fc.campaign_key
    JOIN gold.dim_creative cr ON cr.creative_key = fc.creative_key
    JOIN gold.dim_partner p ON p.partner_key = fc.partner_key

    UNION ALL

    -- conversions (the only source with a resolvable, historically-correct account_manager_id)
    SELECT
        fcv.date_key,
        da.advertiser_id, c.campaign_id, cr.creative_id, p.partner_id,
        am.am_id,
        0, 0, 1,
        fcv.revenue, fcv.payout, fcv.adjustment_delta_total, fcv.adjusted_revenue
    FROM gold.fact_conversion fcv
    JOIN gold.dim_advertiser da ON da.advertiser_key = fcv.advertiser_key
    JOIN gold.dim_campaign c ON c.campaign_key = fcv.campaign_key
    JOIN gold.dim_creative cr ON cr.creative_key = fcv.creative_key
    JOIN gold.dim_partner p ON p.partner_key = fcv.partner_key
    JOIN gold.dim_account_manager am ON am.am_key = fcv.account_manager_key
) combined
GROUP BY date_key, advertiser_id, campaign_id, creative_id, partner_id, account_manager_id;

-- ---------------------------------------------------------------- fact_attribution_daily
-- Multi-touch attribution, kept separate (see 01's header comment). Date
-- and campaign come from the CONVERSION (the business event being
-- attributed); partner comes from the CLICK (who gets the credit).
TRUNCATE mart.fact_attribution_daily;

INSERT INTO mart.fact_attribution_daily (date_key, partner_id, campaign_id, attributed_conversions, attributed_revenue, touch_count)
SELECT
    fcv.date_key,
    p.partner_id,
    c.campaign_id,
    SUM(bt.weight),
    SUM(bt.weight * fcv.revenue),
    COUNT(*)
FROM gold.bridge_conversion_touchpoint bt
JOIN gold.fact_conversion fcv ON fcv.conversion_key = bt.conversion_key
JOIN gold.fact_click fcl ON fcl.click_key = bt.click_key
JOIN gold.dim_partner p ON p.partner_key = fcl.partner_key
JOIN gold.dim_campaign c ON c.campaign_key = fcv.campaign_key
GROUP BY fcv.date_key, p.partner_id, c.campaign_id;
