-- ============================================================================
-- PulseTrack — Data Quality Checks (Q6)
-- Every check below is a real, runnable query against the live warehouse —
-- not a described intention. Grouped exactly per the brief's own categories.
-- Each check returns (check_name, scope, result) so this file can be run as
-- a single script and read top-to-bottom as a quality report.
-- ============================================================================

-- ---------------------------------------------------------------- 1. Schema, null, data-type and malformed-record checks

-- 1a. Bronze -> Silver parse failure rate per date/money column (should be
-- at or near the counts already known from Q4's build; a NEW spike here
-- would indicate a regression in silver.parse_flexible_date/parse_money).
SELECT '1a_date_parse_failures' AS check_name, 'conversions.conversion_time' AS scope,
       COUNT(*) FILTER (WHERE conversion_time IS NOT NULL AND TRIM(conversion_time)<>''
                          AND silver.parse_flexible_date(conversion_time) IS NULL) AS result
FROM bronze.conversions_raw
UNION ALL
SELECT '1a_date_parse_failures', 'impressions.impression_time',
       COUNT(*) FILTER (WHERE impression_time IS NOT NULL AND TRIM(impression_time)<>''
                          AND silver.parse_flexible_date(impression_time) IS NULL)
FROM bronze.impressions_raw
UNION ALL
SELECT '1a_date_parse_failures', 'clicks.click_time',
       COUNT(*) FILTER (WHERE click_time IS NOT NULL AND TRIM(click_time)<>''
                          AND silver.parse_flexible_date(click_time) IS NULL)
FROM bronze.clicks_raw
UNION ALL
SELECT '1a_date_parse_failures', 'revenue_adjustments.adjusted_at',
       COUNT(*) FILTER (WHERE adjusted_at IS NOT NULL AND TRIM(adjusted_at)<>''
                          AND silver.parse_flexible_date(adjusted_at) IS NULL)
FROM bronze.revenue_adjustments_raw
UNION ALL
SELECT '1b_money_parse_failures', 'conversions.revenue',
       COUNT(*) FILTER (WHERE revenue IS NOT NULL AND TRIM(revenue)<>'' AND UPPER(TRIM(revenue))<>'N/A'
                          AND silver.parse_money(revenue) IS NULL)
FROM bronze.conversions_raw
UNION ALL
SELECT '1b_money_parse_failures', 'conversions.payout',
       COUNT(*) FILTER (WHERE payout IS NOT NULL AND TRIM(payout)<>'' AND UPPER(TRIM(payout))<>'N/A'
                          AND silver.parse_money(payout) IS NULL)
FROM bronze.conversions_raw
UNION ALL
SELECT '1b_money_parse_failures', 'revenue_adjustments.delta_amount',
       COUNT(*) FILTER (WHERE delta_amount IS NOT NULL AND TRIM(delta_amount)<>'' AND UPPER(TRIM(delta_amount))<>'N/A'
                          AND silver.parse_money(delta_amount) IS NULL)
FROM bronze.revenue_adjustments_raw
UNION ALL
-- 1c. Malformed stream lines (already quarantined at ingestion — restated
-- here as a live check, not just a one-time ingestion-log fact).
SELECT '1c_malformed_stream_lines', 'stream_events', COUNT(*)
FROM bronze.stream_events WHERE is_malformed
UNION ALL
-- 1d. Nulls in NOT-nullable business-key columns (schema-level integrity —
-- would indicate a genuinely broken source row, not expected messiness).
SELECT '1d_null_business_key', 'conversions_raw.conversion_id', COUNT(*)
FROM bronze.conversions_raw WHERE conversion_id IS NULL OR TRIM(conversion_id) = ''
UNION ALL
SELECT '1d_null_business_key', 'advertisers_raw.advertiser_id', COUNT(*)
FROM bronze.advertisers_raw WHERE advertiser_id IS NULL OR TRIM(advertiser_id) = ''

ORDER BY 1, 2;


-- ---------------------------------------------------------------- 2. Duplicate detection

SELECT '2a_exact_duplicate_rows' AS check_name, 'conversions_raw' AS scope,
       COUNT(*) - COUNT(DISTINCT (conversion_id, click_id, campaign_id, revenue, payout, status, updated_at)) AS result
FROM bronze.conversions_raw
UNION ALL
SELECT '2b_lifecycle_snapshot_ids', 'conversions_raw (ids with >1 row)',
       (SELECT COUNT(*) FROM (SELECT conversion_id FROM bronze.conversions_raw GROUP BY conversion_id HAVING COUNT(*) > 1) t)
UNION ALL
SELECT '2c_duplicate_natural_keys', 'account_managers_raw.am_id',
       (SELECT COUNT(*) FROM (SELECT am_id FROM bronze.account_managers_raw GROUP BY am_id HAVING COUNT(*) > 1) t)
UNION ALL
SELECT '2c_duplicate_natural_keys', 'advertisers_raw.advertiser_id',
       (SELECT COUNT(*) FROM (SELECT advertiser_id FROM bronze.advertisers_raw GROUP BY advertiser_id HAVING COUNT(*) > 1) t)
UNION ALL
SELECT '2c_duplicate_natural_keys', 'partners_raw.partner_id',
       (SELECT COUNT(*) FROM (SELECT partner_id FROM bronze.partners_raw GROUP BY partner_id HAVING COUNT(*) > 1) t)
UNION ALL
SELECT '2c_duplicate_natural_keys', 'gold.fact_conversion.conversion_id (must be zero — PK-enforced)',
       (SELECT COUNT(*) FROM (SELECT conversion_id FROM gold.fact_conversion GROUP BY conversion_id HAVING COUNT(*) > 1) t)
UNION ALL
SELECT '2d_duplicate_stream_events', 'stream_events (event_id seen >1x, at-least-once delivery)',
       (SELECT COUNT(*) FROM (SELECT event_id FROM bronze.stream_events WHERE NOT is_malformed AND event_id IS NOT NULL GROUP BY event_id HAVING COUNT(*) > 1) t)

ORDER BY 1, 2;


-- ---------------------------------------------------------------- 3. Referential integrity (every named relationship pair)

SELECT '3_orphan_check' AS check_name, 'offer -> advertiser' AS scope, COUNT(*) AS result
FROM silver.offers o WHERE NOT EXISTS (SELECT 1 FROM silver.advertisers a WHERE a.advertiser_id = o.advertiser_id)
UNION ALL
SELECT '3_orphan_check', 'campaign -> offer',
       (SELECT COUNT(*) FROM silver.campaigns c WHERE NOT EXISTS (SELECT 1 FROM silver.offers o WHERE o.offer_id = c.offer_id))
UNION ALL
SELECT '3_orphan_check', 'campaign -> advertiser (post-correction — must be zero)',
       (SELECT COUNT(*) FROM silver.campaigns c WHERE NOT EXISTS (SELECT 1 FROM silver.advertisers a WHERE a.advertiser_id = c.advertiser_id))
UNION ALL
SELECT '3_orphan_check', 'creative -> campaign',
       (SELECT COUNT(*) FROM silver.creatives cr WHERE NOT EXISTS (SELECT 1 FROM silver.campaigns c WHERE c.campaign_id = cr.campaign_id))
UNION ALL
SELECT '3_orphan_check', 'impression -> campaign',
       (SELECT COUNT(*) FROM silver.impressions i WHERE NOT EXISTS (SELECT 1 FROM silver.campaigns c WHERE c.campaign_id = i.campaign_id))
UNION ALL
SELECT '3_orphan_check', 'impression -> creative (blank = campaign w/o creative, expected ~5%)',
       (SELECT COUNT(*) FROM silver.impressions i WHERE i.creative_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM silver.creatives cr WHERE cr.creative_id = i.creative_id))
UNION ALL
SELECT '3_orphan_check', 'impression -> partner (unresolvable prefix, expected)',
       (SELECT COUNT(*) FROM silver.impressions i WHERE i.partner_prefix IS NOT NULL AND NOT EXISTS (SELECT 1 FROM silver.partners p WHERE p.partner_id = i.partner_prefix))
UNION ALL
SELECT '3_orphan_check', 'click -> impression (nullable = direct click, expected ~15%)',
       (SELECT COUNT(*) FROM silver.clicks c WHERE c.impression_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM silver.impressions i WHERE i.impression_id = c.impression_id))
UNION ALL
SELECT '3_orphan_check', 'click -> campaign',
       (SELECT COUNT(*) FROM silver.clicks c WHERE NOT EXISTS (SELECT 1 FROM silver.campaigns cm WHERE cm.campaign_id = c.campaign_id))
UNION ALL
SELECT '3_orphan_check', 'click -> partner (unresolvable prefix, expected)',
       (SELECT COUNT(*) FROM silver.clicks c WHERE c.partner_prefix IS NOT NULL AND NOT EXISTS (SELECT 1 FROM silver.partners p WHERE p.partner_id = c.partner_prefix))
UNION ALL
SELECT '3_orphan_check', 'conversion -> click (nullable, expected ~10%)',
       (SELECT COUNT(*) FROM silver.conversions sc WHERE sc.click_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM silver.clicks c WHERE c.click_id = sc.click_id))
UNION ALL
SELECT '3_orphan_check', 'conversion -> campaign (must be zero — Silver join already resolves this)',
       (SELECT COUNT(*) FROM silver.conversions sc WHERE NOT EXISTS (SELECT 1 FROM silver.campaigns c WHERE c.campaign_id = sc.campaign_id))
UNION ALL
SELECT '3_orphan_check', 'conversion -> partner (unresolvable prefix, expected)',
       (SELECT COUNT(*) FROM silver.conversions sc WHERE sc.partner_prefix IS NOT NULL AND NOT EXISTS (SELECT 1 FROM silver.partners p WHERE p.partner_id = sc.partner_prefix))
UNION ALL
SELECT '3_orphan_check', 'touchpoint -> conversion (must be zero — filtered at Silver load)',
       (SELECT COUNT(*) FROM silver.conversion_touchpoints t WHERE NOT EXISTS (SELECT 1 FROM silver.conversions c WHERE c.conversion_id = t.conversion_id))
UNION ALL
SELECT '3_orphan_check', 'touchpoint -> click (must be zero — filtered at Silver load)',
       (SELECT COUNT(*) FROM silver.conversion_touchpoints t WHERE NOT EXISTS (SELECT 1 FROM silver.clicks c WHERE c.click_id = t.click_id))
UNION ALL
SELECT '3_orphan_check', 'revenue_adjustment -> conversion (must be zero — filtered at Silver load)',
       (SELECT COUNT(*) FROM silver.revenue_adjustments a WHERE NOT EXISTS (SELECT 1 FROM silver.conversions c WHERE c.conversion_id = a.conversion_id))
UNION ALL
SELECT '3_gold_fk_check', 'fact_conversion rows resolving to -1 Unknown (any dimension)',
       (SELECT COUNT(*) FROM gold.fact_conversion
        WHERE advertiser_key = -1 OR account_manager_key = -1 OR campaign_key = -1
           OR creative_key = -1 OR partner_key = -1)

ORDER BY 1, 2;


-- ---------------------------------------------------------------- 4. Valid dates, monetary values and statuses

SELECT '4a_future_dated_conversion' AS check_name, 'conversion_time > today' AS scope,
       COUNT(*) AS result
FROM silver.conversions WHERE conversion_time > CURRENT_DATE + INTERVAL '1 day'
UNION ALL
SELECT '4b_negative_revenue', 'conversions.revenue < 0 (a real, documented anomaly class — not rejected, just counted)',
       (SELECT COUNT(*) FROM silver.conversions WHERE revenue < 0)
UNION ALL
SELECT '4c_payout_exceeds_revenue', 'conversions where payout > revenue (documented anomaly class)',
       (SELECT COUNT(*) FROM silver.conversions WHERE payout > revenue)
UNION ALL
SELECT '4d_invalid_status', 'conversions.status = ''unknown'' after canonicalisation',
       (SELECT COUNT(*) FROM silver.conversions WHERE status = 'unknown')
UNION ALL
SELECT '4e_non_finite_weight', 'conversion_touchpoints.weight NULL/negative/>1',
       (SELECT COUNT(*) FROM silver.conversion_touchpoints WHERE weight IS NULL OR weight < 0 OR weight > 1)

ORDER BY 1;


-- ---------------------------------------------------------------- 5. Business-rule and event-sequence validation

-- 5a. is_test rows must never appear past Silver (a business rule, not a
-- referential check) — proves the filter in 03_load_conversions.sql holds.
SELECT '5a_test_rows_past_silver' AS check_name, 'silver.conversions' AS scope, COUNT(*) AS result
FROM silver.conversions sc
JOIN bronze.conversions_raw c ON c.conversion_id = sc.conversion_id AND silver.is_truthy(c.is_test)
UNION ALL
-- 5b. Event-sequence: a click should not occur before its linked impression.
-- The data guide explicitly documents that row-level funnel chronology is
-- NOT guaranteed ("teaches set-level joins, not sessionization") — this is
-- measured and reported as a known characteristic, not treated as a defect.
SELECT '5b_click_before_impression (known non-guarantee, per data guide)', 'clicks linked to an impression',
       (SELECT COUNT(*) FROM silver.clicks c JOIN silver.impressions i ON i.impression_id = c.impression_id
        WHERE c.click_time < i.impression_time)
UNION ALL
-- 5c. Event-sequence: a revenue adjustment should never precede its own
-- conversion (this one SHOULD always hold — it's a real ordering
-- constraint, not a documented non-guarantee).
SELECT '5c_adjustment_before_conversion (should always be zero)', 'revenue_adjustments',
       (SELECT COUNT(*) FROM silver.revenue_adjustments a JOIN silver.conversions c ON c.conversion_id = a.conversion_id
        WHERE a.adjusted_at < c.conversion_time)
UNION ALL
-- 5d. Event-sequence: a touchpoint's click occurring after its conversion.
-- NOTE: this is NOT a defect, verified — 83,093/176,009 touchpoints (47.2%)
-- show this, with gaps up to 530+ days, which is exactly the data guide's
-- documented "row-level funnel chronology is NOT guaranteed... teaches
-- set-level joins, not sessionization" characteristic already known from
-- 5b, extended to touchpoints. Measured and reported, not treated as an
-- error to fix (an earlier version of this check was labelled "should
-- always be zero" — that label, not the data, was wrong).
SELECT '5d_touchpoint_click_after_conversion (known non-guarantee, per data guide — see 5b)', 'conversion_touchpoints',
       (SELECT COUNT(*) FROM silver.conversion_touchpoints t
        JOIN silver.conversions c ON c.conversion_id = t.conversion_id
        JOIN silver.clicks cl ON cl.click_id = t.click_id
        WHERE cl.click_time > c.conversion_time)

ORDER BY 1;


-- ---------------------------------------------------------------- 6. Attribution validation

-- 6a. Touchpoint weights must sum to ≈1 per conversion (Q2's documented
-- ~3-decimal rounding drift — tolerance 0.01, not exact equality).
SELECT '6a_touchpoint_weight_sum_out_of_tolerance' AS check_name,
       'conversions with |SUM(weight) - 1| > 0.01' AS scope,
       COUNT(*) AS result
FROM (
    SELECT conversion_id, SUM(weight) AS total_weight
    FROM silver.conversion_touchpoints
    GROUP BY conversion_id
) t
WHERE ABS(total_weight - 1) > 0.01
UNION ALL
-- 6b. Attributed revenue must never exceed a conversion's own revenue IN
-- MAGNITUDE (compare ABS(), not signed >: a negative-revenue conversion
-- with weight_sum=0.999 makes attributed revenue LESS negative, i.e.
-- numerically "greater than" revenue while still being under-attribution
-- in business terms — a signed comparison here flagged 430 false
-- positives, all of this exact shape, before being corrected to compare
-- magnitudes).
SELECT '6b_attributed_revenue_exceeds_conversion_revenue', 'conversions',
       (SELECT COUNT(*) FROM (
            SELECT t.conversion_id, c.revenue, SUM(t.weight * c.revenue) AS attributed
            FROM silver.conversion_touchpoints t JOIN silver.conversions c ON c.conversion_id = t.conversion_id
            GROUP BY t.conversion_id, c.revenue
        ) x WHERE ABS(attributed) > ABS(revenue) + 0.01)

ORDER BY 1;
