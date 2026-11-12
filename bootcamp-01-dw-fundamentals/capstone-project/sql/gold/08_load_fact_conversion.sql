-- gold.fact_conversion — the historically-resolved load (Q2 B.4). Each
-- conversion joins dim_advertiser on advertiser_id AND
-- conversion_time::date BETWEEN effective_from AND effective_to — the
-- plain-BETWEEN pattern the sentinel-date reconciliation (Q2 change log)
-- exists specifically to enable. account_manager_key comes DIRECTLY from
-- that same matched row (no second lookup) so it is, by construction,
-- always the AM who owned the advertiser at conversion time.
--
-- 2,599 conversions have no parseable conversion_time (genuinely blank in
-- source — verified, not a parser gap) and are quarantined: no date_key is
-- possible, so no fact row is possible.
-- creative_key is resolved via the linked click (Q2 B.4: creative is not on
-- conversions_raw directly) — NULL/unresolved click_id correctly yields -1
-- Unknown, matching the ~10% "unattributable" gap flagged in Q1 §6/Q2.

-- Idempotent (Q5): NOT EXISTS on the quarantine inserts and ON CONFLICT DO
-- NOTHING on the fact insert (below) mean this script can be safely
-- re-run after a partial failure — rows already loaded are left exactly as
-- they are (including any adjustment_delta_total already applied by
-- 10_apply_new_revenue_adjustments.sql — an ON CONFLICT DO UPDATE here
-- would wrongly reset that back to 0).
INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'gold_load', 'silver.conversions', sc.conversion_id,
       'unparseable/blank conversion_time — cannot assign a date_key or resolve historical advertiser version',
       sc.conversion_id
FROM silver.conversions sc
WHERE sc.conversion_time IS NULL
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'silver.conversions' AND q.source_pk = sc.conversion_id);

INSERT INTO gold.fact_conversion
    (conversion_id, date_key, advertiser_key, account_manager_key, campaign_key,
     creative_key, partner_key, click_id, status, sub_id, country, device,
     conversion_time, revenue, payout, adjustment_delta_total, source_updated_at)
SELECT
    sc.conversion_id,
    TO_CHAR(sc.conversion_time, 'YYYYMMDD')::INT,
    COALESCE(da.advertiser_key, -1),
    COALESCE(da.account_manager_key, -1),
    COALESCE(cmp.campaign_key, -1),
    COALESCE(cre.creative_key, -1),
    COALESCE(par.partner_key, -1),
    sc.click_id, sc.status, sc.sub_id, sc.country, sc.device,
    sc.conversion_time,
    COALESCE(sc.revenue, 0),
    COALESCE(sc.payout, 0),
    0,   -- adjustment_delta_total: NOT populated by 09's initial fact_revenue_adjustment
         -- load (that inserts adjustment rows, nothing more) — only
         -- 10_apply_new_revenue_adjustments.sql's recompute rolls this up.
         -- That script must run after every load of this table, not just once
         -- (Q5 §3: this exact gap meant 9,594 conversions had an understated
         -- adjusted_revenue until it was caught and run).
    sc.source_updated_at
FROM silver.conversions sc
LEFT JOIN gold.dim_advertiser da
    ON da.advertiser_id = sc.advertiser_id
   AND sc.conversion_time::DATE BETWEEN da.effective_from AND da.effective_to
   AND da.advertiser_key <> -1
LEFT JOIN gold.dim_campaign cmp ON cmp.campaign_id = sc.campaign_id
LEFT JOIN silver.clicks scl ON scl.click_id = sc.click_id
LEFT JOIN gold.dim_creative cre ON cre.creative_id = scl.creative_id
LEFT JOIN gold.dim_partner par ON par.partner_id = sc.partner_prefix
WHERE sc.conversion_time IS NOT NULL
ON CONFLICT (conversion_id) DO NOTHING;

-- Anything that fell through to the -1 Unknown advertiser DESPITE having a
-- real advertiser_id is a genuine gap in the SCD history (e.g. a conversion
-- timestamped before the earliest history entry) — worth a visible flag,
-- not just a silent -1.
INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'gold_load', 'fact_conversion', fc.conversion_id,
       'advertiser_id has no dim_advertiser version covering conversion_time — check for a gap in account_manager_history',
       fc.conversion_id
FROM gold.fact_conversion fc
JOIN silver.conversions sc ON sc.conversion_id = fc.conversion_id
WHERE fc.advertiser_key = -1
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'fact_conversion' AND q.source_pk = fc.conversion_id);
