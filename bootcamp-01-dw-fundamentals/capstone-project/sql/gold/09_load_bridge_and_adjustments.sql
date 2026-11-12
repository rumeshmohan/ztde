-- gold.bridge_conversion_touchpoint / gold.fact_revenue_adjustment
-- (initial bulk load — see 10_apply_new_revenue_adjustments.sql for the
-- ongoing, incremental adjustment path used after this first run).
-- Idempotent (Q5): ON CONFLICT DO NOTHING + NOT EXISTS guards on every
-- quarantine insert, so recovery after a partial failure never errors or
-- duplicates. Rows whose conversion or click was itself excluded from Gold
-- (null conversion_time / click_time — see 07/08's quarantine entries)
-- cannot join here either; logged explicitly rather than silently
-- vanishing from the touchpoint count between Silver and Gold.

INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'gold_load', 'silver.conversion_touchpoints', t.touchpoint_id,
       'referenced conversion or click was excluded from Gold (unparseable date)', t.touchpoint_id
FROM silver.conversion_touchpoints t
WHERE (NOT EXISTS (SELECT 1 FROM gold.fact_conversion fc WHERE fc.conversion_id = t.conversion_id)
    OR NOT EXISTS (SELECT 1 FROM gold.fact_click fcl WHERE fcl.click_id = t.click_id))
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'silver.conversion_touchpoints' AND q.source_pk = t.touchpoint_id);

INSERT INTO gold.bridge_conversion_touchpoint (touchpoint_id, conversion_key, click_key, touch_position, weight)
SELECT t.touchpoint_id, fc.conversion_key, fcl.click_key, t.touch_position, t.weight
FROM silver.conversion_touchpoints t
JOIN gold.fact_conversion fc ON fc.conversion_id = t.conversion_id
JOIN gold.fact_click fcl ON fcl.click_id = t.click_id
ON CONFLICT (touchpoint_id) DO NOTHING;

-- fact_revenue_adjustment: recover blank delta_amount from the stream
-- (Q1 §6 / silver.stream_recovered_adjustment_totals), applied as one
-- recovered adjustment row per conversion when the batch value is blank —
-- visible and traceable (reason = 'recovered from stream'), not silently
-- folded into the original row.
INSERT INTO gold.fact_revenue_adjustment (adjustment_id, date_key, conversion_key, delta_amount, reason, adjusted_at)
SELECT
    a.adjustment_id,
    TO_CHAR(a.adjusted_at, 'YYYYMMDD')::INT,
    fc.conversion_key,
    COALESCE(a.delta_amount, r.recovered_delta_total),
    CASE WHEN a.delta_amount IS NULL AND r.recovered_delta_total IS NOT NULL
         THEN COALESCE(a.reason, '') || ' [amount recovered from stream]'
         ELSE a.reason END,
    a.adjusted_at
FROM silver.revenue_adjustments a
JOIN gold.fact_conversion fc ON fc.conversion_id = a.conversion_id
LEFT JOIN silver.stream_recovered_adjustment_totals r ON r.conversion_id = a.conversion_id
WHERE a.adjusted_at IS NOT NULL
ON CONFLICT (adjustment_id) DO NOTHING;

INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'gold_load', 'silver.revenue_adjustments', a.adjustment_id,
       'unparseable/blank adjusted_at — cannot assign a date_key', a.adjustment_id
FROM silver.revenue_adjustments a
WHERE a.adjusted_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'silver.revenue_adjustments' AND q.source_pk = a.adjustment_id AND q.reason LIKE 'unparseable%');

INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'gold_load', 'silver.revenue_adjustments', a.adjustment_id,
       'referenced conversion was excluded from Gold (unparseable conversion_time)', a.adjustment_id
FROM silver.revenue_adjustments a
WHERE a.adjusted_at IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM gold.fact_conversion fc WHERE fc.conversion_id = a.conversion_id)
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'silver.revenue_adjustments' AND q.source_pk = a.adjustment_id AND q.reason LIKE 'referenced%');
