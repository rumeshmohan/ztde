-- ============================================================================
-- PulseTrack — Silver: conversion_touchpoints (bridge) + revenue_adjustments
-- Touchpoints reference conversions by conversion_id; if a conversion was
-- filtered out as is_test (or its raw rows were ALL test — 3,665 ids, see
-- Q4 verification), its touchpoints must not silently orphan-join into
-- Gold — filtered to match silver.conversions exactly.
-- revenue_adjustments: the 6.0% orphaned (Q2 A.2) are quarantined, not
-- loaded with a guessed link.
-- ============================================================================

DROP TABLE IF EXISTS silver.conversion_touchpoints;
CREATE TABLE silver.conversion_touchpoints AS
SELECT
    t.touchpoint_id,
    t.conversion_id,
    t.click_id,
    NULLIF(LOWER(TRIM(t.touch_position)), '') AS touch_position,
    silver.parse_money(t.weight) AS weight   -- reuses the money parser: same "strip to numeric" shape
FROM bronze.conversion_touchpoints_raw t
WHERE EXISTS (SELECT 1 FROM silver.conversions c WHERE c.conversion_id = t.conversion_id);

-- Idempotent (Q6): NOT EXISTS guard against quarantine's own source_pk — this
-- table is DROP+rebuilt every run, and without the guard this insert
-- re-logs the same ~7,520 rows every time (caught by Q6's reconciliation
-- run: count doubled 7,520 -> 15,040 on a no-op second pass).
INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'silver_load', 'conversion_touchpoints_raw', t.touchpoint_id,
       'conversion_id belongs to an is_test conversion filtered out of silver.conversions',
       t.conversion_id
FROM bronze.conversion_touchpoints_raw t
WHERE NOT EXISTS (SELECT 1 FROM silver.conversions c WHERE c.conversion_id = t.conversion_id)
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'conversion_touchpoints_raw' AND q.source_pk = t.touchpoint_id);

ALTER TABLE silver.conversion_touchpoints ADD CONSTRAINT pk_silver_touchpoints PRIMARY KEY (touchpoint_id);
CREATE INDEX ix_silver_touchpoints_conversion ON silver.conversion_touchpoints (conversion_id);
CREATE INDEX ix_silver_touchpoints_click ON silver.conversion_touchpoints (click_id);

-- ---------------------------------------------------------------- revenue_adjustments
DROP TABLE IF EXISTS silver.revenue_adjustments;
CREATE TABLE silver.revenue_adjustments AS
SELECT
    a.adjustment_id,
    a.conversion_id,
    silver.parse_money(a.delta_amount) AS delta_amount,
    NULLIF(TRIM(a.reason), '') AS reason,
    silver.parse_flexible_date(a.adjusted_at) AS adjusted_at
FROM bronze.revenue_adjustments_raw a
WHERE EXISTS (SELECT 1 FROM silver.conversions c WHERE c.conversion_id = a.conversion_id);

INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'silver_load', 'revenue_adjustments_raw', a.adjustment_id,
       'conversion_id not found in silver.conversions (orphaned or is_test) — Q2 A.2 measured ~6% orphan rate',
       a.conversion_id
FROM bronze.revenue_adjustments_raw a
WHERE NOT EXISTS (SELECT 1 FROM silver.conversions c WHERE c.conversion_id = a.conversion_id)
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'revenue_adjustments_raw' AND q.source_pk = a.adjustment_id);

ALTER TABLE silver.revenue_adjustments ADD CONSTRAINT pk_silver_revenue_adjustments PRIMARY KEY (adjustment_id);
CREATE INDEX ix_silver_revenue_adjustments_conversion ON silver.revenue_adjustments (conversion_id);

-- ---------------------------------------------------------------- recover blank delta_amount from the stream
-- ~8% of batch delta_amount values are blank/N-A (Q1 §6: measured 7.9%); the
-- stream's adjustment.applied events carry the value the batch export
-- dropped. Recovered via event_key = conversion_id + matching adjustment
-- reason/timing is not reliable enough to pair 1:1 by adjustment_id (the
-- stream doesn't carry adjustment_id) — so this recovers a conversion-level
-- total from the stream where the batch total is entirely unknown, rather
-- than patching individual adjustment rows. Applied as a documented,
-- visible correction, not a silent guess.
CREATE TABLE IF NOT EXISTS silver.stream_recovered_adjustment_totals AS
SELECT
    s.event_key AS conversion_id,
    SUM(silver.parse_money((s.payload->>'delta_amount'))) AS recovered_delta_total,
    COUNT(*) AS stream_adjustment_events
FROM bronze.stream_events s
WHERE s.event_type = 'adjustment.applied'
  AND NOT s.is_malformed
  AND EXISTS (SELECT 1 FROM silver.conversions c WHERE c.conversion_id = s.event_key)
GROUP BY s.event_key;
