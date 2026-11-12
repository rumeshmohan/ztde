-- gold: idempotent, INCREMENTAL revenue-adjustment application (Q5).
--
-- This is deliberately NOT the pattern the Q1/Q2 prose originally sketched
-- ("UPDATE ... SET adjustment_delta_total = adjustment_delta_total + delta").
-- That "+=" pattern is NOT safe under retries: if this step ever ran twice
-- for the same adjustment (a retried task, an at-least-once scheduler,
-- exactly the "duplicate delivery" scenario this question is about), the
-- delta would be double-counted. Caught while implementing Q5, not before —
-- fixed by making the update a RECOMPUTE from the source of truth
-- (fact_revenue_adjustment) rather than an increment: running this script
-- any number of times against the same data converges to the same total
-- every time, which is the actual definition of idempotent.
--
-- Step 1: insert only adjustments not already applied (adjustment_id is
-- the natural idempotency key — ON CONFLICT DO NOTHING as a second guard
-- even though the NOT EXISTS below should already prevent it).
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
  AND NOT EXISTS (SELECT 1 FROM gold.fact_revenue_adjustment fra WHERE fra.adjustment_id = a.adjustment_id)
ON CONFLICT (adjustment_id) DO NOTHING;

-- Step 2: recompute adjustment_delta_total ONLY for conversions with at
-- least one adjustment (cheap relative to a full-table scan; correctness
-- doesn't depend on scoping this, only performance does). This single
-- UPDATE, run after step 1 either the first time or the hundredth time,
-- always leaves adjustment_delta_total exactly equal to
-- SUM(fact_revenue_adjustment.delta_amount) for that conversion — no
-- history of "how many times has this run" affects the result.
UPDATE gold.fact_conversion fc
SET adjustment_delta_total = totals.total_delta
FROM (
    SELECT conversion_key, SUM(delta_amount) AS total_delta
    FROM gold.fact_revenue_adjustment
    GROUP BY conversion_key
) totals
WHERE totals.conversion_key = fc.conversion_key
  AND fc.adjustment_delta_total IS DISTINCT FROM totals.total_delta;
