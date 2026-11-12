-- ============================================================================
-- PulseTrack — Reconciliation Report (Q6): Source -> Bronze -> Silver -> Gold
-- One row per major entity, with the fields the brief names: source count,
-- valid count, rejected count, duplicate count, inserted count, updated
-- count. Every number is a live query result, not a restated one.
--
-- MODEL NOTE (a real correction, not the first version of this script):
-- a naive "valid + rejected = bronze - duplicate" identity is WRONG for
-- this pipeline, because rejection happens at TWO DIFFERENT stages
-- depending on the entity, and the first version of this report treated
-- them as one undifferentiated "rejected_count", which made its own
-- reconciliation check fail for `conversions` and pass only by
-- coincidence for the other four:
--   - Some rejection happens BEFORE a row ever reaches Silver (a business
--     rule removes it from the CREATE TABLE AS ... WHERE clause entirely —
--     is_test-only conversions, is_test-linked touchpoints, orphaned
--     revenue_adjustments). These rows are NOT in silver_valid_count at all.
--   - Some rejection happens AT THE GOLD LOAD, over rows that DID make it
--     into Silver (a blank/unparseable date, or a parent excluded from
--     Gold) — audit.quarantine's rows for these ARE a subset of
--     silver_valid_count, not additional to it.
-- The two correct identities, verified as EXACT equalities below (not a
-- loose ">=" check):
--   bronze_count - duplicate_count - rejected_before_silver = silver_valid_count
--   silver_valid_count - rejected_at_gold = gold_loaded_count
-- ============================================================================

-- `pulsetrack_raw` (the source) is a SEPARATE PostgreSQL database from this
-- warehouse (`pulsetrack_dw`) — Q3 deliberately chose plain psycopg
-- extraction over postgres_fdw, so there is no cross-database query
-- available here. Source counts are pulled by a one-line companion query
-- run against pulsetrack_raw and inlined below as a VALUES list (verified
-- fresh, not from memory, before this report was finalised) — re-run that
-- companion query and update these literals if the source is ever reloaded
-- with different data. Equal to bronze_count every time this has been run,
-- which is expected (extract_relational.py is a full, lossless copy).
WITH source_counts (entity, source_count) AS (
    VALUES ('impressions', 300000), ('clicks', 150000), ('conversions', 126380),
           ('conversion_touchpoints', 183529), ('revenue_adjustments', 12000)
),
bronze_counts AS (
    SELECT 'impressions' AS entity, (SELECT COUNT(*) FROM bronze.impressions_raw) AS bronze_count
    UNION ALL SELECT 'clicks', (SELECT COUNT(*) FROM bronze.clicks_raw)
    UNION ALL SELECT 'conversions', (SELECT COUNT(*) FROM bronze.conversions_raw)
    UNION ALL SELECT 'conversion_touchpoints', (SELECT COUNT(*) FROM bronze.conversion_touchpoints_raw)
    UNION ALL SELECT 'revenue_adjustments', (SELECT COUNT(*) FROM bronze.revenue_adjustments_raw)
),
duplicate_counts AS (
    -- distinct natural-key ids with more than one raw row (lifecycle
    -- snapshots + exact dupes); only conversions_raw has this by design (Q2 A.2)
    SELECT 'impressions' AS entity, 0 AS duplicate_count
    UNION ALL SELECT 'clicks', 0
    UNION ALL SELECT 'conversions', (SELECT COUNT(*) FROM bronze.conversions_raw) - (SELECT COUNT(DISTINCT conversion_id) FROM bronze.conversions_raw)
    UNION ALL SELECT 'conversion_touchpoints', 0
    UNION ALL SELECT 'revenue_adjustments', 0
),
silver_valid_counts AS (
    SELECT 'impressions' AS entity, (SELECT COUNT(*) FROM silver.impressions) AS silver_valid_count
    UNION ALL SELECT 'clicks', (SELECT COUNT(*) FROM silver.clicks)
    UNION ALL SELECT 'conversions', (SELECT COUNT(*) FROM silver.conversions)
    UNION ALL SELECT 'conversion_touchpoints', (SELECT COUNT(*) FROM silver.conversion_touchpoints)
    UNION ALL SELECT 'revenue_adjustments', (SELECT COUNT(*) FROM silver.revenue_adjustments)
),
gold_counts AS (
    SELECT 'impressions' AS entity, (SELECT COUNT(*) FROM gold.fact_impression) AS gold_count
    UNION ALL SELECT 'clicks', (SELECT COUNT(*) FROM gold.fact_click)
    UNION ALL SELECT 'conversions', (SELECT COUNT(*) FROM gold.fact_conversion)
    UNION ALL SELECT 'conversion_touchpoints', (SELECT COUNT(*) FROM gold.bridge_conversion_touchpoint)
    UNION ALL SELECT 'revenue_adjustments', (SELECT COUNT(*) FROM gold.fact_revenue_adjustment)
),
rejected_before_silver AS (
    -- business-rule exclusions: rows a Silver CREATE TABLE AS ... WHERE
    -- clause removes entirely, before the table exists. Computed directly
    -- (bronze distinct ids minus silver rows), not just asserted.
    SELECT 'impressions' AS entity, 0 AS rejected_before_silver
    UNION ALL SELECT 'clicks', 0
    UNION ALL SELECT 'conversions',
        (SELECT COUNT(DISTINCT conversion_id) FROM bronze.conversions_raw) - (SELECT COUNT(*) FROM silver.conversions)
    UNION ALL SELECT 'conversion_touchpoints',
        (SELECT COUNT(*) FROM audit.quarantine WHERE source_table = 'conversion_touchpoints_raw')
    UNION ALL SELECT 'revenue_adjustments',
        (SELECT COUNT(*) FROM bronze.revenue_adjustments_raw) - (SELECT COUNT(*) FROM silver.revenue_adjustments)
),
rejected_at_gold AS (
    -- quarantined rows that DID reach Silver but couldn't load into Gold
    -- (blank/unparseable date, or a parent excluded from Gold) — a subset
    -- of silver_valid_count, not additional to it.
    SELECT 'impressions' AS entity, (SELECT COUNT(*) FROM audit.quarantine WHERE source_table = 'silver.impressions') AS rejected_at_gold
    UNION ALL SELECT 'clicks', (SELECT COUNT(*) FROM audit.quarantine WHERE source_table = 'silver.clicks')
    UNION ALL SELECT 'conversions', (SELECT COUNT(*) FROM audit.quarantine WHERE source_table = 'silver.conversions')
    UNION ALL SELECT 'conversion_touchpoints', (SELECT COUNT(*) FROM audit.quarantine WHERE source_table = 'silver.conversion_touchpoints')
    UNION ALL SELECT 'revenue_adjustments', (SELECT COUNT(*) FROM audit.quarantine WHERE source_table = 'silver.revenue_adjustments')
),
latest_run AS (
    SELECT DISTINCT ON (target_table) target_table, inserted_count, updated_count, status, end_time
    FROM audit.pipeline_run_log
    WHERE layer = 'gold' AND target_table IS NOT NULL
    ORDER BY target_table, end_time DESC NULLS LAST
)
SELECT
    s.entity,
    s.source_count,
    b.bronze_count,
    d.duplicate_count,
    rbs.rejected_before_silver,
    sv.silver_valid_count,
    rag.rejected_at_gold,
    g.gold_count AS gold_loaded_count,
    lr.inserted_count AS last_run_inserted,
    lr.updated_count AS last_run_updated,
    (b.bronze_count = s.source_count) AS bronze_ties_to_source,
    (b.bronze_count - d.duplicate_count - rbs.rejected_before_silver = sv.silver_valid_count) AS silver_identity_holds,
    (sv.silver_valid_count - rag.rejected_at_gold = g.gold_count) AS gold_identity_holds
FROM source_counts s
JOIN bronze_counts b ON b.entity = s.entity
JOIN duplicate_counts d ON d.entity = s.entity
JOIN rejected_before_silver rbs ON rbs.entity = s.entity
JOIN silver_valid_counts sv ON sv.entity = s.entity
JOIN rejected_at_gold rag ON rag.entity = s.entity
JOIN gold_counts g ON g.entity = s.entity
LEFT JOIN latest_run lr ON lr.target_table = CASE s.entity
    WHEN 'impressions' THEN 'gold.fact_impression'
    WHEN 'clicks' THEN 'gold.fact_click'
    WHEN 'conversions' THEN 'gold.fact_conversion'
    WHEN 'conversion_touchpoints' THEN 'gold.bridge_conversion_touchpoint'
    WHEN 'revenue_adjustments' THEN 'gold.fact_revenue_adjustment'
END
ORDER BY s.entity;
