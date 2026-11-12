-- ============================================================
-- SCREENSHOT-READY QUERIES
-- Run these in DataGrip (or psql) against abc_hub_analytical, one
-- section at a time, and screenshot each result - they correspond
-- directly to screenshots/README.md's checklist items 04-08.
-- (Items 01-03 and 09-10 are captured from the Airflow UI/CLI - see
-- screenshots/README.md and screenshots/airflow_cli_outputs.txt.)
-- ============================================================


-- ------------------------------------------------------------
-- 04_postgres_layer_counts - Bronze/Silver/Gold row counts
-- ------------------------------------------------------------
SELECT 'bronze.raw_customer' AS table_name, COUNT(*) FROM bronze.raw_customer
UNION ALL SELECT 'silver.customer', COUNT(*) FROM silver.customer
UNION ALL SELECT 'gold.dim_customer (current)', COUNT(*) FROM gold.dim_customer WHERE is_current
UNION ALL SELECT 'gold.dim_customer (all versions)', COUNT(*) FROM gold.dim_customer
UNION ALL SELECT 'gold.dim_content (current)', COUNT(*) FROM gold.dim_content WHERE is_current
UNION ALL SELECT 'gold.dim_inventory_item (current)', COUNT(*) FROM gold.dim_inventory_item WHERE is_current
UNION ALL SELECT 'gold.fact_customer_daily_activity', COUNT(*) FROM gold.fact_customer_daily_activity
UNION ALL SELECT 'gold.fact_content_monthly_performance', COUNT(*) FROM gold.fact_content_monthly_performance
UNION ALL SELECT 'gold.fact_inventory_utilisation', COUNT(*) FROM gold.fact_inventory_utilisation
UNION ALL SELECT 'silver.dq_rejected', COUNT(*) FROM silver.dq_rejected;


-- ------------------------------------------------------------
-- 05_idempotency_rerun - run the SAME query above again after
-- re-processing a date (e.g. via airflow backfill create
-- --reprocess-behavior completed) and confirm every count is
-- unchanged. (This project already proved this for 2026-01-15 -
-- see the report, Question 3 Part D.)
-- ------------------------------------------------------------
-- (re-run the 04 block above)


-- ------------------------------------------------------------
-- 06_no_duplicates - MUST return zero rows from both halves
-- ------------------------------------------------------------
SELECT 'fact_customer_daily_activity' AS fact, customer_key, date_key, COUNT(*)
FROM gold.fact_customer_daily_activity GROUP BY 2, 3 HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_content_monthly_performance', content_key, date_key, COUNT(*)
FROM gold.fact_content_monthly_performance GROUP BY 2, 3 HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_inventory_utilisation', inventory_key, date_key, COUNT(*)
FROM gold.fact_inventory_utilisation GROUP BY 2, 3 HAVING COUNT(*) > 1;

SELECT 'dim_customer' AS dim, customer_id, COUNT(*)
FROM gold.dim_customer WHERE is_current GROUP BY customer_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'dim_content', content_id, COUNT(*)
FROM gold.dim_content WHERE is_current GROUP BY content_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'dim_inventory_item', inventory_id, COUNT(*)
FROM gold.dim_inventory_item WHERE is_current GROUP BY inventory_id HAVING COUNT(*) > 1;


-- ------------------------------------------------------------
-- 07_dq_rejected_rows - rejected records by reason
-- ------------------------------------------------------------
SELECT source_table, reject_reason, COUNT(*) AS rejected_count
FROM silver.dq_rejected
GROUP BY 1, 2
ORDER BY 1, 2;


-- ------------------------------------------------------------
-- 08_audit_log - full ETL audit trail, one row per DAG run
-- ------------------------------------------------------------
SELECT batch_id, dag_id, run_id, status,
       start_time, end_time, (end_time - start_time) AS duration,
       source_record_count, processed_record_count,
       inserted_record_count, updated_record_count, rejected_record_count
FROM gold.etl_audit_log
ORDER BY start_time;


-- ------------------------------------------------------------
-- Bonus - the two 2026-01-15 audit rows proving the backfill/
-- idempotency test specifically (original run + explicit backfill)
-- ------------------------------------------------------------
SELECT batch_id, status, start_time, end_time,
       source_record_count, processed_record_count, rejected_record_count
FROM gold.etl_audit_log
WHERE batch_id LIKE '%2026-01-15%' OR batch_id LIKE '%2026-01-16%'
ORDER BY start_time;


-- ------------------------------------------------------------
-- Bonus - SCD2 evidence: more total dim_customer rows than
-- current rows proves real historical versions were created
-- ------------------------------------------------------------
SELECT
    (SELECT COUNT(*) FROM gold.dim_customer) AS total_versions,
    (SELECT COUNT(*) FROM gold.dim_customer WHERE is_current) AS current_versions,
    (SELECT COUNT(*) FROM gold.dim_customer) - (SELECT COUNT(*) FROM gold.dim_customer WHERE is_current) AS historical_versions_created;

-- A specific customer who actually changed (pick any customer_id
-- appearing twice below to show the before/after row pair)
SELECT customer_key, customer_id, city_name, country_name, current_plan_name,
       customer_status, effective_date, expiry_date, is_current
FROM gold.dim_customer
WHERE customer_id IN (
    SELECT customer_id FROM gold.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
)
ORDER BY customer_id, effective_date
LIMIT 20;


-- ------------------------------------------------------------
-- Sample analytical query - proves Gold is genuinely queryable
-- ------------------------------------------------------------
SELECT dc.title, SUM(f.total_stream_count) AS lifetime_streams
FROM gold.fact_content_monthly_performance f
JOIN gold.dim_content dc ON dc.content_key = f.content_key
GROUP BY dc.title
ORDER BY lifetime_streams DESC
LIMIT 15;
