-- ============================================================
-- VERIFICATION QUERIES  (idempotency + backfill evidence)
-- Run these after every DAG run and paste the output into your
-- screenshots - the same pattern Mini Project 01's README used.
-- ============================================================

-- 1. Layer row counts - run before and after a re-run of the SAME
--    logical date; every number should be identical (idempotency).
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

-- 2. No duplicate keys anywhere in Gold (the actual "100, not 200" check)
SELECT 'fact_customer_daily_activity' AS fact, customer_key, date_key, COUNT(*)
FROM gold.fact_customer_daily_activity GROUP BY 2, 3 HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_content_monthly_performance', content_key, date_key, COUNT(*)
FROM gold.fact_content_monthly_performance GROUP BY 2, 3 HAVING COUNT(*) > 1
UNION ALL
SELECT 'fact_inventory_utilisation', inventory_key, date_key, COUNT(*)
FROM gold.fact_inventory_utilisation GROUP BY 2, 3 HAVING COUNT(*) > 1;
-- Expect ZERO rows back from this query, on every run.

-- 3. No customer/content/inventory item has more than one CURRENT
--    SCD2 version (idempotent SCD - re-running unchanged data must
--    not open a second current row).
SELECT 'dim_customer' AS dim, customer_id, COUNT(*)
FROM gold.dim_customer WHERE is_current GROUP BY customer_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'dim_content', content_id, COUNT(*)
FROM gold.dim_content WHERE is_current GROUP BY content_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'dim_inventory_item', inventory_id, COUNT(*)
FROM gold.dim_inventory_item WHERE is_current GROUP BY inventory_id HAVING COUNT(*) > 1;
-- Expect ZERO rows back from this query, on every run.

-- 4. Rejected records by reason - evidence for Question 4 Part C
SELECT source_table, reject_reason, COUNT(*) AS rejected_count
FROM silver.dq_rejected
GROUP BY 1, 2
ORDER BY 1, 2;

-- 5. Audit trail - one row per DAG run, in order, with elapsed time
SELECT batch_id, dag_id, run_id, status,
       start_time, end_time, (end_time - start_time) AS duration,
       source_record_count, processed_record_count,
       inserted_record_count, updated_record_count, rejected_record_count
FROM gold.etl_audit_log
ORDER BY start_time;

-- 6. Backfill evidence - one audit row per historical logical date,
--    each with plausible (non-zero, non-duplicated) counts
SELECT batch_id, run_id, status, source_record_count, rejected_record_count
FROM gold.etl_audit_log
WHERE run_id LIKE 'backfill%' OR run_id LIKE '%manual%'
ORDER BY start_time;

-- 7. Sample analytical query, same as Mini Project 01's, now against Gold
SELECT dc.title, SUM(f.total_stream_count) AS lifetime_streams
FROM gold.fact_content_monthly_performance f
JOIN gold.dim_content dc ON dc.content_key = f.content_key
GROUP BY dc.title
ORDER BY lifetime_streams DESC
LIMIT 15;
