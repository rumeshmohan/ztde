-- SQLCheckOperator ("Data Quality Validation" stage, after Gold Load):
-- every fact row must resolve every dimension it references (Section 7).
-- gold.sp_load_fact_* already INNER JOINs every dimension, so this is a
-- regression safety net, not a rule expected to ever fail.
SELECT
    (SELECT COUNT(*) = 0 FROM gold.fact_customer_daily_activity f
     WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer d WHERE d.customer_key = f.customer_key))
AND (SELECT COUNT(*) = 0 FROM gold.fact_content_monthly_performance f
     WHERE NOT EXISTS (SELECT 1 FROM gold.dim_content d WHERE d.content_key = f.content_key))
AND (SELECT COUNT(*) = 0 FROM gold.fact_inventory_utilisation f
     WHERE NOT EXISTS (SELECT 1 FROM gold.dim_inventory_item d WHERE d.inventory_key = f.inventory_key))
AND (SELECT COUNT(*) = 0 FROM gold.fact_inventory_utilisation f
     WHERE NOT EXISTS (SELECT 1 FROM gold.dim_warehouse d WHERE d.warehouse_key = f.warehouse_key));
