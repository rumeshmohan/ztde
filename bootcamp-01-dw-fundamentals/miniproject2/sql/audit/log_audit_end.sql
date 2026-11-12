-- ============================================================
-- Task: log_audit_end (runs immediately before Finish, only on the
-- success path - the failure path is handled by the DAG's
-- on_failure_callback instead, see dags/abc_hub_medallion_pipeline.py)
--
-- Dimension rows (always a plain INSERT for brand-new/new-version
-- members, from sp_load_dim_*) are counted as inserted_record_count;
-- fact rows (always INSERT ... ON CONFLICT DO UPDATE, so a single
-- ROW_COUNT cannot distinguish a fresh insert from a restated
-- existing row) are counted as updated_record_count.
-- ============================================================
UPDATE gold.etl_audit_log
SET end_time = CURRENT_TIMESTAMP,
    status = 'SUCCESS',
    source_record_count = (
        SELECT COALESCE(SUM(c), 0) FROM (
            SELECT COUNT(*) AS c FROM bronze.raw_customer             WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_customer_address          WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_customer_subscription     WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_content                   WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_content_genre              WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_inventory_item             WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_streaming_session          WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_rental                     WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_payment                    WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_support_ticket             WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_review                     WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM bronze.raw_wishlist                   WHERE batch_id = '{{ run_id }}'
        ) src
    ),
    processed_record_count = (
        SELECT COALESCE(SUM(c), 0) FROM (
            SELECT COUNT(*) AS c FROM silver.customer          WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM silver.content                 WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM silver.inventory_item           WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM silver.streaming_session        WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM silver.rental                   WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM silver.payment                  WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM silver.support_ticket           WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM silver.review                   WHERE batch_id = '{{ run_id }}' UNION ALL
            SELECT COUNT(*) FROM silver.wishlist                 WHERE batch_id = '{{ run_id }}'
        ) proc
    ),
    inserted_record_count = (
        SELECT COALESCE(SUM(rows_affected), 0) FROM gold.etl_audit_step_log
        WHERE batch_id = '{{ run_id }}'
          AND step_name IN ('sp_load_dim_reference', 'sp_load_dim_customer', 'sp_load_dim_content', 'sp_load_dim_inventory')
    ),
    updated_record_count = (
        SELECT COALESCE(SUM(rows_affected), 0) FROM gold.etl_audit_step_log
        WHERE batch_id = '{{ run_id }}'
          AND step_name IN ('sp_load_fact_customer_daily', 'sp_load_fact_content_monthly', 'sp_load_fact_inventory_util')
    ),
    rejected_record_count = (
        SELECT COUNT(*) FROM silver.dq_rejected WHERE batch_id = '{{ run_id }}'
    )
WHERE run_id = '{{ run_id }}';
