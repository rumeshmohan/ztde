-- ============================================================
-- DATA QUALITY CHECKS  (Question 4 Part C)
--
-- Two gates, matching the brief's flowchart:
--   "Data Validation"          - runs on BRONZE, before Silver Transformation
--   "Data Quality Validation"  - runs on GOLD,   before Finish
--
-- Pattern for every rule below (all scoped to the current run via
-- batch_id = '{{ run_id }}', so re-running a batch re-validates only
-- that batch's rows):
--   1. QUARANTINE - move violating rows into silver.dq_rejected
--      (visible, queryable evidence - never a silent drop).
--   2. EXCLUDE    - the paired Silver transform INSERT (see
--      sql/silver/02_*.sql) repeats the identical WHERE NOT(...)
--      so a quarantined row can never also land in Silver.
--   3. ASSERT     - a SQLCheckOperator-ready boolean query that
--      fails the Airflow task if step 2 ever let a violation
--      through. This is the "pipeline must not silently load
--      invalid data" requirement: if this assertion ever returns
--      FALSE, the task fails, the run shows FAILED in
--      gold.etl_audit_log, and the cause is one query away.
-- ============================================================


-- ============================================================
-- SECTION 1 - Null / mandatory-field checks
-- ============================================================

-- 1a. customer: customer_no, first_name, last_name, registration_date, status
INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_customer', customer_id::TEXT, 'missing mandatory field', batch_id, to_jsonb(t)
FROM bronze.raw_customer t
WHERE batch_id = '{{ run_id }}'
  AND (customer_no IS NULL OR first_name IS NULL OR last_name IS NULL
       OR registration_date IS NULL OR status IS NULL);

-- 1b. payment: amount, payment_date, payment_type, status
INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_payment', payment_id::TEXT, 'missing mandatory field', batch_id, to_jsonb(t)
FROM bronze.raw_payment t
WHERE batch_id = '{{ run_id }}'
  AND (amount IS NULL OR payment_date IS NULL OR payment_type IS NULL OR status IS NULL);

-- ASSERT (SQLCheckOperator): zero mandatory-field violations reached Silver
-- SELECT COUNT(*) = 0 FROM silver.customer WHERE customer_no IS NULL OR first_name IS NULL OR last_name IS NULL OR registration_date IS NULL OR status IS NULL;
-- SELECT COUNT(*) = 0 FROM silver.payment WHERE amount IS NULL OR payment_date IS NULL OR payment_type IS NULL OR status IS NULL;


-- ============================================================
-- SECTION 2 - Duplicate detection
-- Natural-key duplicates within the SAME extraction batch (the
-- ON CONFLICT upserts in Bronze/Silver already prevent duplicates
-- ACROSS batches by design - this rule catches a source anomaly
-- where the same key appears twice inside one extract).
-- ============================================================

INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_streaming_session', stream_id::TEXT, 'duplicate natural key in batch', batch_id, to_jsonb(t)
FROM bronze.raw_streaming_session t
WHERE batch_id = '{{ run_id }}'
  AND stream_id IN (
      SELECT stream_id FROM bronze.raw_streaming_session
      WHERE batch_id = '{{ run_id }}'
      GROUP BY stream_id HAVING COUNT(*) > 1
  );

-- ASSERT: SELECT COUNT(*) = 0 FROM (SELECT stream_id FROM silver.streaming_session GROUP BY stream_id HAVING COUNT(*) > 1) d;


-- ============================================================
-- SECTION 3 - Referential integrity
-- A business event referencing a customer/content id that does not
-- exist anywhere in this batch's Bronze reference data.
-- ============================================================

INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_rental', r.rental_id::TEXT, 'inventory_id not found in bronze.raw_inventory_item', r.batch_id, to_jsonb(r)
FROM bronze.raw_rental r
WHERE r.batch_id = '{{ run_id }}'
  AND NOT EXISTS (SELECT 1 FROM bronze.raw_inventory_item i WHERE i.inventory_id = r.inventory_id);

-- ASSERT: SELECT COUNT(*) = 0 FROM silver.rental r WHERE NOT EXISTS (SELECT 1 FROM silver.inventory_item i WHERE i.inventory_id = r.inventory_id);


-- ============================================================
-- SECTION 4 - Valid dates
-- ============================================================

INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_rental', rental_id::TEXT, 'return_date or due_date before rental_date', batch_id, to_jsonb(t)
FROM bronze.raw_rental t
WHERE batch_id = '{{ run_id }}'
  AND ((return_date IS NOT NULL AND return_date < rental_date)
    OR (due_date    IS NOT NULL AND due_date    < rental_date));

INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_streaming_session', stream_id::TEXT, 'end_time before start_time', batch_id, to_jsonb(t)
FROM bronze.raw_streaming_session t
WHERE batch_id = '{{ run_id }}'
  AND end_time IS NOT NULL AND end_time < start_time;

-- ASSERT: SELECT COUNT(*) = 0 FROM silver.rental WHERE (return_date IS NOT NULL AND return_date < rental_date) OR (due_date IS NOT NULL AND due_date < rental_date);
-- ASSERT: SELECT COUNT(*) = 0 FROM silver.streaming_session WHERE end_time IS NOT NULL AND end_time < start_time;


-- ============================================================
-- SECTION 5 - Valid monetary values
-- These are the exact planted issues Mini Project 01's NiFi flow
-- found (late_fee < 0 in rental, amount < 0 in payment) - the same
-- rule, now expressed as a SQL gate instead of a NiFi QueryRecord.
-- ============================================================

INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_rental', rental_id::TEXT, 'negative rental_fee or late_fee', batch_id, to_jsonb(t)
FROM bronze.raw_rental t
WHERE batch_id = '{{ run_id }}'
  AND (COALESCE(rental_fee, 0) < 0 OR COALESCE(late_fee, 0) < 0);

INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_payment', payment_id::TEXT, 'negative amount', batch_id, to_jsonb(t)
FROM bronze.raw_payment t
WHERE batch_id = '{{ run_id }}'
  AND amount < 0;

-- ASSERT: SELECT COUNT(*) = 0 FROM silver.rental WHERE rental_fee < 0 OR late_fee < 0;
-- ASSERT: SELECT COUNT(*) = 0 FROM silver.payment WHERE amount < 0;


-- ============================================================
-- SECTION 6 - Valid business statuses / value ranges
-- Includes the other two planted issues (completion_percentage > 100
-- in streaming_session, rating outside 1-5 in review).
-- ============================================================

INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_streaming_session', stream_id::TEXT, 'completion_percentage out of [0,100]', batch_id, to_jsonb(t)
FROM bronze.raw_streaming_session t
WHERE batch_id = '{{ run_id }}'
  AND (completion_percentage < 0 OR completion_percentage > 100);

INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_review', review_id::TEXT, 'rating out of [1,5]', batch_id, to_jsonb(t)
FROM bronze.raw_review t
WHERE batch_id = '{{ run_id }}'
  AND (rating < 1 OR rating > 5);

INSERT INTO silver.dq_rejected (source_table, source_pk, reject_reason, batch_id, record_payload)
SELECT 'bronze.raw_rental', rental_id::TEXT, 'status not a recognised value', batch_id, to_jsonb(t)
FROM bronze.raw_rental t
WHERE batch_id = '{{ run_id }}'
  AND LOWER(TRIM(status)) NOT IN ('active', 'returned', 'overdue', 'cancelled');

-- ASSERT: SELECT COUNT(*) = 0 FROM silver.streaming_session WHERE completion_percentage < 0 OR completion_percentage > 100;
-- ASSERT: SELECT COUNT(*) = 0 FROM silver.review WHERE rating < 1 OR rating > 5;
-- ASSERT: SELECT COUNT(*) = 0 FROM silver.rental WHERE LOWER(TRIM(status)) NOT IN ('active','returned','overdue','cancelled');


-- ============================================================
-- SECTION 7 - Fact / Dimension consistency  ("Data Quality
-- Validation" stage, runs AFTER Gold Load)
--
-- gold.sp_load_fact_* already INNER JOINs every dimension, so an
-- orphaned fact row structurally cannot be inserted - this is a
-- regression safety net confirming that guarantee still holds,
-- not a rule expected to ever find a violation.
-- ============================================================

-- ASSERT: SELECT COUNT(*) = 0 FROM gold.fact_customer_daily_activity f WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer d WHERE d.customer_key = f.customer_key);
-- ASSERT: SELECT COUNT(*) = 0 FROM gold.fact_content_monthly_performance f WHERE NOT EXISTS (SELECT 1 FROM gold.dim_content d WHERE d.content_key = f.content_key);
-- ASSERT: SELECT COUNT(*) = 0 FROM gold.fact_inventory_utilisation f WHERE NOT EXISTS (SELECT 1 FROM gold.dim_inventory_item d WHERE d.inventory_key = f.inventory_key);
-- ASSERT: SELECT COUNT(*) = 0 FROM gold.fact_inventory_utilisation f WHERE NOT EXISTS (SELECT 1 FROM gold.dim_warehouse d WHERE d.warehouse_key = f.warehouse_key);
