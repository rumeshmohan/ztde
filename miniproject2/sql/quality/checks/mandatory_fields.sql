-- SQLCheckOperator ("Data Validation" stage, runs on BRONZE right
-- after the quarantine task, BEFORE Silver Transformation).
-- Asserts every mandatory-field violation this batch produced has a
-- matching row in silver.dq_rejected - i.e. quarantine caught 100% of
-- them, so Silver Transformation is safe to run next.
SELECT
    (SELECT COUNT(*) = 0 FROM bronze.raw_customer c
     WHERE c.batch_id = '{{ run_id }}'
       AND (c.customer_no IS NULL OR c.first_name IS NULL OR c.last_name IS NULL
            OR c.registration_date IS NULL OR c.status IS NULL)
       AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected r
                        WHERE r.source_table = 'bronze.raw_customer'
                          AND r.source_pk = c.customer_id::TEXT AND r.batch_id = c.batch_id))
AND (SELECT COUNT(*) = 0 FROM bronze.raw_payment p
     WHERE p.batch_id = '{{ run_id }}'
       AND (p.amount IS NULL OR p.payment_date IS NULL OR p.payment_type IS NULL OR p.status IS NULL)
       AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected r
                        WHERE r.source_table = 'bronze.raw_payment'
                          AND r.source_pk = p.payment_id::TEXT AND r.batch_id = p.batch_id));
