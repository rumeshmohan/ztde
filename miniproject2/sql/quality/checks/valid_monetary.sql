-- SQLCheckOperator ("Data Validation" stage): every negative
-- monetary value this batch produced was quarantined. These are the
-- exact planted issues Mini Project 01 found (rental.late_fee < 0,
-- payment.amount < 0).
SELECT
    (SELECT COUNT(*) = 0 FROM bronze.raw_rental r
     WHERE r.batch_id = '{{ run_id }}'
       AND (COALESCE(r.rental_fee, 0) < 0 OR COALESCE(r.late_fee, 0) < 0)
       AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected rej
                        WHERE rej.source_table = 'bronze.raw_rental'
                          AND rej.source_pk = r.rental_id::TEXT AND rej.batch_id = r.batch_id))
AND (SELECT COUNT(*) = 0 FROM bronze.raw_payment p
     WHERE p.batch_id = '{{ run_id }}'
       AND p.amount < 0
       AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected rej
                        WHERE rej.source_table = 'bronze.raw_payment'
                          AND rej.source_pk = p.payment_id::TEXT AND rej.batch_id = p.batch_id));
