-- SQLCheckOperator ("Data Validation" stage): every impossible date
-- ordering this batch produced was quarantined.
SELECT
    (SELECT COUNT(*) = 0 FROM bronze.raw_rental r
     WHERE r.batch_id = '{{ run_id }}'
       AND ((r.return_date IS NOT NULL AND r.return_date < r.rental_date)
         OR (r.due_date    IS NOT NULL AND r.due_date    < r.rental_date))
       AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected rej
                        WHERE rej.source_table = 'bronze.raw_rental'
                          AND rej.source_pk = r.rental_id::TEXT AND rej.batch_id = r.batch_id))
AND (SELECT COUNT(*) = 0 FROM bronze.raw_streaming_session s
     WHERE s.batch_id = '{{ run_id }}'
       AND s.end_time IS NOT NULL AND s.end_time < s.start_time
       AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected rej
                        WHERE rej.source_table = 'bronze.raw_streaming_session'
                          AND rej.source_pk = s.stream_id::TEXT AND rej.batch_id = s.batch_id));
