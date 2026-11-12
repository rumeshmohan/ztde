-- SQLCheckOperator ("Data Validation" stage): every duplicated
-- natural key this batch produced was quarantined.
SELECT COUNT(*) = 0 FROM (
    SELECT stream_id FROM bronze.raw_streaming_session
    WHERE batch_id = '{{ run_id }}'
    GROUP BY stream_id HAVING COUNT(*) > 1
) dup
WHERE NOT EXISTS (
    SELECT 1 FROM silver.dq_rejected r
    WHERE r.source_table = 'bronze.raw_streaming_session'
      AND r.source_pk = dup.stream_id::TEXT AND r.batch_id = '{{ run_id }}'
);
