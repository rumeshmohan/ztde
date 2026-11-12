-- SQLCheckOperator ("Data Validation" stage): every out-of-range
-- value or unrecognised status this batch produced was quarantined.
-- Two of these are Mini Project 01's other planted issues
-- (streaming_session.completion_percentage > 100, review.rating
-- outside 1-5).
SELECT
    (SELECT COUNT(*) = 0 FROM bronze.raw_streaming_session s
     WHERE s.batch_id = '{{ run_id }}'
       AND (s.completion_percentage < 0 OR s.completion_percentage > 100)
       AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected rej
                        WHERE rej.source_table = 'bronze.raw_streaming_session'
                          AND rej.source_pk = s.stream_id::TEXT AND rej.batch_id = s.batch_id))
AND (SELECT COUNT(*) = 0 FROM bronze.raw_review v
     WHERE v.batch_id = '{{ run_id }}'
       AND (v.rating < 1 OR v.rating > 5)
       AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected rej
                        WHERE rej.source_table = 'bronze.raw_review'
                          AND rej.source_pk = v.review_id::TEXT AND rej.batch_id = v.batch_id))
AND (SELECT COUNT(*) = 0 FROM bronze.raw_rental r
     WHERE r.batch_id = '{{ run_id }}'
       AND LOWER(TRIM(r.status)) NOT IN ('active', 'returned', 'overdue', 'cancelled')
       AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected rej
                        WHERE rej.source_table = 'bronze.raw_rental'
                          AND rej.source_pk = r.rental_id::TEXT AND rej.batch_id = r.batch_id));
