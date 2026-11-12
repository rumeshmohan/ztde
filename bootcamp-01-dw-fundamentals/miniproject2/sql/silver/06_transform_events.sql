-- ============================================================
-- SILVER TRANSFORM - Event / transactional domain
-- Task: transform_events  (Silver Transformation stage)
--
-- Every WHERE NOT(...) below mirrors the matching quarantine rule
-- in sql/quality/01_data_quality_checks.sql exactly, so a row
-- rejected there can never also appear here.
-- ============================================================

INSERT INTO silver.streaming_session (
    stream_id, customer_id, content_id, device_id, start_time, end_time,
    activity_date, watch_duration, completion_percentage, batch_id
)
SELECT
    stream_id, customer_id, content_id, device_id, start_time, end_time,
    CAST(start_time AS DATE), COALESCE(watch_duration, 0), COALESCE(completion_percentage, 0),
    batch_id
FROM bronze.raw_streaming_session t
WHERE batch_id = '{{ run_id }}'
  -- Section 2 (duplicate) + Section 4 (end_time < start_time) + Section 6 (completion % out of range)
  AND stream_id NOT IN (
        SELECT stream_id FROM bronze.raw_streaming_session
        WHERE batch_id = '{{ run_id }}' GROUP BY stream_id HAVING COUNT(*) > 1)
  AND NOT (end_time IS NOT NULL AND end_time < start_time)
  AND NOT (completion_percentage < 0 OR completion_percentage > 100)
ON CONFLICT (stream_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, content_id = EXCLUDED.content_id, device_id = EXCLUDED.device_id,
    start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time, activity_date = EXCLUDED.activity_date,
    watch_duration = EXCLUDED.watch_duration, completion_percentage = EXCLUDED.completion_percentage,
    batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;

INSERT INTO silver.rental (
    rental_id, customer_id, inventory_id, content_id, rental_date, due_date, return_date,
    rental_fee, late_fee, total_fee, status, batch_id
)
SELECT
    r.rental_id, r.customer_id, r.inventory_id, i.content_id, r.rental_date, r.due_date, r.return_date,
    COALESCE(r.rental_fee, 0), COALESCE(r.late_fee, 0), COALESCE(r.rental_fee, 0) + COALESCE(r.late_fee, 0),
    INITCAP(TRIM(r.status)), r.batch_id
FROM bronze.raw_rental r
JOIN bronze.raw_inventory_item i ON i.inventory_id = r.inventory_id
WHERE r.batch_id = '{{ run_id }}'
  -- Section 3 (referential integrity) is enforced by the JOIN above.
  -- Section 4 (bad dates) + Section 5 (negative fees) + Section 6 (bad status)
  AND NOT ((r.return_date IS NOT NULL AND r.return_date < r.rental_date)
        OR (r.due_date    IS NOT NULL AND r.due_date    < r.rental_date))
  AND NOT (COALESCE(r.rental_fee, 0) < 0 OR COALESCE(r.late_fee, 0) < 0)
  AND LOWER(TRIM(r.status)) IN ('active', 'returned', 'overdue', 'cancelled')
ON CONFLICT (rental_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, inventory_id = EXCLUDED.inventory_id, content_id = EXCLUDED.content_id,
    rental_date = EXCLUDED.rental_date, due_date = EXCLUDED.due_date, return_date = EXCLUDED.return_date,
    rental_fee = EXCLUDED.rental_fee, late_fee = EXCLUDED.late_fee, total_fee = EXCLUDED.total_fee,
    status = EXCLUDED.status, batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;

INSERT INTO silver.payment (
    payment_id, customer_id, rental_id, subscription_id, amount, payment_date,
    activity_date, payment_type, status, batch_id
)
SELECT
    payment_id, customer_id, rental_id, subscription_id, amount, payment_date,
    CAST(payment_date AS DATE), payment_type, INITCAP(TRIM(status)), batch_id
FROM bronze.raw_payment t
WHERE batch_id = '{{ run_id }}'
  -- Section 1b (mandatory fields) + Section 5 (negative amount)
  AND NOT (amount IS NULL OR payment_date IS NULL OR payment_type IS NULL OR status IS NULL)
  AND amount >= 0
ON CONFLICT (payment_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, rental_id = EXCLUDED.rental_id, subscription_id = EXCLUDED.subscription_id,
    amount = EXCLUDED.amount, payment_date = EXCLUDED.payment_date, activity_date = EXCLUDED.activity_date,
    payment_type = EXCLUDED.payment_type, status = EXCLUDED.status,
    batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;

INSERT INTO silver.support_ticket (
    ticket_id, customer_id, category_id, opened_date, closed_date, activity_date, priority, status, batch_id
)
SELECT
    ticket_id, customer_id, category_id, opened_date, closed_date,
    CAST(opened_date AS DATE), priority, INITCAP(TRIM(status)), batch_id
FROM bronze.raw_support_ticket
WHERE batch_id = '{{ run_id }}'
ON CONFLICT (ticket_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, category_id = EXCLUDED.category_id,
    opened_date = EXCLUDED.opened_date, closed_date = EXCLUDED.closed_date, activity_date = EXCLUDED.activity_date,
    priority = EXCLUDED.priority, status = EXCLUDED.status,
    batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;

INSERT INTO silver.review (review_id, customer_id, content_id, rating, review_date, activity_date, batch_id)
SELECT review_id, customer_id, content_id, rating, review_date, CAST(review_date AS DATE), batch_id
FROM bronze.raw_review
WHERE batch_id = '{{ run_id }}'
  -- Section 6 (rating out of [1,5])
  AND rating BETWEEN 1 AND 5
ON CONFLICT (review_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, content_id = EXCLUDED.content_id, rating = EXCLUDED.rating,
    review_date = EXCLUDED.review_date, activity_date = EXCLUDED.activity_date,
    batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;

INSERT INTO silver.wishlist (wishlist_id, customer_id, content_id, added_date, activity_date, batch_id)
SELECT wishlist_id, customer_id, content_id, added_date, CAST(added_date AS DATE), batch_id
FROM bronze.raw_wishlist
WHERE batch_id = '{{ run_id }}'
ON CONFLICT (wishlist_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, content_id = EXCLUDED.content_id, added_date = EXCLUDED.added_date,
    activity_date = EXCLUDED.activity_date, batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;
