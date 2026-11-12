-- ============================================================
-- BRONZE EXTRACT - Event / transactional domain
-- Task: extract_events  (Extract stage, Airflow TaskGroup)
--
-- These 6 sources are the high-volume, insert-mostly event tables
-- (Mini Project 01 extracted them incrementally via NiFi's
-- Maximum-value Column on the surrogate key). Here the watermark is
-- created_at/updated_at instead, which is what these columns were
-- added to the dataset for - see miniproject1/README.md's dataset
-- update note. Same upsert-by-natural-key pattern keeps re-runs
-- idempotent.
-- ============================================================

INSERT INTO bronze.raw_streaming_session (
    stream_id, customer_id, content_id, device_id, start_time, end_time,
    watch_duration, completion_percentage, created_at, updated_at, batch_id
)
SELECT
    stream_id, customer_id, content_id, device_id, start_time, end_time,
    watch_duration, completion_percentage, created_at, updated_at, '{{ run_id }}'
FROM source.streaming_session
WHERE created_at >= '{{ data_interval_start }}' AND created_at < '{{ data_interval_end }}'
   OR updated_at >= '{{ data_interval_start }}' AND updated_at < '{{ data_interval_end }}'
ON CONFLICT (stream_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, content_id = EXCLUDED.content_id,
    device_id = EXCLUDED.device_id, start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time,
    watch_duration = EXCLUDED.watch_duration, completion_percentage = EXCLUDED.completion_percentage,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_rental (
    rental_id, customer_id, inventory_id, rental_date, due_date, return_date,
    rental_fee, late_fee, status, created_at, updated_at, batch_id
)
SELECT
    rental_id, customer_id, inventory_id, rental_date, due_date, return_date,
    rental_fee, late_fee, status, created_at, updated_at, '{{ run_id }}'
FROM source.rental
WHERE created_at >= '{{ data_interval_start }}' AND created_at < '{{ data_interval_end }}'
   OR updated_at >= '{{ data_interval_start }}' AND updated_at < '{{ data_interval_end }}'
ON CONFLICT (rental_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, inventory_id = EXCLUDED.inventory_id,
    rental_date = EXCLUDED.rental_date, due_date = EXCLUDED.due_date, return_date = EXCLUDED.return_date,
    rental_fee = EXCLUDED.rental_fee, late_fee = EXCLUDED.late_fee, status = EXCLUDED.status,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_payment (
    payment_id, customer_id, rental_id, subscription_id, payment_method_id,
    amount, payment_date, payment_type, status, created_at, updated_at, batch_id
)
SELECT
    payment_id, customer_id, rental_id, subscription_id, payment_method_id,
    amount, payment_date, payment_type, status, created_at, updated_at, '{{ run_id }}'
FROM source.payment
WHERE created_at >= '{{ data_interval_start }}' AND created_at < '{{ data_interval_end }}'
   OR updated_at >= '{{ data_interval_start }}' AND updated_at < '{{ data_interval_end }}'
ON CONFLICT (payment_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, rental_id = EXCLUDED.rental_id,
    subscription_id = EXCLUDED.subscription_id, payment_method_id = EXCLUDED.payment_method_id,
    amount = EXCLUDED.amount, payment_date = EXCLUDED.payment_date,
    payment_type = EXCLUDED.payment_type, status = EXCLUDED.status,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_support_ticket (
    ticket_id, customer_id, category_id, opened_date, closed_date, priority, status, created_at, updated_at, batch_id
)
SELECT
    ticket_id, customer_id, category_id, opened_date, closed_date, priority, status, created_at, updated_at, '{{ run_id }}'
FROM source.support_ticket
WHERE created_at >= '{{ data_interval_start }}' AND created_at < '{{ data_interval_end }}'
   OR updated_at >= '{{ data_interval_start }}' AND updated_at < '{{ data_interval_end }}'
ON CONFLICT (ticket_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, category_id = EXCLUDED.category_id,
    opened_date = EXCLUDED.opened_date, closed_date = EXCLUDED.closed_date,
    priority = EXCLUDED.priority, status = EXCLUDED.status,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_review (
    review_id, customer_id, content_id, rating, review_text, review_date, created_at, updated_at, batch_id
)
SELECT
    review_id, customer_id, content_id, rating, review_text, review_date, created_at, updated_at, '{{ run_id }}'
FROM source.review
WHERE created_at >= '{{ data_interval_start }}' AND created_at < '{{ data_interval_end }}'
   OR updated_at >= '{{ data_interval_start }}' AND updated_at < '{{ data_interval_end }}'
ON CONFLICT (review_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, content_id = EXCLUDED.content_id,
    rating = EXCLUDED.rating, review_text = EXCLUDED.review_text, review_date = EXCLUDED.review_date,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_wishlist (
    wishlist_id, customer_id, content_id, added_date, created_at, updated_at, batch_id
)
SELECT
    wishlist_id, customer_id, content_id, added_date, created_at, updated_at, '{{ run_id }}'
FROM source.wishlist
WHERE created_at >= '{{ data_interval_start }}' AND created_at < '{{ data_interval_end }}'
   OR updated_at >= '{{ data_interval_start }}' AND updated_at < '{{ data_interval_end }}'
ON CONFLICT (wishlist_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, content_id = EXCLUDED.content_id, added_date = EXCLUDED.added_date,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;
