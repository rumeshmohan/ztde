-- ============================================================
-- ETL Transformation Queries — SUPERSEDED, kept for reference only
--
-- This file predates the staging-based pipeline and is NOT part of the
-- executed ETL. The pipeline that actually runs (and produced the
-- verification numbers in the report) is:
--   07_create_staging_tables.sql -> 08_load_dimensions_from_staging.sql
--   -> 09_load_facts_from_staging.sql, driven by the NiFi flow in
--   nifi/ABC_Hub_ETL_Pipeline.json.
-- ============================================================

-- ============================================================
-- DIMENSION LOADING QUERIES
-- ============================================================

-- Load dim_customer (SCD Type 2)
-- Step 1: Expire changed records
UPDATE dim_customer
SET expiry_date = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
WHERE is_current = TRUE
  AND customer_id IN (
      SELECT c.customer_id
      FROM customer c
      LEFT JOIN customer_address ca ON c.customer_id = ca.customer_id AND ca.address_type = 'PRIMARY'
      LEFT JOIN city ci ON ca.city_id = ci.city_id
      LEFT JOIN country co ON ci.country_id = co.country_id
      LEFT JOIN customer_subscription cs ON c.customer_id = cs.customer_id AND cs.status = 'active'
      LEFT JOIN subscription_plan sp ON cs.plan_id = sp.plan_id
      WHERE c.customer_id = dim_customer.customer_id
        AND (
            c.status != dim_customer.customer_status
            OR COALESCE(ci.city_name, '') != COALESCE(dim_customer.city_name, '')
            OR COALESCE(sp.plan_name, '') != COALESCE(dim_customer.current_plan_name, '')
        )
  );

-- Step 2: Insert new/changed records
INSERT INTO dim_customer (
    customer_id, customer_no, first_name, last_name, full_name,
    email, phone, date_of_birth, gender, age_group,
    registration_date, customer_status, city_name, country_name,
    current_plan_name, current_plan_fee, effective_date, expiry_date, is_current
)
SELECT
    c.customer_id,
    c.customer_no,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email,
    c.phone,
    c.date_of_birth,
    c.gender,
    CASE
        WHEN EXTRACT(YEAR FROM AGE(c.date_of_birth)) < 18 THEN 'Under 18'
        WHEN EXTRACT(YEAR FROM AGE(c.date_of_birth)) BETWEEN 18 AND 24 THEN '18-24'
        WHEN EXTRACT(YEAR FROM AGE(c.date_of_birth)) BETWEEN 25 AND 34 THEN '25-34'
        WHEN EXTRACT(YEAR FROM AGE(c.date_of_birth)) BETWEEN 35 AND 44 THEN '35-44'
        WHEN EXTRACT(YEAR FROM AGE(c.date_of_birth)) BETWEEN 45 AND 54 THEN '45-54'
        WHEN EXTRACT(YEAR FROM AGE(c.date_of_birth)) BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    c.registration_date,
    c.status AS customer_status,
    ci.city_name,
    co.country_name,
    sp.plan_name AS current_plan_name,
    sp.monthly_fee AS current_plan_fee,
    CURRENT_DATE AS effective_date,
    '9999-12-31'::DATE AS expiry_date,
    TRUE AS is_current
FROM customer c
LEFT JOIN customer_address ca ON c.customer_id = ca.customer_id AND ca.address_type = 'PRIMARY'
LEFT JOIN city ci ON ca.city_id = ci.city_id
LEFT JOIN country co ON ci.country_id = co.country_id
LEFT JOIN customer_subscription cs ON c.customer_id = cs.customer_id AND cs.status = 'active'
LEFT JOIN subscription_plan sp ON cs.plan_id = sp.plan_id
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer dc
    WHERE dc.customer_id = c.customer_id AND dc.is_current = TRUE
);

-- Load dim_content (SCD Type 2)
INSERT INTO dim_content (
    content_id, title, content_type, release_date, release_year,
    duration_minutes, language, age_rating, primary_genre,
    effective_date, expiry_date, is_current
)
SELECT
    c.content_id,
    c.title,
    ct.content_type,
    c.release_date,
    EXTRACT(YEAR FROM c.release_date)::INT AS release_year,
    c.duration_minutes,
    c.language,
    c.age_rating,
    (SELECT g.genre_name FROM content_genre cg
     JOIN genre g ON cg.genre_id = g.genre_id
     WHERE cg.content_id = c.content_id
     LIMIT 1) AS primary_genre,
    CURRENT_DATE AS effective_date,
    '9999-12-31'::DATE AS expiry_date,
    TRUE AS is_current
FROM content c
JOIN content_type ct ON c.content_type_id = ct.content_type_id
WHERE NOT EXISTS (
    SELECT 1 FROM dim_content dc
    WHERE dc.content_id = c.content_id AND dc.is_current = TRUE
);

-- Load dim_device
INSERT INTO dim_device (device_id, device_name)
SELECT d.device_id, d.device_name
FROM device d
WHERE NOT EXISTS (
    SELECT 1 FROM dim_device dd WHERE dd.device_id = d.device_id
);

-- Load dim_subscription_plan
INSERT INTO dim_subscription_plan (plan_id, plan_name, monthly_fee, video_quality, max_devices)
SELECT sp.plan_id, sp.plan_name, sp.monthly_fee, sp.video_quality, sp.max_devices
FROM subscription_plan sp
WHERE NOT EXISTS (
    SELECT 1 FROM dim_subscription_plan dsp WHERE dsp.plan_id = sp.plan_id
);

-- Load dim_warehouse
INSERT INTO dim_warehouse (warehouse_id, warehouse_name, city_name, country_name)
SELECT w.warehouse_id, w.warehouse_name, ci.city_name, co.country_name
FROM warehouse w
JOIN city ci ON w.city_id = ci.city_id
JOIN country co ON ci.country_id = co.country_id
WHERE NOT EXISTS (
    SELECT 1 FROM dim_warehouse dw WHERE dw.warehouse_id = w.warehouse_id
);

-- Load dim_inventory_item
INSERT INTO dim_inventory_item (
    inventory_id, content_id, content_title, content_type,
    barcode, warehouse_name, city_name, purchase_date,
    item_condition, item_status, effective_date, expiry_date, is_current
)
SELECT
    ii.inventory_id,
    ii.content_id,
    c.title AS content_title,
    ct.content_type,
    ii.barcode,
    w.warehouse_name,
    ci.city_name,
    ii.purchase_date,
    ii.item_condition,
    ii.status AS item_status,
    CURRENT_DATE AS effective_date,
    '9999-12-31'::DATE AS expiry_date,
    TRUE AS is_current
FROM inventory_item ii
JOIN content c ON ii.content_id = c.content_id
JOIN content_type ct ON c.content_type_id = ct.content_type_id
JOIN warehouse w ON ii.warehouse_id = w.warehouse_id
JOIN city ci ON w.city_id = ci.city_id
WHERE NOT EXISTS (
    SELECT 1 FROM dim_inventory_item di
    WHERE di.inventory_id = ii.inventory_id AND di.is_current = TRUE
);

-- ============================================================
-- FACT TABLE LOADING QUERIES
-- ============================================================

-- Fact: Customer Daily Activity
INSERT INTO fact_customer_daily_activity (
    customer_key, date_key,
    streaming_session_count, total_streaming_duration_minutes,
    physical_rental_count, returned_item_count,
    total_amount_spent, support_ticket_count,
    wishlist_addition_count, review_count, avg_completion_percentage
)
SELECT
    dc.customer_key,
    TO_CHAR(activity_date, 'YYYYMMDD')::INT AS date_key,
    COALESCE(stream_counts.session_count, 0),
    COALESCE(stream_counts.total_duration, 0),
    COALESCE(rental_counts.rental_count, 0),
    COALESCE(return_counts.return_count, 0),
    COALESCE(payment_totals.total_spent, 0),
    COALESCE(ticket_counts.ticket_count, 0),
    COALESCE(wishlist_counts.wishlist_count, 0),
    COALESCE(review_counts.review_count, 0),
    COALESCE(stream_counts.avg_completion, 0)
FROM (
    SELECT DISTINCT customer_id, activity_date
    FROM (
        SELECT customer_id, start_time::DATE AS activity_date FROM streaming_session
        UNION
        SELECT customer_id, rental_date AS activity_date FROM rental
        UNION
        SELECT customer_id, payment_date::DATE AS activity_date FROM payment
        UNION
        SELECT customer_id, opened_date::DATE AS activity_date FROM support_ticket
        UNION
        SELECT customer_id, added_date::DATE AS activity_date FROM wishlist
        UNION
        SELECT customer_id, review_date::DATE AS activity_date FROM review
    ) all_activities
) activities
JOIN dim_customer dc ON activities.customer_id = dc.customer_id AND dc.is_current = TRUE
LEFT JOIN (
    SELECT customer_id, start_time::DATE AS activity_date,
           COUNT(*) AS session_count,
           SUM(watch_duration) AS total_duration,
           AVG(completion_percentage) AS avg_completion
    FROM streaming_session
    GROUP BY customer_id, start_time::DATE
) stream_counts ON activities.customer_id = stream_counts.customer_id
    AND activities.activity_date = stream_counts.activity_date
LEFT JOIN (
    SELECT customer_id, rental_date AS activity_date, COUNT(*) AS rental_count
    FROM rental
    GROUP BY customer_id, rental_date
) rental_counts ON activities.customer_id = rental_counts.customer_id
    AND activities.activity_date = rental_counts.activity_date
LEFT JOIN (
    SELECT customer_id, return_date AS activity_date, COUNT(*) AS return_count
    FROM rental
    WHERE return_date IS NOT NULL
    GROUP BY customer_id, return_date
) return_counts ON activities.customer_id = return_counts.customer_id
    AND activities.activity_date = return_counts.activity_date
LEFT JOIN (
    SELECT customer_id, payment_date::DATE AS activity_date, SUM(amount) AS total_spent
    FROM payment
    WHERE status = 'completed'
    GROUP BY customer_id, payment_date::DATE
) payment_totals ON activities.customer_id = payment_totals.customer_id
    AND activities.activity_date = payment_totals.activity_date
LEFT JOIN (
    SELECT customer_id, opened_date::DATE AS activity_date, COUNT(*) AS ticket_count
    FROM support_ticket
    GROUP BY customer_id, opened_date::DATE
) ticket_counts ON activities.customer_id = ticket_counts.customer_id
    AND activities.activity_date = ticket_counts.activity_date
LEFT JOIN (
    SELECT customer_id, added_date::DATE AS activity_date, COUNT(*) AS wishlist_count
    FROM wishlist
    GROUP BY customer_id, added_date::DATE
) wishlist_counts ON activities.customer_id = wishlist_counts.customer_id
    AND activities.activity_date = wishlist_counts.activity_date
LEFT JOIN (
    SELECT customer_id, review_date::DATE AS activity_date, COUNT(*) AS review_count
    FROM review
    GROUP BY customer_id, review_date::DATE
) review_counts ON activities.customer_id = review_counts.customer_id
    AND activities.activity_date = review_counts.activity_date
ON CONFLICT (customer_key, date_key) DO UPDATE SET
    streaming_session_count = EXCLUDED.streaming_session_count,
    total_streaming_duration_minutes = EXCLUDED.total_streaming_duration_minutes,
    physical_rental_count = EXCLUDED.physical_rental_count,
    returned_item_count = EXCLUDED.returned_item_count,
    total_amount_spent = EXCLUDED.total_amount_spent,
    support_ticket_count = EXCLUDED.support_ticket_count,
    wishlist_addition_count = EXCLUDED.wishlist_addition_count,
    review_count = EXCLUDED.review_count,
    avg_completion_percentage = EXCLUDED.avg_completion_percentage;

-- Fact: Content Monthly Performance
INSERT INTO fact_content_monthly_performance (
    content_key, date_key,
    total_stream_count, total_rental_count, total_revenue,
    average_customer_rating, total_ratings_count,
    wishlist_addition_count, unique_streamers, unique_renters,
    total_streaming_minutes, avg_completion_percentage
)
SELECT
    dcon.content_key,
    TO_CHAR(DATE_TRUNC('month', activity_month), 'YYYYMMDD')::INT AS date_key,
    COALESCE(streams.stream_count, 0),
    COALESCE(rentals.rental_count, 0),
    COALESCE(rentals.total_revenue, 0),
    COALESCE(reviews.avg_rating, 0),
    COALESCE(reviews.rating_count, 0),
    COALESCE(wishlists.wishlist_count, 0),
    COALESCE(streams.unique_streamers, 0),
    COALESCE(rentals.unique_renters, 0),
    COALESCE(streams.total_minutes, 0),
    COALESCE(streams.avg_completion, 0)
FROM (
    SELECT DISTINCT content_id, activity_month
    FROM (
        SELECT content_id, DATE_TRUNC('month', start_time)::DATE AS activity_month FROM streaming_session
        UNION
        SELECT ii.content_id, DATE_TRUNC('month', r.rental_date)::DATE AS activity_month
        FROM rental r JOIN inventory_item ii ON r.inventory_id = ii.inventory_id
        UNION
        SELECT content_id, DATE_TRUNC('month', review_date)::DATE AS activity_month FROM review
        UNION
        SELECT content_id, DATE_TRUNC('month', added_date)::DATE AS activity_month FROM wishlist
    ) all_content_activity
) content_months
JOIN dim_content dcon ON content_months.content_id = dcon.content_id AND dcon.is_current = TRUE
LEFT JOIN (
    SELECT content_id, DATE_TRUNC('month', start_time)::DATE AS activity_month,
           COUNT(*) AS stream_count,
           COUNT(DISTINCT customer_id) AS unique_streamers,
           SUM(watch_duration) AS total_minutes,
           AVG(completion_percentage) AS avg_completion
    FROM streaming_session
    GROUP BY content_id, DATE_TRUNC('month', start_time)::DATE
) streams ON content_months.content_id = streams.content_id
    AND content_months.activity_month = streams.activity_month
LEFT JOIN (
    SELECT ii.content_id, DATE_TRUNC('month', r.rental_date)::DATE AS activity_month,
           COUNT(*) AS rental_count,
           COUNT(DISTINCT r.customer_id) AS unique_renters,
           SUM(r.rental_fee + COALESCE(r.late_fee, 0)) AS total_revenue
    FROM rental r
    JOIN inventory_item ii ON r.inventory_id = ii.inventory_id
    GROUP BY ii.content_id, DATE_TRUNC('month', r.rental_date)::DATE
) rentals ON content_months.content_id = rentals.content_id
    AND content_months.activity_month = rentals.activity_month
LEFT JOIN (
    SELECT content_id, DATE_TRUNC('month', review_date)::DATE AS activity_month,
           AVG(rating) AS avg_rating, COUNT(*) AS rating_count
    FROM review
    GROUP BY content_id, DATE_TRUNC('month', review_date)::DATE
) reviews ON content_months.content_id = reviews.content_id
    AND content_months.activity_month = reviews.activity_month
LEFT JOIN (
    SELECT content_id, DATE_TRUNC('month', added_date)::DATE AS activity_month,
           COUNT(*) AS wishlist_count
    FROM wishlist
    GROUP BY content_id, DATE_TRUNC('month', added_date)::DATE
) wishlists ON content_months.content_id = wishlists.content_id
    AND content_months.activity_month = wishlists.activity_month
ON CONFLICT (content_key, date_key) DO UPDATE SET
    total_stream_count = EXCLUDED.total_stream_count,
    total_rental_count = EXCLUDED.total_rental_count,
    total_revenue = EXCLUDED.total_revenue,
    average_customer_rating = EXCLUDED.average_customer_rating,
    total_ratings_count = EXCLUDED.total_ratings_count,
    wishlist_addition_count = EXCLUDED.wishlist_addition_count,
    unique_streamers = EXCLUDED.unique_streamers,
    unique_renters = EXCLUDED.unique_renters,
    total_streaming_minutes = EXCLUDED.total_streaming_minutes,
    avg_completion_percentage = EXCLUDED.avg_completion_percentage;

-- Fact: Inventory Utilisation (Daily Snapshot)
INSERT INTO fact_inventory_utilisation (
    inventory_key, date_key, warehouse_key,
    rental_count, return_count, is_available, is_rented_out,
    days_available, cumulative_days_rented, cumulative_total_days,
    utilisation_percentage
)
SELECT
    di.inventory_key,
    TO_CHAR(snapshot_date, 'YYYYMMDD')::INT AS date_key,
    dw.warehouse_key,
    COALESCE(daily_rentals.rental_count, 0) AS rental_count,
    COALESCE(daily_returns.return_count, 0) AS return_count,
    CASE WHEN active_rental.inventory_id IS NULL THEN 1 ELSE 0 END AS is_available,
    CASE WHEN active_rental.inventory_id IS NOT NULL THEN 1 ELSE 0 END AS is_rented_out,
    CASE WHEN active_rental.inventory_id IS NULL THEN 1 ELSE 0 END AS days_available,
    (snapshot_date - ii.purchase_date) -
        (SELECT COUNT(*) FROM generate_series(ii.purchase_date, snapshot_date, '1 day'::INTERVAL) d
         WHERE NOT EXISTS (
             SELECT 1 FROM rental r2
             WHERE r2.inventory_id = ii.inventory_id
               AND r2.rental_date <= d::DATE
               AND (r2.return_date IS NULL OR r2.return_date >= d::DATE)
         )
        ) AS cumulative_days_rented,
    (snapshot_date - ii.purchase_date) AS cumulative_total_days,
    CASE
        WHEN (snapshot_date - ii.purchase_date) > 0 THEN
            ROUND(
                (
                    (snapshot_date - ii.purchase_date) -
                    (SELECT COUNT(*) FROM generate_series(ii.purchase_date, snapshot_date, '1 day'::INTERVAL) d
                     WHERE NOT EXISTS (
                         SELECT 1 FROM rental r2
                         WHERE r2.inventory_id = ii.inventory_id
                           AND r2.rental_date <= d::DATE
                           AND (r2.return_date IS NULL OR r2.return_date >= d::DATE)
                     ))
                )::DECIMAL / (snapshot_date - ii.purchase_date) * 100, 2
            )
        ELSE 0
    END AS utilisation_percentage
FROM inventory_item ii
CROSS JOIN (SELECT CURRENT_DATE AS snapshot_date) dates
JOIN dim_inventory_item di ON ii.inventory_id = di.inventory_id AND di.is_current = TRUE
JOIN warehouse w ON ii.warehouse_id = w.warehouse_id
JOIN dim_warehouse dw ON w.warehouse_id = dw.warehouse_id
LEFT JOIN (
    SELECT inventory_id, rental_date, COUNT(*) AS rental_count
    FROM rental
    WHERE rental_date = CURRENT_DATE
    GROUP BY inventory_id, rental_date
) daily_rentals ON ii.inventory_id = daily_rentals.inventory_id
LEFT JOIN (
    SELECT inventory_id, return_date, COUNT(*) AS return_count
    FROM rental
    WHERE return_date = CURRENT_DATE
    GROUP BY inventory_id, return_date
) daily_returns ON ii.inventory_id = daily_returns.inventory_id
LEFT JOIN (
    SELECT DISTINCT inventory_id
    FROM rental
    WHERE rental_date <= CURRENT_DATE
      AND (return_date IS NULL OR return_date > CURRENT_DATE)
      AND status = 'active'
) active_rental ON ii.inventory_id = active_rental.inventory_id
ON CONFLICT (inventory_key, date_key) DO UPDATE SET
    rental_count = EXCLUDED.rental_count,
    return_count = EXCLUDED.return_count,
    is_available = EXCLUDED.is_available,
    is_rented_out = EXCLUDED.is_rented_out,
    days_available = EXCLUDED.days_available,
    cumulative_days_rented = EXCLUDED.cumulative_days_rented,
    cumulative_total_days = EXCLUDED.cumulative_total_days,
    utilisation_percentage = EXCLUDED.utilisation_percentage;
