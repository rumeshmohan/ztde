-- ============================================================
-- FACT LOAD PROCEDURES  (Gold Load stage)
--
-- Mini Project 01's sp_load_fact_* procedures
-- (miniproject1/sql/analytical/09_load_facts_from_staging.sql),
-- re-pointed from staging.stg_*/public.fact_* to silver.*/gold.fact_*.
-- Invoked AFTER every SCD Processing task has completed, so every
-- fact row can resolve the surrogate key it needs - that ordering is
-- what maintains referential integrity (the INNER JOINs below hold a
-- fact row back rather than load it with a broken key).
--
-- INSERT ... ON CONFLICT DO UPDATE means repeated executions restate
-- existing rows instead of duplicating them - this is the mechanism
-- behind the Question 3 Part D demonstration (Run 1 = 100 rows,
-- Run 2 on the same data = 100 rows, not 200).
--
-- Run in: abc_hub_analytical
-- ============================================================


-- ============================================================
-- fact_customer_daily_activity   (Scenario A) - one row per customer per day
-- ============================================================
CREATE OR REPLACE PROCEDURE gold.sp_load_fact_customer_daily(p_batch_id VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    INSERT INTO gold.fact_customer_daily_activity (
        customer_key, date_key,
        streaming_session_count, total_streaming_duration_minutes,
        physical_rental_count, returned_item_count,
        total_amount_spent, support_ticket_count,
        wishlist_addition_count, review_count, avg_completion_percentage
    )
    WITH activity_dates AS (
        SELECT customer_id, activity_date FROM silver.streaming_session
        UNION
        SELECT customer_id, rental_date   FROM silver.rental
        UNION
        SELECT customer_id, return_date   FROM silver.rental WHERE return_date IS NOT NULL
        UNION
        SELECT customer_id, activity_date FROM silver.payment WHERE status = 'Completed'
        UNION
        SELECT customer_id, activity_date FROM silver.support_ticket
        UNION
        SELECT customer_id, activity_date FROM silver.wishlist
        UNION
        SELECT customer_id, activity_date FROM silver.review
    ),
    streams AS (
        SELECT customer_id, activity_date,
               COUNT(*) AS session_count, SUM(watch_duration) AS total_duration,
               AVG(completion_percentage) AS avg_completion
        FROM silver.streaming_session
        GROUP BY customer_id, activity_date
    ),
    rentals AS (
        SELECT customer_id, rental_date AS activity_date, COUNT(*) AS rental_count
        FROM silver.rental GROUP BY customer_id, rental_date
    ),
    returns AS (
        SELECT customer_id, return_date AS activity_date, COUNT(*) AS return_count
        FROM silver.rental WHERE return_date IS NOT NULL GROUP BY customer_id, return_date
    ),
    spend AS (
        SELECT customer_id, activity_date, SUM(amount) AS total_spent
        FROM silver.payment WHERE status = 'Completed' GROUP BY customer_id, activity_date
    ),
    tickets AS (
        SELECT customer_id, activity_date, COUNT(*) AS ticket_count
        FROM silver.support_ticket GROUP BY customer_id, activity_date
    ),
    wishlists AS (
        SELECT customer_id, activity_date, COUNT(*) AS wishlist_count
        FROM silver.wishlist GROUP BY customer_id, activity_date
    ),
    reviews AS (
        SELECT customer_id, activity_date, COUNT(*) AS review_count
        FROM silver.review GROUP BY customer_id, activity_date
    )
    SELECT
        dc.customer_key,
        TO_CHAR(a.activity_date, 'YYYYMMDD')::INT,
        COALESCE(st.session_count,  0), COALESCE(st.total_duration, 0),
        COALESCE(rn.rental_count,   0), COALESCE(rt.return_count,   0),
        COALESCE(sp.total_spent,    0.00), COALESCE(tk.ticket_count, 0),
        COALESCE(wl.wishlist_count, 0), COALESCE(rv.review_count,   0),
        ROUND(COALESCE(st.avg_completion, 0), 2)
    FROM activity_dates a
    JOIN gold.dim_customer dc ON dc.customer_id = a.customer_id AND dc.is_current = TRUE
    JOIN gold.dim_date     dd ON dd.date_key    = TO_CHAR(a.activity_date, 'YYYYMMDD')::INT
    LEFT JOIN streams   st ON st.customer_id = a.customer_id AND st.activity_date = a.activity_date
    LEFT JOIN rentals   rn ON rn.customer_id = a.customer_id AND rn.activity_date = a.activity_date
    LEFT JOIN returns   rt ON rt.customer_id = a.customer_id AND rt.activity_date = a.activity_date
    LEFT JOIN spend     sp ON sp.customer_id = a.customer_id AND sp.activity_date = a.activity_date
    LEFT JOIN tickets   tk ON tk.customer_id = a.customer_id AND tk.activity_date = a.activity_date
    LEFT JOIN wishlists wl ON wl.customer_id = a.customer_id AND wl.activity_date = a.activity_date
    LEFT JOIN reviews   rv ON rv.customer_id = a.customer_id AND rv.activity_date = a.activity_date
    ON CONFLICT (customer_key, date_key) DO UPDATE SET
        streaming_session_count          = EXCLUDED.streaming_session_count,
        total_streaming_duration_minutes = EXCLUDED.total_streaming_duration_minutes,
        physical_rental_count            = EXCLUDED.physical_rental_count,
        returned_item_count              = EXCLUDED.returned_item_count,
        total_amount_spent               = EXCLUDED.total_amount_spent,
        support_ticket_count             = EXCLUDED.support_ticket_count,
        wishlist_addition_count          = EXCLUDED.wishlist_addition_count,
        review_count                     = EXCLUDED.review_count,
        avg_completion_percentage        = EXCLUDED.avg_completion_percentage;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    INSERT INTO gold.etl_audit_step_log (batch_id, step_name, target_table, rows_affected, notes)
    VALUES (p_batch_id, 'sp_load_fact_customer_daily', 'fact_customer_daily_activity', v_rows,
            'Scenario A - customer/day grain');
END;
$$;


-- ============================================================
-- fact_content_monthly_performance   (Scenario B) - one row per content per month
-- ============================================================
CREATE OR REPLACE PROCEDURE gold.sp_load_fact_content_monthly(p_batch_id VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    INSERT INTO gold.fact_content_monthly_performance (
        content_key, date_key,
        total_stream_count, total_rental_count, total_revenue,
        average_customer_rating, total_ratings_count,
        wishlist_addition_count, unique_streamers, unique_renters,
        total_streaming_minutes, avg_completion_percentage
    )
    WITH content_months AS (
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE AS month_start FROM silver.streaming_session
        UNION
        SELECT content_id, DATE_TRUNC('month', rental_date)::DATE FROM silver.rental WHERE content_id IS NOT NULL
        UNION
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE FROM silver.review
        UNION
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE FROM silver.wishlist
    ),
    streams AS (
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE AS month_start,
               COUNT(*) AS stream_count, COUNT(DISTINCT customer_id) AS unique_streamers,
               SUM(watch_duration) AS total_minutes, AVG(completion_percentage) AS avg_completion
        FROM silver.streaming_session GROUP BY content_id, DATE_TRUNC('month', activity_date)::DATE
    ),
    rentals AS (
        SELECT content_id, DATE_TRUNC('month', rental_date)::DATE AS month_start,
               COUNT(*) AS rental_count, COUNT(DISTINCT customer_id) AS unique_renters, SUM(total_fee) AS revenue
        FROM silver.rental WHERE content_id IS NOT NULL
        GROUP BY content_id, DATE_TRUNC('month', rental_date)::DATE
    ),
    ratings AS (
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE AS month_start,
               AVG(rating) AS avg_rating, COUNT(*) AS rating_count
        FROM silver.review GROUP BY content_id, DATE_TRUNC('month', activity_date)::DATE
    ),
    wishlists AS (
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE AS month_start, COUNT(*) AS wishlist_count
        FROM silver.wishlist GROUP BY content_id, DATE_TRUNC('month', activity_date)::DATE
    )
    SELECT
        dcon.content_key,
        TO_CHAR(cm.month_start, 'YYYYMMDD')::INT,
        COALESCE(st.stream_count,     0), COALESCE(rn.rental_count,     0),
        COALESCE(rn.revenue,          0.00), ROUND(COALESCE(rt.avg_rating, 0), 2),
        COALESCE(rt.rating_count,     0), COALESCE(wl.wishlist_count,   0),
        COALESCE(st.unique_streamers, 0), COALESCE(rn.unique_renters,   0),
        COALESCE(st.total_minutes,    0), ROUND(COALESCE(st.avg_completion, 0), 2)
    FROM content_months cm
    JOIN gold.dim_content dcon ON dcon.content_id = cm.content_id AND dcon.is_current = TRUE
    JOIN gold.dim_date    dd   ON dd.date_key     = TO_CHAR(cm.month_start, 'YYYYMMDD')::INT
    LEFT JOIN streams   st ON st.content_id = cm.content_id AND st.month_start = cm.month_start
    LEFT JOIN rentals   rn ON rn.content_id = cm.content_id AND rn.month_start = cm.month_start
    LEFT JOIN ratings   rt ON rt.content_id = cm.content_id AND rt.month_start = cm.month_start
    LEFT JOIN wishlists wl ON wl.content_id = cm.content_id AND wl.month_start = cm.month_start
    ON CONFLICT (content_key, date_key) DO UPDATE SET
        total_stream_count        = EXCLUDED.total_stream_count,
        total_rental_count        = EXCLUDED.total_rental_count,
        total_revenue             = EXCLUDED.total_revenue,
        average_customer_rating   = EXCLUDED.average_customer_rating,
        total_ratings_count       = EXCLUDED.total_ratings_count,
        wishlist_addition_count   = EXCLUDED.wishlist_addition_count,
        unique_streamers          = EXCLUDED.unique_streamers,
        unique_renters            = EXCLUDED.unique_renters,
        total_streaming_minutes   = EXCLUDED.total_streaming_minutes,
        avg_completion_percentage = EXCLUDED.avg_completion_percentage;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    INSERT INTO gold.etl_audit_step_log (batch_id, step_name, target_table, rows_affected, notes)
    VALUES (p_batch_id, 'sp_load_fact_content_monthly', 'fact_content_monthly_performance', v_rows,
            'Scenario B - content/month grain');
END;
$$;


-- ============================================================
-- fact_inventory_utilisation   (Scenario C) - one row per item per day
-- ============================================================
CREATE OR REPLACE PROCEDURE gold.sp_load_fact_inventory_util(p_batch_id VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    INSERT INTO gold.fact_inventory_utilisation (
        inventory_key, date_key, warehouse_key,
        rental_count, return_count, is_available, is_rented_out,
        days_available, cumulative_days_rented, cumulative_total_days,
        utilisation_percentage
    )
    WITH reporting_window AS (
        SELECT
            GREATEST(
                (SELECT MIN(purchase_date) FROM silver.inventory_item),
                DATE_TRUNC('month', (SELECT MIN(rental_date) FROM silver.rental))::DATE
            ) AS window_start,
            LEAST(
                CURRENT_DATE,
                (DATE_TRUNC('month',
                    (SELECT MAX(GREATEST(rental_date, COALESCE(return_date, rental_date))) FROM silver.rental))
                 + INTERVAL '1 month - 1 day')::DATE
            ) AS window_end
    ),
    item_days AS (
        SELECT s.inventory_id, s.warehouse_id, s.purchase_date, d.full_date AS snapshot_date
        FROM silver.inventory_item s
        CROSS JOIN reporting_window rw
        JOIN gold.dim_date d
          ON d.full_date BETWEEN GREATEST(s.purchase_date, rw.window_start) AND rw.window_end
    ),
    daily_rentals AS (
        SELECT inventory_id, rental_date AS snapshot_date, COUNT(*) AS rental_count
        FROM silver.rental GROUP BY inventory_id, rental_date
    ),
    daily_returns AS (
        SELECT inventory_id, return_date AS snapshot_date, COUNT(*) AS return_count
        FROM silver.rental WHERE return_date IS NOT NULL GROUP BY inventory_id, return_date
    ),
    on_loan AS (
        SELECT r.inventory_id, d.full_date AS snapshot_date
        FROM silver.rental r
        CROSS JOIN reporting_window rw
        JOIN gold.dim_date d
          ON d.full_date >= r.rental_date AND d.full_date <= COALESCE(r.return_date, rw.window_end)
        GROUP BY r.inventory_id, d.full_date
    ),
    snapshot AS (
        SELECT
            i.inventory_id, i.warehouse_id, i.snapshot_date,
            COALESCE(dr.rental_count, 0) AS rental_count,
            COALESCE(rr.return_count, 0) AS return_count,
            CASE WHEN ol.inventory_id IS NULL THEN 1 ELSE 0 END AS is_available,
            CASE WHEN ol.inventory_id IS NULL THEN 0 ELSE 1 END AS is_rented_out,
            COUNT(*) OVER (PARTITION BY i.inventory_id ORDER BY i.snapshot_date
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_total_days,
            SUM(CASE WHEN ol.inventory_id IS NULL THEN 0 ELSE 1 END)
                OVER (PARTITION BY i.inventory_id ORDER BY i.snapshot_date
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_rented
        FROM item_days i
        LEFT JOIN daily_rentals dr ON dr.inventory_id = i.inventory_id AND dr.snapshot_date = i.snapshot_date
        LEFT JOIN daily_returns rr ON rr.inventory_id = i.inventory_id AND rr.snapshot_date = i.snapshot_date
        LEFT JOIN on_loan       ol ON ol.inventory_id = i.inventory_id AND ol.snapshot_date = i.snapshot_date
    )
    SELECT
        di.inventory_key, TO_CHAR(s.snapshot_date, 'YYYYMMDD')::INT, dw.warehouse_key,
        s.rental_count, s.return_count, s.is_available, s.is_rented_out,
        s.is_available, s.cumulative_days_rented, s.cumulative_total_days,
        ROUND(s.cumulative_days_rented::DECIMAL / NULLIF(s.cumulative_total_days, 0) * 100, 2)
    FROM snapshot s
    JOIN gold.dim_inventory_item di ON di.inventory_id = s.inventory_id AND di.is_current = TRUE
    JOIN gold.dim_warehouse      dw ON dw.warehouse_id = s.warehouse_id
    JOIN gold.dim_date           dd ON dd.date_key     = TO_CHAR(s.snapshot_date, 'YYYYMMDD')::INT
    ON CONFLICT (inventory_key, date_key) DO UPDATE SET
        warehouse_key          = EXCLUDED.warehouse_key,
        rental_count           = EXCLUDED.rental_count,
        return_count           = EXCLUDED.return_count,
        is_available           = EXCLUDED.is_available,
        is_rented_out          = EXCLUDED.is_rented_out,
        days_available         = EXCLUDED.days_available,
        cumulative_days_rented = EXCLUDED.cumulative_days_rented,
        cumulative_total_days  = EXCLUDED.cumulative_total_days,
        utilisation_percentage = EXCLUDED.utilisation_percentage;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    INSERT INTO gold.etl_audit_step_log (batch_id, step_name, target_table, rows_affected, notes)
    VALUES (p_batch_id, 'sp_load_fact_inventory_util', 'fact_inventory_utilisation', v_rows,
            'Scenario C - inventory item/day periodic snapshot');
END;
$$;
