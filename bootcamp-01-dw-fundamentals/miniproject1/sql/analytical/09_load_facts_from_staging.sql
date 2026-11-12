-- ============================================================
-- FACT LOAD PROCEDURES
--
-- Invoked by NiFi in process group PG_04_Warehouse_Load, AFTER all
-- four dimension procedures have completed. Loading dimensions first
-- guarantees every fact row finds the surrogate key it needs, which
-- is what maintains referential integrity.
--
-- Each procedure aggregates ALREADY-CLEANSED staging data into one of
-- the three analytical models designed in Question 7, and uses
-- INSERT ... ON CONFLICT DO UPDATE so that repeated pipeline
-- executions restate existing rows instead of duplicating them.
--
-- Run in: abc_hub_analytical
-- ============================================================


-- ============================================================
-- SP 5 : fact_customer_daily_activity   (Scenario A)
-- Grain - one row per customer per day
--
-- The INNER JOIN to dim_customer / dim_date is deliberate: it is the
-- referential integrity guard. An activity row whose customer has not
-- yet reached the dimension is held back rather than loaded with a
-- broken key.
-- ============================================================
CREATE OR REPLACE PROCEDURE staging.sp_load_fact_customer_daily()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    INSERT INTO fact_customer_daily_activity (
        customer_key, date_key,
        streaming_session_count, total_streaming_duration_minutes,
        physical_rental_count, returned_item_count,
        total_amount_spent, support_ticket_count,
        wishlist_addition_count, review_count, avg_completion_percentage
    )
    WITH activity_dates AS (
        -- every customer-day on which ANY business event occurred
        SELECT customer_id, activity_date FROM staging.stg_streaming_session
        UNION
        SELECT customer_id, rental_date   FROM staging.stg_rental
        UNION
        SELECT customer_id, return_date   FROM staging.stg_rental WHERE return_date IS NOT NULL
        UNION
        SELECT customer_id, activity_date FROM staging.stg_payment WHERE status = 'completed'
        UNION
        SELECT customer_id, activity_date FROM staging.stg_support_ticket
        UNION
        SELECT customer_id, activity_date FROM staging.stg_wishlist
        UNION
        SELECT customer_id, activity_date FROM staging.stg_review
    ),
    streams AS (
        SELECT customer_id, activity_date,
               COUNT(*)                    AS session_count,
               SUM(watch_duration)         AS total_duration,
               AVG(completion_percentage)  AS avg_completion
        FROM staging.stg_streaming_session
        GROUP BY customer_id, activity_date
    ),
    rentals AS (
        SELECT customer_id, rental_date AS activity_date, COUNT(*) AS rental_count
        FROM staging.stg_rental
        GROUP BY customer_id, rental_date
    ),
    returns AS (
        SELECT customer_id, return_date AS activity_date, COUNT(*) AS return_count
        FROM staging.stg_rental
        WHERE return_date IS NOT NULL
        GROUP BY customer_id, return_date
    ),
    spend AS (
        SELECT customer_id, activity_date, SUM(amount) AS total_spent
        FROM staging.stg_payment
        WHERE status = 'completed'
        GROUP BY customer_id, activity_date
    ),
    tickets AS (
        SELECT customer_id, activity_date, COUNT(*) AS ticket_count
        FROM staging.stg_support_ticket
        GROUP BY customer_id, activity_date
    ),
    wishlists AS (
        SELECT customer_id, activity_date, COUNT(*) AS wishlist_count
        FROM staging.stg_wishlist
        GROUP BY customer_id, activity_date
    ),
    reviews AS (
        SELECT customer_id, activity_date, COUNT(*) AS review_count
        FROM staging.stg_review
        GROUP BY customer_id, activity_date
    )
    SELECT
        dc.customer_key,
        TO_CHAR(a.activity_date, 'YYYYMMDD')::INT,
        COALESCE(st.session_count,   0),
        COALESCE(st.total_duration,  0),
        COALESCE(rn.rental_count,    0),
        COALESCE(rt.return_count,    0),
        COALESCE(sp.total_spent,     0.00),
        COALESCE(tk.ticket_count,    0),
        COALESCE(wl.wishlist_count,  0),
        COALESCE(rv.review_count,    0),
        ROUND(COALESCE(st.avg_completion, 0), 2)
    FROM activity_dates a
    JOIN dim_customer dc ON dc.customer_id = a.customer_id AND dc.is_current = TRUE
    JOIN dim_date     dd ON dd.date_key    = TO_CHAR(a.activity_date, 'YYYYMMDD')::INT
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
    INSERT INTO staging.etl_audit_log (process_name, target_table, rows_affected, run_status, notes)
    VALUES ('SP_Load_Fact_Customer_Daily', 'fact_customer_daily_activity', v_rows, 'SUCCESS',
            'Scenario A - customer/day grain');
END;
$$;


-- ============================================================
-- SP 6 : fact_content_monthly_performance   (Scenario B)
-- Grain - one row per content item per month
-- ============================================================
CREATE OR REPLACE PROCEDURE staging.sp_load_fact_content_monthly()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    INSERT INTO fact_content_monthly_performance (
        content_key, date_key,
        total_stream_count, total_rental_count, total_revenue,
        average_customer_rating, total_ratings_count,
        wishlist_addition_count, unique_streamers, unique_renters,
        total_streaming_minutes, avg_completion_percentage
    )
    WITH content_months AS (
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE AS month_start
        FROM staging.stg_streaming_session
        UNION
        SELECT content_id, DATE_TRUNC('month', rental_date)::DATE
        FROM staging.stg_rental WHERE content_id IS NOT NULL
        UNION
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE
        FROM staging.stg_review
        UNION
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE
        FROM staging.stg_wishlist
    ),
    streams AS (
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE AS month_start,
               COUNT(*)                            AS stream_count,
               COUNT(DISTINCT customer_id)         AS unique_streamers,
               SUM(watch_duration)                 AS total_minutes,
               AVG(completion_percentage)          AS avg_completion
        FROM staging.stg_streaming_session
        GROUP BY content_id, DATE_TRUNC('month', activity_date)::DATE
    ),
    rentals AS (
        SELECT content_id, DATE_TRUNC('month', rental_date)::DATE AS month_start,
               COUNT(*)                    AS rental_count,
               COUNT(DISTINCT customer_id) AS unique_renters,
               SUM(total_fee)              AS revenue
        FROM staging.stg_rental
        WHERE content_id IS NOT NULL
        GROUP BY content_id, DATE_TRUNC('month', rental_date)::DATE
    ),
    ratings AS (
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE AS month_start,
               AVG(rating) AS avg_rating,
               COUNT(*)    AS rating_count
        FROM staging.stg_review
        GROUP BY content_id, DATE_TRUNC('month', activity_date)::DATE
    ),
    wishlists AS (
        SELECT content_id, DATE_TRUNC('month', activity_date)::DATE AS month_start,
               COUNT(*) AS wishlist_count
        FROM staging.stg_wishlist
        GROUP BY content_id, DATE_TRUNC('month', activity_date)::DATE
    )
    SELECT
        dcon.content_key,
        TO_CHAR(cm.month_start, 'YYYYMMDD')::INT,
        COALESCE(st.stream_count,     0),
        COALESCE(rn.rental_count,     0),
        COALESCE(rn.revenue,          0.00),
        ROUND(COALESCE(rt.avg_rating, 0), 2),
        COALESCE(rt.rating_count,     0),
        COALESCE(wl.wishlist_count,   0),
        COALESCE(st.unique_streamers, 0),
        COALESCE(rn.unique_renters,   0),
        COALESCE(st.total_minutes,    0),
        ROUND(COALESCE(st.avg_completion, 0), 2)
    FROM content_months cm
    JOIN dim_content dcon ON dcon.content_id = cm.content_id AND dcon.is_current = TRUE
    JOIN dim_date    dd   ON dd.date_key     = TO_CHAR(cm.month_start, 'YYYYMMDD')::INT
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
    INSERT INTO staging.etl_audit_log (process_name, target_table, rows_affected, run_status, notes)
    VALUES ('SP_Load_Fact_Content_Monthly', 'fact_content_monthly_performance', v_rows, 'SUCCESS',
            'Scenario B - content/month grain');
END;
$$;


-- ============================================================
-- SP 7 : fact_inventory_utilisation   (Scenario C)
-- Grain - one row per physical inventory item per day
-- ============================================================
CREATE OR REPLACE PROCEDURE staging.sp_load_fact_inventory_util()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    INSERT INTO fact_inventory_utilisation (
        inventory_key, date_key, warehouse_key,
        rental_count, return_count, is_available, is_rented_out,
        days_available, cumulative_days_rented, cumulative_total_days,
        utilisation_percentage
    )
    WITH reporting_window AS (
        -- The snapshot is bounded by the period actually covered by the
        -- extracted data rather than by CURRENT_DATE. Snapshotting every
        -- item up to today would fabricate millions of rows for calendar
        -- days on which no business activity has been loaded.
        SELECT
            GREATEST(
                (SELECT MIN(purchase_date) FROM staging.stg_inventory_item),
                DATE_TRUNC('month',
                    (SELECT MIN(rental_date) FROM staging.stg_rental))::DATE
            ) AS window_start,
            LEAST(
                CURRENT_DATE,
                (DATE_TRUNC('month',
                    (SELECT MAX(GREATEST(rental_date, COALESCE(return_date, rental_date)))
                     FROM staging.stg_rental))
                 + INTERVAL '1 month - 1 day')::DATE
            ) AS window_end
    ),
    item_days AS (
        -- one row per inventory item per calendar day inside the window
        SELECT s.inventory_id,
               s.warehouse_id,
               s.purchase_date,
               d.full_date AS snapshot_date
        FROM staging.stg_inventory_item s
        CROSS JOIN reporting_window rw
        JOIN dim_date d
          ON d.full_date BETWEEN GREATEST(s.purchase_date, rw.window_start)
                             AND rw.window_end
    ),
    daily_rentals AS (
        SELECT inventory_id, rental_date AS snapshot_date, COUNT(*) AS rental_count
        FROM staging.stg_rental
        GROUP BY inventory_id, rental_date
    ),
    daily_returns AS (
        SELECT inventory_id, return_date AS snapshot_date, COUNT(*) AS return_count
        FROM staging.stg_rental
        WHERE return_date IS NOT NULL
        GROUP BY inventory_id, return_date
    ),
    on_loan AS (
        -- days on which the item was out with a customer
        -- (an unreturned rental is treated as on loan up to the window end)
        SELECT r.inventory_id, d.full_date AS snapshot_date
        FROM staging.stg_rental r
        CROSS JOIN reporting_window rw
        JOIN dim_date d
          ON d.full_date >= r.rental_date
         AND d.full_date <= COALESCE(r.return_date, rw.window_end)
        GROUP BY r.inventory_id, d.full_date
    ),
    snapshot AS (
        -- Both cumulative measures are semi-additive running totals over
        -- the reporting window, so utilisation % is always
        -- "days on loan / days observed" for the same period.
        SELECT
            i.inventory_id,
            i.warehouse_id,
            i.snapshot_date,
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
        di.inventory_key,
        TO_CHAR(s.snapshot_date, 'YYYYMMDD')::INT,
        dw.warehouse_key,
        s.rental_count,
        s.return_count,
        s.is_available,
        s.is_rented_out,
        s.is_available AS days_available,
        s.cumulative_days_rented,
        s.cumulative_total_days,
        ROUND(s.cumulative_days_rented::DECIMAL / NULLIF(s.cumulative_total_days, 0) * 100, 2)
    FROM snapshot s
    JOIN dim_inventory_item di ON di.inventory_id = s.inventory_id AND di.is_current = TRUE
    JOIN dim_warehouse      dw ON dw.warehouse_id = s.warehouse_id
    JOIN dim_date           dd ON dd.date_key     = TO_CHAR(s.snapshot_date, 'YYYYMMDD')::INT
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
    INSERT INTO staging.etl_audit_log (process_name, target_table, rows_affected, run_status, notes)
    VALUES ('SP_Load_Fact_Inventory_Util', 'fact_inventory_utilisation', v_rows, 'SUCCESS',
            'Scenario C - inventory item/day periodic snapshot');
END;
$$;
