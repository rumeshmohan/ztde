-- ============================================================
-- Analytical Views for Business Reporting
-- These views provide simplified access for analysts and BI tools
-- Run this in: abc_hub_analytical
-- ============================================================

-- ============================================================
-- VIEW: Customer Activity Summary (Monthly)
-- ============================================================
CREATE OR REPLACE VIEW vw_customer_monthly_activity AS
SELECT
    dc.full_name,
    dc.customer_no,
    dc.customer_status,
    dc.city_name,
    dc.country_name,
    dc.current_plan_name,
    dc.age_group,
    dd.year,
    dd.month_name,
    dd.month_number,
    SUM(f.streaming_session_count) AS total_streams,
    SUM(f.total_streaming_duration_minutes) AS total_watch_minutes,
    SUM(f.physical_rental_count) AS total_rentals,
    SUM(f.returned_item_count) AS total_returns,
    SUM(f.total_amount_spent) AS total_revenue,
    SUM(f.support_ticket_count) AS total_tickets,
    COUNT(DISTINCT f.date_key) AS active_days
FROM fact_customer_daily_activity f
JOIN dim_customer dc ON f.customer_key = dc.customer_key
JOIN dim_date dd ON f.date_key = dd.date_key
WHERE dc.is_current = TRUE
GROUP BY
    dc.full_name, dc.customer_no, dc.customer_status,
    dc.city_name, dc.country_name, dc.current_plan_name,
    dc.age_group, dd.year, dd.month_name, dd.month_number;

-- ============================================================
-- VIEW: Content Performance Dashboard
-- ============================================================
CREATE OR REPLACE VIEW vw_content_performance AS
SELECT
    dcon.title,
    dcon.content_type,
    dcon.primary_genre,
    dcon.language,
    dcon.age_rating,
    dcon.release_year,
    dd.year,
    dd.month_name,
    f.total_stream_count,
    f.total_rental_count,
    f.total_revenue,
    f.average_customer_rating,
    f.total_ratings_count,
    f.wishlist_addition_count,
    f.unique_streamers,
    f.unique_renters,
    f.total_streaming_minutes,
    f.avg_completion_percentage
FROM fact_content_monthly_performance f
JOIN dim_content dcon ON f.content_key = dcon.content_key
JOIN dim_date dd ON f.date_key = dd.date_key
WHERE dcon.is_current = TRUE;

-- ============================================================
-- VIEW: Inventory Utilisation Dashboard
-- ============================================================
CREATE OR REPLACE VIEW vw_inventory_utilisation AS
SELECT
    di.content_title,
    di.content_type,
    di.barcode,
    di.warehouse_name,
    di.city_name,
    di.item_condition,
    di.item_status,
    dw.warehouse_name AS warehouse,
    dw.country_name AS warehouse_country,
    dd.full_date,
    dd.month_name,
    dd.year,
    f.rental_count,
    f.return_count,
    f.is_available,
    f.is_rented_out,
    f.utilisation_percentage
FROM fact_inventory_utilisation f
JOIN dim_inventory_item di ON f.inventory_key = di.inventory_key
JOIN dim_warehouse dw ON f.warehouse_key = dw.warehouse_key
JOIN dim_date dd ON f.date_key = dd.date_key
WHERE di.is_current = TRUE;

-- ============================================================
-- VIEW: Revenue Summary
-- ============================================================
CREATE OR REPLACE VIEW vw_revenue_summary AS
SELECT
    dc.country_name,
    dc.city_name,
    dc.current_plan_name,
    dd.year,
    dd.quarter,
    dd.month_name,
    SUM(f.total_amount_spent) AS total_revenue,
    COUNT(DISTINCT f.customer_key) AS paying_customers,
    SUM(f.total_amount_spent) / NULLIF(COUNT(DISTINCT f.customer_key), 0) AS arpu
FROM fact_customer_daily_activity f
JOIN dim_customer dc ON f.customer_key = dc.customer_key
JOIN dim_date dd ON f.date_key = dd.date_key
WHERE dc.is_current = TRUE
  AND f.total_amount_spent > 0
GROUP BY
    dc.country_name, dc.city_name, dc.current_plan_name,
    dd.year, dd.quarter, dd.month_name;

-- ============================================================
-- VIEW: Top Content by Engagement
-- ============================================================
CREATE OR REPLACE VIEW vw_top_content AS
SELECT
    dcon.title,
    dcon.content_type,
    dcon.primary_genre,
    SUM(f.total_stream_count) AS lifetime_streams,
    SUM(f.total_rental_count) AS lifetime_rentals,
    SUM(f.total_revenue) AS lifetime_revenue,
    AVG(f.average_customer_rating) AS overall_rating,
    SUM(f.wishlist_addition_count) AS total_wishlists
FROM fact_content_monthly_performance f
JOIN dim_content dcon ON f.content_key = dcon.content_key
WHERE dcon.is_current = TRUE
GROUP BY dcon.title, dcon.content_type, dcon.primary_genre
ORDER BY SUM(f.total_stream_count) + SUM(f.total_rental_count) DESC;

-- ============================================================
-- VIEW: Warehouse Inventory Health
-- ============================================================
CREATE OR REPLACE VIEW vw_warehouse_health AS
SELECT
    dw.warehouse_name,
    dw.city_name,
    dw.country_name,
    COUNT(DISTINCT f.inventory_key) AS total_items,
    SUM(f.is_available) AS available_items,
    SUM(f.is_rented_out) AS rented_items,
    AVG(f.utilisation_percentage) AS avg_utilisation,
    SUM(CASE WHEN f.utilisation_percentage < 20 THEN 1 ELSE 0 END) AS underutilised_items
FROM fact_inventory_utilisation f
JOIN dim_warehouse dw ON f.warehouse_key = dw.warehouse_key
JOIN dim_date dd ON f.date_key = dd.date_key
WHERE dd.full_date = CURRENT_DATE
GROUP BY dw.warehouse_name, dw.city_name, dw.country_name;
