-- ============================================================
-- NiFi EXTRACTION QUERIES  (Question 8 / Task 1)
--
-- These statements are NOT run manually. Each one is pasted into
-- the "Custom Query" or "SQL select query" property of the matching
-- NiFi processor inside process group  PG_01_Extraction.
--
-- Source connection : OperationalDB_Pool
--                     jdbc:postgresql://localhost:5432/abc_hub_operational
--
-- EXTRACTION STRATEGY
--   Reference / dimension sources  -> FULL EXTRACT
--        Low volume, slowly changing. A full extract keeps the SCD
--        comparison logic simple and cheap.
--   Transactional / fact sources   -> INCREMENTAL EXTRACT
--        QueryDatabaseTableRecord with a Maximum-value Column.
--        NiFi persists the highest value seen in processor state, so
--        each run only pulls rows created since the previous run.
-- ============================================================


-- ============================================================
-- SECTION 1 - FULL EXTRACTS (dimension sources)
-- Processor type: ExecuteSQLRecord
-- ============================================================

-- ------------------------------------------------------------
-- EX_Customer  -> staging.stg_customer
-- Denormalises city / country / current subscription plan so the
-- dimension load does not have to re-join the operational model.
-- ------------------------------------------------------------
SELECT
    c.customer_id,
    c.customer_no,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.date_of_birth,
    c.gender,
    c.registration_date,
    c.status,
    ci.city_name,
    co.country_name,
    sp.plan_name,
    sp.monthly_fee
FROM customer c
LEFT JOIN customer_address ca
       ON ca.customer_id = c.customer_id
      AND ca.address_type = 'home'
LEFT JOIN city    ci ON ci.city_id    = ca.city_id
LEFT JOIN country co ON co.country_id = ci.country_id
LEFT JOIN customer_subscription cs
       ON cs.customer_id = c.customer_id
      AND cs.status = 'active'
LEFT JOIN subscription_plan sp ON sp.plan_id = cs.plan_id;


-- ------------------------------------------------------------
-- EX_Content  -> staging.stg_content
-- content_genre is many-to-many; the alphabetically first genre is
-- taken as the primary genre so dim_content stays at content grain.
-- ------------------------------------------------------------
SELECT
    c.content_id,
    c.title,
    ct.content_type,
    c.release_date,
    c.duration_minutes,
    c.language,
    c.age_rating,
    (
        SELECT MIN(g.genre_name)
        FROM content_genre cg
        JOIN genre g ON g.genre_id = cg.genre_id
        WHERE cg.content_id = c.content_id
    ) AS primary_genre
FROM content c
JOIN content_type ct ON ct.content_type_id = c.content_type_id;


-- ------------------------------------------------------------
-- EX_Device  -> staging.stg_device
-- ------------------------------------------------------------
SELECT device_id, device_name
FROM device;


-- ------------------------------------------------------------
-- EX_SubscriptionPlan  -> staging.stg_subscription_plan
-- ------------------------------------------------------------
SELECT plan_id, plan_name, monthly_fee, video_quality, max_devices
FROM subscription_plan;


-- ------------------------------------------------------------
-- EX_Warehouse  -> staging.stg_warehouse
-- ------------------------------------------------------------
SELECT
    w.warehouse_id,
    w.warehouse_name,
    ci.city_name,
    co.country_name
FROM warehouse w
JOIN city    ci ON ci.city_id    = w.city_id
JOIN country co ON co.country_id = ci.country_id;


-- ------------------------------------------------------------
-- EX_SupportCategory  -> staging.stg_support_category
-- ------------------------------------------------------------
SELECT category_id, category_name
FROM support_category;


-- ------------------------------------------------------------
-- EX_InventoryItem  -> staging.stg_inventory_item
-- Denormalises the content title and the holding warehouse.
-- ------------------------------------------------------------
SELECT
    i.inventory_id,
    i.content_id,
    c.title AS content_title,
    ct.content_type,
    i.warehouse_id,
    w.warehouse_name,
    ci.city_name,
    i.barcode,
    i.purchase_date,
    i.item_condition,
    i.status
FROM inventory_item i
JOIN content      c  ON c.content_id       = i.content_id
JOIN content_type ct ON ct.content_type_id = c.content_type_id
JOIN warehouse    w  ON w.warehouse_id     = i.warehouse_id
JOIN city         ci ON ci.city_id         = w.city_id;


-- ============================================================
-- SECTION 2 - INCREMENTAL EXTRACTS (fact sources)
-- Processor type: QueryDatabaseTableRecord
--
-- Configure the processor properties rather than pasting the whole
-- statement; the query below documents the equivalent SQL.
--
--   Table Name             : <source table>
--   Columns to Return      : <the column list below>
--   Maximum-value Columns  : <the surrogate key / timestamp below>
--
-- The operational model carries no row-level "last_modified"
-- column, so the BIGSERIAL primary key is used as the high-water
-- mark. It is monotonically increasing and never reused, which
-- makes it a safe incremental marker for insert-only event tables.
-- ============================================================

-- ------------------------------------------------------------
-- EX_StreamingSession  -> staging.stg_streaming_session
--   Table Name            : streaming_session
--   Maximum-value Columns : stream_id
-- ------------------------------------------------------------
SELECT
    stream_id,
    customer_id,
    content_id,
    device_id,
    start_time,
    end_time,
    CAST(start_time AS DATE) AS activity_date,
    watch_duration,
    completion_percentage
FROM streaming_session
WHERE stream_id > :last_max_stream_id;      -- managed by NiFi state


-- ------------------------------------------------------------
-- EX_Rental  -> staging.stg_rental
--   Table Name            : rental
--   Maximum-value Columns : rental_id
--
-- rental has no content_id; it is resolved through inventory_item
-- so that Scenario B can attribute rental revenue to a title.
-- total_fee is deliberately NOT calculated here - it is derived by
-- the QueryRecord processor in PG_02_Transformation.
-- ------------------------------------------------------------
SELECT
    r.rental_id,
    r.customer_id,
    r.inventory_id,
    i.content_id,
    r.rental_date,
    r.due_date,
    r.return_date,
    r.rental_fee,
    r.late_fee,
    r.status
FROM rental r
JOIN inventory_item i ON i.inventory_id = r.inventory_id
WHERE r.rental_id > :last_max_rental_id;


-- ------------------------------------------------------------
-- EX_Payment  -> staging.stg_payment
--   Table Name            : payment
--   Maximum-value Columns : payment_id
-- ------------------------------------------------------------
SELECT
    payment_id,
    customer_id,
    rental_id,
    subscription_id,
    amount,
    payment_date,
    CAST(payment_date AS DATE) AS activity_date,
    payment_type,
    status
FROM payment
WHERE payment_id > :last_max_payment_id;


-- ------------------------------------------------------------
-- EX_SupportTicket  -> staging.stg_support_ticket
--   Table Name            : support_ticket
--   Maximum-value Columns : ticket_id
-- ------------------------------------------------------------
SELECT
    ticket_id,
    customer_id,
    category_id,
    opened_date,
    closed_date,
    CAST(opened_date AS DATE) AS activity_date,
    priority,
    status
FROM support_ticket
WHERE ticket_id > :last_max_ticket_id;


-- ------------------------------------------------------------
-- EX_Review  -> staging.stg_review
--   Table Name            : review
--   Maximum-value Columns : review_id
-- ------------------------------------------------------------
SELECT
    review_id,
    customer_id,
    content_id,
    rating,
    review_date,
    CAST(review_date AS DATE) AS activity_date
FROM review
WHERE review_id > :last_max_review_id;


-- ------------------------------------------------------------
-- EX_Wishlist  -> staging.stg_wishlist
--   Table Name            : wishlist
--   Maximum-value Columns : wishlist_id
-- ------------------------------------------------------------
SELECT
    wishlist_id,
    customer_id,
    content_id,
    added_date,
    CAST(added_date AS DATE) AS activity_date
FROM wishlist
WHERE wishlist_id > :last_max_wishlist_id;
