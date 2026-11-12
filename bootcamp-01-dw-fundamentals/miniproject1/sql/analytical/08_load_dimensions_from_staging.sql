-- ============================================================
-- DIMENSION LOAD PROCEDURES
--
-- Each procedure is invoked by ONE named processor inside the NiFi
-- process group PG_04_Warehouse_Load. Splitting the load into
-- separate callable units lets the NiFi canvas show the mandatory
-- execution order explicitly:
--
--     Reference dims -> Customer -> Content -> Inventory -> Facts
--
-- All procedures are idempotent. Repeated pipeline executions do not
-- create duplicate dimension rows.
--
-- Run in: abc_hub_analytical
-- ============================================================


-- ============================================================
-- SP 1 : Reference dimensions (SCD Type 1 - overwrite)
--
-- These attributes carry no analytical value once superseded, so the
-- prior value is simply overwritten rather than versioned.
-- ============================================================
CREATE OR REPLACE PROCEDURE staging.sp_load_dim_reference()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    -- ---------------- dim_device ----------------
    INSERT INTO dim_device (device_id, device_name)
    SELECT s.device_id, s.device_name
    FROM staging.stg_device s
    WHERE NOT EXISTS (SELECT 1 FROM dim_device d WHERE d.device_id = s.device_id);

    UPDATE dim_device d
    SET device_name = s.device_name
    FROM staging.stg_device s
    WHERE d.device_id = s.device_id
      AND d.device_name IS DISTINCT FROM s.device_name;

    -- ---------------- dim_subscription_plan ----------------
    INSERT INTO dim_subscription_plan (plan_id, plan_name, monthly_fee, video_quality, max_devices)
    SELECT s.plan_id, s.plan_name, s.monthly_fee, s.video_quality, s.max_devices
    FROM staging.stg_subscription_plan s
    WHERE NOT EXISTS (SELECT 1 FROM dim_subscription_plan d WHERE d.plan_id = s.plan_id);

    UPDATE dim_subscription_plan d
    SET plan_name     = s.plan_name,
        monthly_fee   = s.monthly_fee,
        video_quality = s.video_quality,
        max_devices   = s.max_devices
    FROM staging.stg_subscription_plan s
    WHERE d.plan_id = s.plan_id
      AND (d.plan_name     IS DISTINCT FROM s.plan_name
        OR d.monthly_fee   IS DISTINCT FROM s.monthly_fee
        OR d.video_quality IS DISTINCT FROM s.video_quality
        OR d.max_devices   IS DISTINCT FROM s.max_devices);

    -- ---------------- dim_warehouse ----------------
    INSERT INTO dim_warehouse (warehouse_id, warehouse_name, city_name, country_name)
    SELECT s.warehouse_id, s.warehouse_name,
           COALESCE(s.city_name, 'Unknown'), COALESCE(s.country_name, 'Unknown')
    FROM staging.stg_warehouse s
    WHERE NOT EXISTS (SELECT 1 FROM dim_warehouse d WHERE d.warehouse_id = s.warehouse_id);

    UPDATE dim_warehouse d
    SET warehouse_name = s.warehouse_name,
        city_name      = COALESCE(s.city_name,    'Unknown'),
        country_name   = COALESCE(s.country_name, 'Unknown')
    FROM staging.stg_warehouse s
    WHERE d.warehouse_id = s.warehouse_id
      AND (d.warehouse_name IS DISTINCT FROM s.warehouse_name
        OR d.city_name      IS DISTINCT FROM COALESCE(s.city_name,    'Unknown')
        OR d.country_name   IS DISTINCT FROM COALESCE(s.country_name, 'Unknown'));

    -- ---------------- dim_support_category ----------------
    INSERT INTO dim_support_category (category_id, category_name)
    SELECT s.category_id, s.category_name
    FROM staging.stg_support_category s
    WHERE NOT EXISTS (SELECT 1 FROM dim_support_category d WHERE d.category_id = s.category_id);

    UPDATE dim_support_category d
    SET category_name = s.category_name
    FROM staging.stg_support_category s
    WHERE d.category_id = s.category_id
      AND d.category_name IS DISTINCT FROM s.category_name;

    SELECT COUNT(*) INTO v_rows FROM dim_device;
    INSERT INTO staging.etl_audit_log (process_name, target_table, rows_affected, run_status, notes)
    VALUES ('SP_Load_Dim_Reference', 'dim_device/plan/warehouse/category', v_rows, 'SUCCESS',
            'SCD Type 1 reference dimensions loaded');
END;
$$;


-- ============================================================
-- SP 2 : dim_customer (SCD Type 2)
--
-- Tracked attributes: status, city, country, subscription plan.
-- A change expires the current row and inserts a new version, so
-- historical facts stay joined to the attribute values that were
-- true when the event happened.
--
-- Every comparison applies exactly the same COALESCE default that
-- the INSERT applies. Comparing a cleansed dimension value against a
-- raw staging value would make any row with a NULL source attribute
-- look "changed" on every run, creating a new version each execution.
-- ============================================================
CREATE OR REPLACE PROCEDURE staging.sp_load_dim_customer()
LANGUAGE plpgsql
AS $$
DECLARE
    v_expired BIGINT;
    v_added   BIGINT;
BEGIN
    -- Step 1 - expire rows whose tracked attributes have changed
    UPDATE dim_customer d
    SET expiry_date = CURRENT_DATE - INTERVAL '1 day',
        is_current  = FALSE
    FROM staging.stg_customer s
    WHERE d.customer_id = s.customer_id
      AND d.is_current  = TRUE
      AND (
            d.customer_status   IS DISTINCT FROM s.status
         OR d.city_name         IS DISTINCT FROM COALESCE(s.city_name,    'Unknown')
         OR d.country_name      IS DISTINCT FROM COALESCE(s.country_name, 'Unknown')
         OR d.current_plan_name IS DISTINCT FROM s.plan_name
      );
    GET DIAGNOSTICS v_expired = ROW_COUNT;

    -- Step 2 - insert brand new customers AND new versions of changed ones
    INSERT INTO dim_customer (
        customer_id, customer_no, first_name, last_name, full_name,
        email, phone, date_of_birth, gender, age_group,
        registration_date, customer_status, city_name, country_name,
        current_plan_name, current_plan_fee,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.customer_id,
        s.customer_no,
        s.first_name,
        s.last_name,
        s.first_name || ' ' || s.last_name,          -- derived attribute
        s.email,
        s.phone,
        s.date_of_birth,
        COALESCE(s.gender, 'Unknown'),
        CASE                                          -- derived banding
            WHEN s.date_of_birth IS NULL THEN 'Unknown'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) < 18              THEN 'Under 18'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 18 AND 24 THEN '18-24'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 25 AND 34 THEN '25-34'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 35 AND 44 THEN '35-44'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 45 AND 54 THEN '45-54'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+'
        END,
        s.registration_date,
        s.status,
        COALESCE(s.city_name,    'Unknown'),
        COALESCE(s.country_name, 'Unknown'),
        s.plan_name,
        s.monthly_fee,
        CURRENT_DATE,
        '9999-12-31'::DATE,
        TRUE
    FROM staging.stg_customer s
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_customer d
        WHERE d.customer_id = s.customer_id
          AND d.is_current  = TRUE
    );
    GET DIAGNOSTICS v_added = ROW_COUNT;

    INSERT INTO staging.etl_audit_log (process_name, target_table, rows_affected, run_status, notes)
    VALUES ('SP_Load_Dim_Customer', 'dim_customer', v_added, 'SUCCESS',
            FORMAT('SCD2: %s rows expired, %s new versions inserted', v_expired, v_added));
END;
$$;


-- ============================================================
-- SP 3 : dim_content (SCD Type 2)
-- Tracked attributes: title, content type, age rating, primary genre.
-- ============================================================
CREATE OR REPLACE PROCEDURE staging.sp_load_dim_content()
LANGUAGE plpgsql
AS $$
DECLARE
    v_expired BIGINT;
    v_added   BIGINT;
BEGIN
    UPDATE dim_content d
    SET expiry_date = CURRENT_DATE - INTERVAL '1 day',
        is_current  = FALSE
    FROM staging.stg_content s
    WHERE d.content_id = s.content_id
      AND d.is_current = TRUE
      AND (
            d.title         IS DISTINCT FROM s.title
         OR d.content_type  IS DISTINCT FROM s.content_type
         OR d.age_rating    IS DISTINCT FROM COALESCE(s.age_rating,    'Unrated')
         OR d.primary_genre IS DISTINCT FROM COALESCE(s.primary_genre, 'Uncategorised')
      );
    GET DIAGNOSTICS v_expired = ROW_COUNT;

    INSERT INTO dim_content (
        content_id, title, content_type, release_date, release_year,
        duration_minutes, language, age_rating, primary_genre,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.content_id,
        s.title,
        s.content_type,
        s.release_date,
        EXTRACT(YEAR FROM s.release_date)::INT,       -- derived attribute
        s.duration_minutes,
        COALESCE(s.content_language, 'Unknown'),
        COALESCE(s.age_rating,    'Unrated'),
        COALESCE(s.primary_genre, 'Uncategorised'),
        CURRENT_DATE,
        '9999-12-31'::DATE,
        TRUE
    FROM staging.stg_content s
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_content d
        WHERE d.content_id = s.content_id
          AND d.is_current = TRUE
    );
    GET DIAGNOSTICS v_added = ROW_COUNT;

    INSERT INTO staging.etl_audit_log (process_name, target_table, rows_affected, run_status, notes)
    VALUES ('SP_Load_Dim_Content', 'dim_content', v_added, 'SUCCESS',
            FORMAT('SCD2: %s rows expired, %s new versions inserted', v_expired, v_added));
END;
$$;


-- ============================================================
-- SP 4 : dim_inventory_item (SCD Type 2)
-- Tracked attributes: physical condition, status, holding warehouse.
-- Condition history matters because it explains utilisation drops.
-- ============================================================
CREATE OR REPLACE PROCEDURE staging.sp_load_dim_inventory()
LANGUAGE plpgsql
AS $$
DECLARE
    v_expired BIGINT;
    v_added   BIGINT;
BEGIN
    UPDATE dim_inventory_item d
    SET expiry_date = CURRENT_DATE - INTERVAL '1 day',
        is_current  = FALSE
    FROM staging.stg_inventory_item s
    WHERE d.inventory_id = s.inventory_id
      AND d.is_current   = TRUE
      AND (
            d.item_condition IS DISTINCT FROM COALESCE(s.item_condition, 'Unknown')
         OR d.item_status    IS DISTINCT FROM s.status
         OR d.warehouse_name IS DISTINCT FROM s.warehouse_name
      );
    GET DIAGNOSTICS v_expired = ROW_COUNT;

    INSERT INTO dim_inventory_item (
        inventory_id, content_id, content_title, content_type,
        barcode, warehouse_name, city_name, purchase_date,
        item_condition, item_status,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.inventory_id,
        s.content_id,
        s.content_title,
        s.content_type,
        s.barcode,
        s.warehouse_name,
        COALESCE(s.city_name,      'Unknown'),
        s.purchase_date,
        COALESCE(s.item_condition, 'Unknown'),
        s.status,
        CURRENT_DATE,
        '9999-12-31'::DATE,
        TRUE
    FROM staging.stg_inventory_item s
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_inventory_item d
        WHERE d.inventory_id = s.inventory_id
          AND d.is_current   = TRUE
    );
    GET DIAGNOSTICS v_added = ROW_COUNT;

    INSERT INTO staging.etl_audit_log (process_name, target_table, rows_affected, run_status, notes)
    VALUES ('SP_Load_Dim_Inventory', 'dim_inventory_item', v_added, 'SUCCESS',
            FORMAT('SCD2: %s rows expired, %s new versions inserted', v_expired, v_added));
END;
$$;
