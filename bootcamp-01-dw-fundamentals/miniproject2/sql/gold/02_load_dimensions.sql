-- ============================================================
-- DIMENSION LOAD PROCEDURES  (SCD Processing stage)
--
-- These are Mini Project 01's sp_load_dim_* procedures
-- (miniproject1/sql/analytical/08_load_dimensions_from_staging.sql),
-- re-pointed from staging.stg_*/public.dim_* to silver.*/gold.dim_* -
-- the SCD1/SCD2 logic itself is unchanged: it was already correct
-- and idempotent, and it is exactly what Mini Project 02 Question 2
-- Part B asks for.
--
-- Invoked by named Airflow tasks inside the SCD Processing
-- TaskGroup, strictly in this order so every task's dependency is
-- explicit on the DAG graph rather than hidden inside one script:
--
--     load_dim_reference -> load_dim_customer -> load_dim_content -> load_dim_inventory
--
-- Each procedure takes p_batch_id as a PARAMETER rather than
-- embedding {{ run_id }} in the procedure body: this file is run
-- ONCE via psql to create the procedures (never Jinja-rendered), so
-- the Airflow task instead templates the CALL statement itself, e.g.
--   CALL gold.sp_load_dim_reference('{{ run_id }}');
-- which is real Jinja, rendered per DAG run - see
-- dags/abc_hub_medallion_pipeline.py.
--
-- Run in: abc_hub_analytical
-- ============================================================


-- ============================================================
-- SCD Type 1 (overwrite) - reference dimensions
-- Week 07/SCD.sql calls this pattern dim_customer_scd1: the prior
-- value carries no analytical need once superseded, so it is simply
-- overwritten rather than versioned.
-- ============================================================
CREATE OR REPLACE PROCEDURE gold.sp_load_dim_reference(p_batch_id VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows BIGINT;
BEGIN
    INSERT INTO gold.dim_device (device_id, device_name)
    SELECT s.device_id, s.device_name
    FROM silver.device s
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_device d WHERE d.device_id = s.device_id);

    UPDATE gold.dim_device d
    SET device_name = s.device_name
    FROM silver.device s
    WHERE d.device_id = s.device_id
      AND d.device_name IS DISTINCT FROM s.device_name;

    INSERT INTO gold.dim_subscription_plan (plan_id, plan_name, monthly_fee, video_quality, max_devices)
    SELECT s.plan_id, s.plan_name, s.monthly_fee, s.video_quality, s.max_devices
    FROM silver.subscription_plan s
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_subscription_plan d WHERE d.plan_id = s.plan_id);

    UPDATE gold.dim_subscription_plan d
    SET plan_name     = s.plan_name,
        monthly_fee   = s.monthly_fee,
        video_quality = s.video_quality,
        max_devices   = s.max_devices
    FROM silver.subscription_plan s
    WHERE d.plan_id = s.plan_id
      AND (d.plan_name     IS DISTINCT FROM s.plan_name
        OR d.monthly_fee   IS DISTINCT FROM s.monthly_fee
        OR d.video_quality IS DISTINCT FROM s.video_quality
        OR d.max_devices   IS DISTINCT FROM s.max_devices);

    INSERT INTO gold.dim_warehouse (warehouse_id, warehouse_name, city_name, country_name)
    SELECT s.warehouse_id, s.warehouse_name,
           COALESCE(s.city_name, 'Unknown'), COALESCE(s.country_name, 'Unknown')
    FROM silver.warehouse s
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_warehouse d WHERE d.warehouse_id = s.warehouse_id);

    UPDATE gold.dim_warehouse d
    SET warehouse_name = s.warehouse_name,
        city_name      = COALESCE(s.city_name,    'Unknown'),
        country_name   = COALESCE(s.country_name, 'Unknown')
    FROM silver.warehouse s
    WHERE d.warehouse_id = s.warehouse_id
      AND (d.warehouse_name IS DISTINCT FROM s.warehouse_name
        OR d.city_name      IS DISTINCT FROM COALESCE(s.city_name,    'Unknown')
        OR d.country_name   IS DISTINCT FROM COALESCE(s.country_name, 'Unknown'));

    INSERT INTO gold.dim_support_category (category_id, category_name)
    SELECT s.category_id, s.category_name
    FROM silver.support_category s
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_support_category d WHERE d.category_id = s.category_id);

    UPDATE gold.dim_support_category d
    SET category_name = s.category_name
    FROM silver.support_category s
    WHERE d.category_id = s.category_id
      AND d.category_name IS DISTINCT FROM s.category_name;

    SELECT COUNT(*) INTO v_rows FROM gold.dim_device;
    INSERT INTO gold.etl_audit_step_log (batch_id, step_name, target_table, rows_affected, notes)
    VALUES (p_batch_id, 'sp_load_dim_reference', 'dim_device/plan/warehouse/category', v_rows,
            'SCD Type 1 reference dimensions loaded');
END;
$$;


-- ============================================================
-- SCD Type 2 - dim_customer
-- Tracked attributes: status, city, country, subscription plan.
-- A change expires the current row and inserts a new version, so
-- historical facts stay joined to the attribute values that were
-- true when the event happened (Week 07/SCD.sql's dim_customer_scd2
-- shape: effective_from/effective_to/is_current - here named
-- effective_date/expiry_date/is_current, matching Mini Project 01's
-- existing column names so nothing downstream has to be renamed).
--
-- p_logical_date is the DAG run's OWN business date ({{ ds }}, i.e.
-- data_interval_start), NOT CURRENT_DATE. Found and fixed after
-- screenshotting real dim_customer rows: every closed version's
-- effective_date/expiry_date had collapsed onto the same real
-- wall-clock day (with expiry_date one day BEFORE its own
-- effective_date - internally inconsistent), because a fast
-- catchup/backfill replays many historical logical dates within one
-- real-time session, and the original CURRENT_DATE-based version
-- stamped every one of them with today's actual date instead of the
-- simulated business date each run represents. Already-existing rows
-- from before this fix keep their (wrong) dates - bronze/silver only
-- retain each customer's LATEST state (upsert, not append-only), so
-- there is no way to recover what the true historical effective date
-- should have been without inventing one; disclosed here rather than
-- silently patched over. New transitions from this fix forward are
-- correctly dated.
-- ============================================================
CREATE OR REPLACE PROCEDURE gold.sp_load_dim_customer(p_batch_id VARCHAR, p_logical_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_expired BIGINT;
    v_added   BIGINT;
BEGIN
    -- Step 1 - expire rows whose tracked attributes have changed
    UPDATE gold.dim_customer d
    SET expiry_date = p_logical_date - INTERVAL '1 day',
        is_current  = FALSE
    FROM silver.customer s
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
    INSERT INTO gold.dim_customer (
        customer_id, customer_no, first_name, last_name, full_name,
        email, phone, date_of_birth, gender, age_group,
        registration_date, customer_status, city_name, country_name,
        current_plan_name, current_plan_fee,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.customer_id, s.customer_no, s.first_name, s.last_name,
        s.first_name || ' ' || s.last_name,
        s.email, s.phone, s.date_of_birth,
        COALESCE(s.gender, 'Unknown'),
        CASE
            WHEN s.date_of_birth IS NULL THEN 'Unknown'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) < 18              THEN 'Under 18'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 18 AND 24 THEN '18-24'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 25 AND 34 THEN '25-34'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 35 AND 44 THEN '35-44'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 45 AND 54 THEN '45-54'
            WHEN EXTRACT(YEAR FROM AGE(s.date_of_birth)) BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+'
        END,
        s.registration_date, s.status,
        COALESCE(s.city_name,    'Unknown'),
        COALESCE(s.country_name, 'Unknown'),
        s.plan_name, s.monthly_fee,
        p_logical_date, '9999-12-31'::DATE, TRUE
    FROM silver.customer s
    WHERE NOT EXISTS (
        SELECT 1 FROM gold.dim_customer d
        WHERE d.customer_id = s.customer_id AND d.is_current = TRUE
    );
    GET DIAGNOSTICS v_added = ROW_COUNT;

    INSERT INTO gold.etl_audit_step_log (batch_id, step_name, target_table, rows_affected, notes)
    VALUES (p_batch_id, 'sp_load_dim_customer', 'dim_customer', v_added,
            FORMAT('SCD2: %s rows expired, %s new versions inserted', v_expired, v_added));
END;
$$;


-- ============================================================
-- SCD Type 2 - dim_content
-- Tracked attributes: title, content type, age rating, primary genre.
-- p_logical_date: see sp_load_dim_customer's header - same
-- CURRENT_DATE -> logical-date fix, same disclosed limitation on
-- already-existing rows.
-- ============================================================
CREATE OR REPLACE PROCEDURE gold.sp_load_dim_content(p_batch_id VARCHAR, p_logical_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_expired BIGINT;
    v_added   BIGINT;
BEGIN
    UPDATE gold.dim_content d
    SET expiry_date = p_logical_date - INTERVAL '1 day',
        is_current  = FALSE
    FROM silver.content s
    WHERE d.content_id = s.content_id
      AND d.is_current = TRUE
      AND (
            d.title         IS DISTINCT FROM s.title
         OR d.content_type  IS DISTINCT FROM s.content_type
         OR d.age_rating    IS DISTINCT FROM COALESCE(s.age_rating,    'Unrated')
         OR d.primary_genre IS DISTINCT FROM COALESCE(s.primary_genre, 'Uncategorised')
      );
    GET DIAGNOSTICS v_expired = ROW_COUNT;

    INSERT INTO gold.dim_content (
        content_id, title, content_type, release_date, release_year,
        duration_minutes, language, age_rating, primary_genre,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.content_id, s.title, s.content_type, s.release_date,
        EXTRACT(YEAR FROM s.release_date)::INT,
        s.duration_minutes,
        COALESCE(s.content_language, 'Unknown'),
        COALESCE(s.age_rating,    'Unrated'),
        COALESCE(s.primary_genre, 'Uncategorised'),
        p_logical_date, '9999-12-31'::DATE, TRUE
    FROM silver.content s
    WHERE NOT EXISTS (
        SELECT 1 FROM gold.dim_content d
        WHERE d.content_id = s.content_id AND d.is_current = TRUE
    );
    GET DIAGNOSTICS v_added = ROW_COUNT;

    INSERT INTO gold.etl_audit_step_log (batch_id, step_name, target_table, rows_affected, notes)
    VALUES (p_batch_id, 'sp_load_dim_content', 'dim_content', v_added,
            FORMAT('SCD2: %s rows expired, %s new versions inserted', v_expired, v_added));
END;
$$;


-- ============================================================
-- SCD Type 2 - dim_inventory_item
-- Tracked attributes: physical condition, status, holding warehouse.
-- Condition history matters because it explains utilisation drops.
-- p_logical_date: see sp_load_dim_customer's header - same
-- CURRENT_DATE -> logical-date fix, same disclosed limitation on
-- already-existing rows.
-- ============================================================
CREATE OR REPLACE PROCEDURE gold.sp_load_dim_inventory(p_batch_id VARCHAR, p_logical_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_expired BIGINT;
    v_added   BIGINT;
BEGIN
    UPDATE gold.dim_inventory_item d
    SET expiry_date = p_logical_date - INTERVAL '1 day',
        is_current  = FALSE
    FROM silver.inventory_item s
    WHERE d.inventory_id = s.inventory_id
      AND d.is_current   = TRUE
      AND (
            d.item_condition IS DISTINCT FROM COALESCE(s.item_condition, 'Unknown')
         OR d.item_status    IS DISTINCT FROM s.status
         OR d.warehouse_name IS DISTINCT FROM s.warehouse_name
      );
    GET DIAGNOSTICS v_expired = ROW_COUNT;

    INSERT INTO gold.dim_inventory_item (
        inventory_id, content_id, content_title, content_type,
        barcode, warehouse_name, city_name, purchase_date,
        item_condition, item_status,
        effective_date, expiry_date, is_current
    )
    SELECT
        s.inventory_id, s.content_id, s.content_title, s.content_type,
        s.barcode, s.warehouse_name, COALESCE(s.city_name, 'Unknown'), s.purchase_date,
        COALESCE(s.item_condition, 'Unknown'), s.status,
        p_logical_date, '9999-12-31'::DATE, TRUE
    FROM silver.inventory_item s
    WHERE NOT EXISTS (
        SELECT 1 FROM gold.dim_inventory_item d
        WHERE d.inventory_id = s.inventory_id AND d.is_current = TRUE
    );
    GET DIAGNOSTICS v_added = ROW_COUNT;

    INSERT INTO gold.etl_audit_step_log (batch_id, step_name, target_table, rows_affected, notes)
    VALUES (p_batch_id, 'sp_load_dim_inventory', 'dim_inventory_item', v_added,
            FORMAT('SCD2: %s rows expired, %s new versions inserted', v_expired, v_added));
END;
$$;
