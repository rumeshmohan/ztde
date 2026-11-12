-- ============================================================
-- BRONZE EXTRACT - Reference / lookup sources
-- Task: extract_reference  (Extract stage, Airflow TaskGroup)
--
-- FULL EXTRACT AS-OF {{ data_interval_end }}, not a windowed
-- incremental extract - same strategy Mini Project 01's NiFi flow
-- used for these low-volume, slowly-changing sources ("a full
-- extract every run keeps the SCD comparison simple", see
-- miniproject1/sql/nifi/extraction_queries.sql). This also fixes a
-- real bug an earlier windowed version of this file had: a lookup
-- row created before the DAG's start_date and never updated since
-- would never enter Bronze under a `>= data_interval_start` lower
-- bound, silently starving every table that joins against it. Using
-- only an upper bound (`< data_interval_end`) means every run always
-- sees the complete lookup set as it stood at the end of that day.
--
-- Still genuinely Jinja-templated (real, per-run values, not
-- hardcoded), and still idempotent via ON CONFLICT DO UPDATE -
-- re-running the same day converges to the same rows rather than
-- duplicating them. See sql/bronze/extract/05_extract_events.sql for
-- the true incremental-window pattern used on the high-volume event
-- sources, where a windowed extract is the correct choice.
-- ============================================================

INSERT INTO bronze.raw_country (country_id, country_name, country_code, created_at, updated_at, batch_id)
SELECT country_id, country_name, country_code, created_at, updated_at, '{{ run_id }}'
FROM source.country
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (country_id) DO UPDATE SET
    country_name = EXCLUDED.country_name, country_code = EXCLUDED.country_code,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_city (city_id, country_id, city_name, created_at, updated_at, batch_id)
SELECT city_id, country_id, city_name, created_at, updated_at, '{{ run_id }}'
FROM source.city
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (city_id) DO UPDATE SET
    country_id = EXCLUDED.country_id, city_name = EXCLUDED.city_name,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_content_type (content_type_id, content_type, created_at, updated_at, batch_id)
SELECT content_type_id, content_type, created_at, updated_at, '{{ run_id }}'
FROM source.content_type
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (content_type_id) DO UPDATE SET
    content_type = EXCLUDED.content_type,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_genre (genre_id, genre_name, created_at, updated_at, batch_id)
SELECT genre_id, genre_name, created_at, updated_at, '{{ run_id }}'
FROM source.genre
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (genre_id) DO UPDATE SET
    genre_name = EXCLUDED.genre_name,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_device (device_id, device_name, created_at, updated_at, batch_id)
SELECT device_id, device_name, created_at, updated_at, '{{ run_id }}'
FROM source.device
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (device_id) DO UPDATE SET
    device_name = EXCLUDED.device_name,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_subscription_plan (plan_id, plan_name, monthly_fee, video_quality, max_devices, created_at, updated_at, batch_id)
SELECT plan_id, plan_name, monthly_fee, video_quality, max_devices, created_at, updated_at, '{{ run_id }}'
FROM source.subscription_plan
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (plan_id) DO UPDATE SET
    plan_name = EXCLUDED.plan_name, monthly_fee = EXCLUDED.monthly_fee,
    video_quality = EXCLUDED.video_quality, max_devices = EXCLUDED.max_devices,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_support_category (category_id, category_name, created_at, updated_at, batch_id)
SELECT category_id, category_name, created_at, updated_at, '{{ run_id }}'
FROM source.support_category
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (category_id) DO UPDATE SET
    category_name = EXCLUDED.category_name,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_warehouse (warehouse_id, city_id, warehouse_name, created_at, updated_at, batch_id)
SELECT warehouse_id, city_id, warehouse_name, created_at, updated_at, '{{ run_id }}'
FROM source.warehouse
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (warehouse_id) DO UPDATE SET
    city_id = EXCLUDED.city_id, warehouse_name = EXCLUDED.warehouse_name,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;
