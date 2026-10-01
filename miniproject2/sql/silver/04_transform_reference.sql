-- ============================================================
-- SILVER TRANSFORM - Reference domain
-- Task: transform_reference  (Silver Transformation stage)
-- device, subscription_plan, support_category are simple pass-
-- throughs; warehouse denormalises city/country the same way
-- customer/inventory do.
-- ============================================================

INSERT INTO silver.device (device_id, device_name, batch_id)
SELECT device_id, device_name, batch_id
FROM bronze.raw_device
WHERE batch_id = '{{ run_id }}'
ON CONFLICT (device_id) DO UPDATE SET
    device_name = EXCLUDED.device_name, batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;

INSERT INTO silver.subscription_plan (plan_id, plan_name, monthly_fee, video_quality, max_devices, batch_id)
SELECT plan_id, plan_name, monthly_fee, video_quality, max_devices, batch_id
FROM bronze.raw_subscription_plan
WHERE batch_id = '{{ run_id }}'
ON CONFLICT (plan_id) DO UPDATE SET
    plan_name = EXCLUDED.plan_name, monthly_fee = EXCLUDED.monthly_fee,
    video_quality = EXCLUDED.video_quality, max_devices = EXCLUDED.max_devices,
    batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;

INSERT INTO silver.support_category (category_id, category_name, batch_id)
SELECT category_id, category_name, batch_id
FROM bronze.raw_support_category
WHERE batch_id = '{{ run_id }}'
ON CONFLICT (category_id) DO UPDATE SET
    category_name = EXCLUDED.category_name, batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;

INSERT INTO silver.warehouse (warehouse_id, warehouse_name, city_name, country_name, batch_id)
SELECT w.warehouse_id, w.warehouse_name, ci.city_name, co.country_name, w.batch_id
FROM bronze.raw_warehouse w
JOIN bronze.raw_city    ci ON ci.city_id    = w.city_id
JOIN bronze.raw_country co ON co.country_id = ci.country_id
WHERE w.batch_id = '{{ run_id }}'
ON CONFLICT (warehouse_id) DO UPDATE SET
    warehouse_name = EXCLUDED.warehouse_name, city_name = EXCLUDED.city_name,
    country_name = EXCLUDED.country_name, batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;
