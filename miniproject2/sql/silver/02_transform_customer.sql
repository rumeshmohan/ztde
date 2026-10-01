-- ============================================================
-- SILVER TRANSFORM - Customer domain
-- Task: transform_customer  (Silver Transformation stage)
--
-- Denormalises home address (city/country) and the active
-- subscription plan against Bronze (not the live operational DB -
-- Silver only ever reads Bronze). Excludes exactly the rows Section
-- 1a of sql/quality/01_data_quality_checks.sql quarantined, so a
-- row can never be both rejected AND loaded.
-- ============================================================

INSERT INTO silver.customer (
    customer_id, customer_no, first_name, last_name, email, phone,
    date_of_birth, gender, registration_date, status,
    city_name, country_name, plan_name, monthly_fee, batch_id
)
SELECT
    c.customer_id, c.customer_no, c.first_name, c.last_name, c.email, c.phone,
    c.date_of_birth, c.gender, c.registration_date, INITCAP(TRIM(c.status)),
    ci.city_name, co.country_name, sp.plan_name, sp.monthly_fee, c.batch_id
FROM bronze.raw_customer c
LEFT JOIN bronze.raw_customer_address ca
       ON ca.customer_id = c.customer_id AND LOWER(TRIM(ca.address_type)) = 'home'
LEFT JOIN bronze.raw_city    ci ON ci.city_id    = ca.city_id
LEFT JOIN bronze.raw_country co ON co.country_id = ci.country_id
LEFT JOIN bronze.raw_customer_subscription cs
       ON cs.customer_id = c.customer_id AND LOWER(TRIM(cs.status)) = 'active'
LEFT JOIN bronze.raw_subscription_plan sp ON sp.plan_id = cs.plan_id
WHERE c.batch_id = '{{ run_id }}'
  AND NOT (c.customer_no IS NULL OR c.first_name IS NULL OR c.last_name IS NULL
           OR c.registration_date IS NULL OR c.status IS NULL)
ON CONFLICT (customer_id) DO UPDATE SET
    customer_no = EXCLUDED.customer_no, first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name, email = EXCLUDED.email, phone = EXCLUDED.phone,
    date_of_birth = EXCLUDED.date_of_birth, gender = EXCLUDED.gender,
    registration_date = EXCLUDED.registration_date, status = EXCLUDED.status,
    city_name = EXCLUDED.city_name, country_name = EXCLUDED.country_name,
    plan_name = EXCLUDED.plan_name, monthly_fee = EXCLUDED.monthly_fee,
    batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;
