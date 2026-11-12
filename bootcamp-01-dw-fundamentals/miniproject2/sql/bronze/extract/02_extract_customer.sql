-- ============================================================
-- BRONZE EXTRACT - Customer domain
-- Task: extract_customer  (Extract stage, Airflow TaskGroup)
-- Same full-extract-as-of-{{ data_interval_end }} + upsert pattern as
-- 01_extract_reference.sql (see that file's header for why this is a
-- full extract, not a windowed one).
-- ============================================================

INSERT INTO bronze.raw_customer (
    customer_id, customer_no, first_name, last_name, email, phone,
    date_of_birth, gender, registration_date, status, created_at, updated_at, batch_id
)
SELECT
    customer_id, customer_no, first_name, last_name, email, phone,
    date_of_birth, gender, registration_date, status, created_at, updated_at, '{{ run_id }}'
FROM source.customer
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (customer_id) DO UPDATE SET
    customer_no = EXCLUDED.customer_no, first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name, email = EXCLUDED.email, phone = EXCLUDED.phone,
    date_of_birth = EXCLUDED.date_of_birth, gender = EXCLUDED.gender,
    registration_date = EXCLUDED.registration_date, status = EXCLUDED.status,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_customer_address (
    address_id, customer_id, city_id, address_line, postal_code, address_type, created_at, updated_at, batch_id
)
SELECT
    address_id, customer_id, city_id, address_line, postal_code, address_type, created_at, updated_at, '{{ run_id }}'
FROM source.customer_address
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (address_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, city_id = EXCLUDED.city_id,
    address_line = EXCLUDED.address_line, postal_code = EXCLUDED.postal_code,
    address_type = EXCLUDED.address_type,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

INSERT INTO bronze.raw_customer_subscription (
    subscription_id, customer_id, plan_id, start_date, end_date, status, auto_renew, created_at, updated_at, batch_id
)
SELECT
    subscription_id, customer_id, plan_id, start_date, end_date, status, auto_renew, created_at, updated_at, '{{ run_id }}'
FROM source.customer_subscription
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (subscription_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id, plan_id = EXCLUDED.plan_id,
    start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date,
    status = EXCLUDED.status, auto_renew = EXCLUDED.auto_renew,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;
