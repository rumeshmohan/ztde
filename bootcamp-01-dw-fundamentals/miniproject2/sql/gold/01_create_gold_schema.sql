-- ============================================================
-- GOLD LAYER
-- The star schema Mini Project 01 designed (Question 7, Scenarios
-- A/B/C), moved from the public schema into gold - identical column
-- shapes and constraints, just schema-qualified. This is the layer
-- BI tools and analysts query directly.
--
-- Run this in: abc_hub_analytical
-- ============================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ------------------------------------------------------------
-- SHARED DIMENSIONS
-- ------------------------------------------------------------

DROP TABLE IF EXISTS gold.dim_date CASCADE;
CREATE TABLE gold.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- dim_customer - SCD Type 2 (status, city, country, subscription plan tracked)
DROP TABLE IF EXISTS gold.dim_customer CASCADE;
CREATE TABLE gold.dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    customer_no VARCHAR(50) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    full_name VARCHAR(200) NOT NULL,
    email VARCHAR(200),
    phone VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(20),
    age_group VARCHAR(30),
    registration_date DATE NOT NULL,
    customer_status VARCHAR(20) NOT NULL,
    city_name VARCHAR(100),
    country_name VARCHAR(100),
    current_plan_name VARCHAR(100),
    current_plan_fee DECIMAL(10,2),
    effective_date DATE NOT NULL,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);
CREATE INDEX idx_dim_customer_id_current ON gold.dim_customer(customer_id, is_current);

-- dim_content - SCD Type 2 (title, content type, age rating, primary genre tracked)
DROP TABLE IF EXISTS gold.dim_content CASCADE;
CREATE TABLE gold.dim_content (
    content_key BIGSERIAL PRIMARY KEY,
    content_id BIGINT NOT NULL,
    title VARCHAR(500) NOT NULL,
    content_type VARCHAR(50) NOT NULL,
    release_date DATE,
    release_year INT,
    duration_minutes INT,
    language VARCHAR(50),
    age_rating VARCHAR(20),
    primary_genre VARCHAR(100),
    effective_date DATE NOT NULL,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);
CREATE INDEX idx_dim_content_id_current ON gold.dim_content(content_id, is_current);

-- dim_device - SCD Type 1 (reference)
DROP TABLE IF EXISTS gold.dim_device CASCADE;
CREATE TABLE gold.dim_device (
    device_key BIGSERIAL PRIMARY KEY,
    device_id BIGINT NOT NULL UNIQUE,
    device_name VARCHAR(100) NOT NULL
);

-- dim_subscription_plan - SCD Type 1 (reference)
DROP TABLE IF EXISTS gold.dim_subscription_plan CASCADE;
CREATE TABLE gold.dim_subscription_plan (
    plan_key BIGSERIAL PRIMARY KEY,
    plan_id BIGINT NOT NULL UNIQUE,
    plan_name VARCHAR(100) NOT NULL,
    monthly_fee DECIMAL(10,2) NOT NULL,
    video_quality VARCHAR(50),
    max_devices INT
);

-- dim_warehouse - SCD Type 1 (reference)
DROP TABLE IF EXISTS gold.dim_warehouse CASCADE;
CREATE TABLE gold.dim_warehouse (
    warehouse_key BIGSERIAL PRIMARY KEY,
    warehouse_id BIGINT NOT NULL UNIQUE,
    warehouse_name VARCHAR(200) NOT NULL,
    city_name VARCHAR(100),
    country_name VARCHAR(100)
);

-- dim_inventory_item - SCD Type 2 (condition, status, holding warehouse tracked)
DROP TABLE IF EXISTS gold.dim_inventory_item CASCADE;
CREATE TABLE gold.dim_inventory_item (
    inventory_key BIGSERIAL PRIMARY KEY,
    inventory_id BIGINT NOT NULL,
    content_id BIGINT NOT NULL,
    content_title VARCHAR(500),
    content_type VARCHAR(50),
    barcode VARCHAR(100),
    warehouse_name VARCHAR(200),
    city_name VARCHAR(100),
    purchase_date DATE,
    item_condition VARCHAR(50),
    item_status VARCHAR(50),
    effective_date DATE NOT NULL,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);
CREATE INDEX idx_dim_inventory_id_current ON gold.dim_inventory_item(inventory_id, is_current);

-- dim_support_category - SCD Type 1 (reference)
DROP TABLE IF EXISTS gold.dim_support_category CASCADE;
CREATE TABLE gold.dim_support_category (
    category_key BIGSERIAL PRIMARY KEY,
    category_id BIGINT NOT NULL UNIQUE,
    category_name VARCHAR(100) NOT NULL
);

-- ------------------------------------------------------------
-- FACT TABLES  (Question 7 scenarios A / B / C, unchanged grain
-- and measures - see miniproject1/report/mini_project_report.md
-- Question 7 for the additive/semi-additive justification of every
-- measure below)
-- ------------------------------------------------------------

-- SCENARIO A: Customer Daily Activity - one row per customer per day
DROP TABLE IF EXISTS gold.fact_customer_daily_activity CASCADE;
CREATE TABLE gold.fact_customer_daily_activity (
    customer_key BIGINT NOT NULL REFERENCES gold.dim_customer(customer_key),
    date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
    streaming_session_count INT DEFAULT 0,
    total_streaming_duration_minutes INT DEFAULT 0,
    physical_rental_count INT DEFAULT 0,
    returned_item_count INT DEFAULT 0,
    total_amount_spent DECIMAL(12,2) DEFAULT 0.00,
    support_ticket_count INT DEFAULT 0,
    wishlist_addition_count INT DEFAULT 0,
    review_count INT DEFAULT 0,
    avg_completion_percentage DECIMAL(5,2) DEFAULT 0.00,
    PRIMARY KEY (customer_key, date_key)
);
CREATE INDEX idx_fact_cda_date     ON gold.fact_customer_daily_activity(date_key);
CREATE INDEX idx_fact_cda_customer ON gold.fact_customer_daily_activity(customer_key);

-- SCENARIO B: Content Monthly Performance - one row per content item per month
DROP TABLE IF EXISTS gold.fact_content_monthly_performance CASCADE;
CREATE TABLE gold.fact_content_monthly_performance (
    content_key BIGINT NOT NULL REFERENCES gold.dim_content(content_key),
    date_key INT NOT NULL REFERENCES gold.dim_date(date_key),  -- first day of month
    total_stream_count INT DEFAULT 0,
    total_rental_count INT DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0.00,
    average_customer_rating DECIMAL(3,2) DEFAULT 0.00,
    total_ratings_count INT DEFAULT 0,
    wishlist_addition_count INT DEFAULT 0,
    unique_streamers INT DEFAULT 0,
    unique_renters INT DEFAULT 0,
    total_streaming_minutes INT DEFAULT 0,
    avg_completion_percentage DECIMAL(5,2) DEFAULT 0.00,
    PRIMARY KEY (content_key, date_key)
);
CREATE INDEX idx_fact_cmp_date    ON gold.fact_content_monthly_performance(date_key);
CREATE INDEX idx_fact_cmp_content ON gold.fact_content_monthly_performance(content_key);

-- SCENARIO C: Inventory Utilisation - one row per inventory item per day
DROP TABLE IF EXISTS gold.fact_inventory_utilisation CASCADE;
CREATE TABLE gold.fact_inventory_utilisation (
    inventory_key BIGINT NOT NULL REFERENCES gold.dim_inventory_item(inventory_key),
    date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
    warehouse_key BIGINT NOT NULL REFERENCES gold.dim_warehouse(warehouse_key),
    rental_count INT DEFAULT 0,
    return_count INT DEFAULT 0,
    is_available INT DEFAULT 1,
    is_rented_out INT DEFAULT 0,
    days_available INT DEFAULT 0,
    cumulative_days_rented INT DEFAULT 0,
    cumulative_total_days INT DEFAULT 0,
    utilisation_percentage DECIMAL(5,2) DEFAULT 0.00,
    PRIMARY KEY (inventory_key, date_key)
);
CREATE INDEX idx_fact_iu_date      ON gold.fact_inventory_utilisation(date_key);
CREATE INDEX idx_fact_iu_inventory ON gold.fact_inventory_utilisation(inventory_key);
CREATE INDEX idx_fact_iu_warehouse ON gold.fact_inventory_utilisation(warehouse_key);

-- ------------------------------------------------------------
-- dim_date is populated once (2020-01-01 to 2030-12-31), independent
-- of any DAG run - not a per-batch load.
-- ------------------------------------------------------------
INSERT INTO gold.dim_date (date_key, full_date, day_of_week, day_name, day_of_month,
                           day_of_year, week_of_year, month_number, month_name,
                           quarter, year, is_weekend, is_holiday)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d,
    EXTRACT(DOW  FROM d)::INT,
    TO_CHAR(d, 'Day'),
    EXTRACT(DAY  FROM d)::INT,
    EXTRACT(DOY  FROM d)::INT,
    EXTRACT(WEEK FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(DOW FROM d) IN (0, 6),
    FALSE
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;
