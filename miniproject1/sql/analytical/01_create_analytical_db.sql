-- ============================================================
-- ABC Hub Analytical Database
-- Star Schema Implementation
-- Run this in: abc_hub_analytical
-- ============================================================

-- ============================================================
-- SHARED DIMENSION TABLES
-- ============================================================

-- Dim Date - Shared across all fact tables
CREATE TABLE dim_date (
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

-- Dim Customer
CREATE TABLE dim_customer (
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

-- Dim Content
CREATE TABLE dim_content (
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

-- Dim Device
CREATE TABLE dim_device (
    device_key BIGSERIAL PRIMARY KEY,
    device_id BIGINT NOT NULL,
    device_name VARCHAR(100) NOT NULL
);

-- Dim Subscription Plan
CREATE TABLE dim_subscription_plan (
    plan_key BIGSERIAL PRIMARY KEY,
    plan_id BIGINT NOT NULL,
    plan_name VARCHAR(100) NOT NULL,
    monthly_fee DECIMAL(10,2) NOT NULL,
    video_quality VARCHAR(50),
    max_devices INT
);

-- Dim Warehouse
CREATE TABLE dim_warehouse (
    warehouse_key BIGSERIAL PRIMARY KEY,
    warehouse_id BIGINT NOT NULL,
    warehouse_name VARCHAR(200) NOT NULL,
    city_name VARCHAR(100),
    country_name VARCHAR(100)
);

-- Dim Inventory Item
CREATE TABLE dim_inventory_item (
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

-- Dim Support Category
CREATE TABLE dim_support_category (
    category_key BIGSERIAL PRIMARY KEY,
    category_id BIGINT NOT NULL,
    category_name VARCHAR(100) NOT NULL
);
