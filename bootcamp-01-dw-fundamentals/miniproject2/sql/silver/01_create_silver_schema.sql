-- ============================================================
-- SILVER LAYER
-- Validated, cleansed, conformed - the same 13 tables Mini Project
-- 01 called staging.stg_* (identical column shapes; the schema name
-- now signals the layer, so the stg_ prefix is dropped). This is
-- also where the denormalising joins Mini Project 01's NiFi flow did
-- at extraction time now happen (against Bronze, not the live
-- operational DB), since Bronze must stay a raw 1:1 copy per table.
--
-- Run this in: abc_hub_analytical
-- ============================================================

CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.customer CASCADE;
CREATE TABLE silver.customer (
    customer_id        BIGINT PRIMARY KEY,
    customer_no        VARCHAR(50)  NOT NULL,
    first_name         VARCHAR(100) NOT NULL,
    last_name          VARCHAR(100) NOT NULL,
    email              VARCHAR(200),
    phone              VARCHAR(50),
    date_of_birth      DATE,
    gender             VARCHAR(20),
    registration_date  DATE         NOT NULL,
    status             VARCHAR(20)  NOT NULL,
    city_name          VARCHAR(100),
    country_name       VARCHAR(100),
    plan_name          VARCHAR(100),
    monthly_fee        DECIMAL(10,2),
    load_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id           VARCHAR(150),
    record_source      VARCHAR(100) DEFAULT 'bronze.raw_customer'
);

DROP TABLE IF EXISTS silver.content CASCADE;
CREATE TABLE silver.content (
    content_id       BIGINT PRIMARY KEY,
    title            VARCHAR(500) NOT NULL,
    content_type     VARCHAR(50)  NOT NULL,
    release_date     DATE,
    duration_minutes INT,
    -- kept as content_language (not "language") purely for continuity
    -- with the Mini Project 01 procedures being reused unchanged
    content_language VARCHAR(50),
    age_rating       VARCHAR(20),
    primary_genre    VARCHAR(100),
    load_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id         VARCHAR(150),
    record_source    VARCHAR(100) DEFAULT 'bronze.raw_content'
);

DROP TABLE IF EXISTS silver.device CASCADE;
CREATE TABLE silver.device (
    device_id      BIGINT PRIMARY KEY,
    device_name    VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id       VARCHAR(150),
    record_source  VARCHAR(100) DEFAULT 'bronze.raw_device'
);

DROP TABLE IF EXISTS silver.subscription_plan CASCADE;
CREATE TABLE silver.subscription_plan (
    plan_id        BIGINT PRIMARY KEY,
    plan_name      VARCHAR(100) NOT NULL,
    monthly_fee    DECIMAL(10,2) NOT NULL,
    video_quality  VARCHAR(50),
    max_devices    INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id       VARCHAR(150),
    record_source  VARCHAR(100) DEFAULT 'bronze.raw_subscription_plan'
);

DROP TABLE IF EXISTS silver.warehouse CASCADE;
CREATE TABLE silver.warehouse (
    warehouse_id   BIGINT PRIMARY KEY,
    warehouse_name VARCHAR(200) NOT NULL,
    city_name      VARCHAR(100),
    country_name   VARCHAR(100),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id       VARCHAR(150),
    record_source  VARCHAR(100) DEFAULT 'bronze.raw_warehouse'
);

DROP TABLE IF EXISTS silver.inventory_item CASCADE;
CREATE TABLE silver.inventory_item (
    inventory_id   BIGINT PRIMARY KEY,
    content_id     BIGINT NOT NULL,
    content_title  VARCHAR(500),
    content_type   VARCHAR(50),
    warehouse_id   BIGINT NOT NULL,
    warehouse_name VARCHAR(200),
    city_name      VARCHAR(100),
    barcode        VARCHAR(100),
    purchase_date  DATE,
    item_condition VARCHAR(50),
    status         VARCHAR(50) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id       VARCHAR(150),
    record_source  VARCHAR(100) DEFAULT 'bronze.raw_inventory_item'
);

DROP TABLE IF EXISTS silver.support_category CASCADE;
CREATE TABLE silver.support_category (
    category_id    BIGINT PRIMARY KEY,
    category_name  VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id       VARCHAR(150),
    record_source  VARCHAR(100) DEFAULT 'bronze.raw_support_category'
);

DROP TABLE IF EXISTS silver.streaming_session CASCADE;
CREATE TABLE silver.streaming_session (
    stream_id             BIGINT PRIMARY KEY,
    customer_id           BIGINT NOT NULL,
    content_id            BIGINT NOT NULL,
    device_id             BIGINT,
    start_time            TIMESTAMP NOT NULL,
    end_time              TIMESTAMP,
    activity_date         DATE      NOT NULL,
    watch_duration        INT       DEFAULT 0,
    completion_percentage DECIMAL(5,2) DEFAULT 0,
    load_timestamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id               VARCHAR(150),
    record_source         VARCHAR(100) DEFAULT 'bronze.raw_streaming_session'
);

DROP TABLE IF EXISTS silver.rental CASCADE;
CREATE TABLE silver.rental (
    rental_id      BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    inventory_id   BIGINT NOT NULL,
    content_id     BIGINT,
    rental_date    DATE   NOT NULL,
    due_date       DATE,
    return_date    DATE,
    rental_fee     DECIMAL(10,2) DEFAULT 0,
    late_fee       DECIMAL(10,2) DEFAULT 0,
    total_fee      DECIMAL(10,2) DEFAULT 0,
    status         VARCHAR(50) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id       VARCHAR(150),
    record_source  VARCHAR(100) DEFAULT 'bronze.raw_rental'
);

DROP TABLE IF EXISTS silver.payment CASCADE;
CREATE TABLE silver.payment (
    payment_id      BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    rental_id       BIGINT,
    subscription_id BIGINT,
    amount          DECIMAL(10,2) NOT NULL,
    payment_date    TIMESTAMP     NOT NULL,
    activity_date   DATE          NOT NULL,
    payment_type    VARCHAR(50)   NOT NULL,
    status          VARCHAR(50)   NOT NULL,
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id        VARCHAR(150),
    record_source   VARCHAR(100) DEFAULT 'bronze.raw_payment'
);

DROP TABLE IF EXISTS silver.support_ticket CASCADE;
CREATE TABLE silver.support_ticket (
    ticket_id      BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    category_id    BIGINT,
    opened_date    TIMESTAMP NOT NULL,
    closed_date    TIMESTAMP,
    activity_date  DATE      NOT NULL,
    priority       VARCHAR(20),
    status         VARCHAR(50) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id       VARCHAR(150),
    record_source  VARCHAR(100) DEFAULT 'bronze.raw_support_ticket'
);

DROP TABLE IF EXISTS silver.review CASCADE;
CREATE TABLE silver.review (
    review_id      BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    content_id     BIGINT NOT NULL,
    rating         INT    NOT NULL,
    review_date    TIMESTAMP NOT NULL,
    activity_date  DATE      NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id       VARCHAR(150),
    record_source  VARCHAR(100) DEFAULT 'bronze.raw_review'
);

DROP TABLE IF EXISTS silver.wishlist CASCADE;
CREATE TABLE silver.wishlist (
    wishlist_id    BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    content_id     BIGINT NOT NULL,
    added_date     TIMESTAMP NOT NULL,
    activity_date  DATE      NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id       VARCHAR(150),
    record_source  VARCHAR(100) DEFAULT 'bronze.raw_wishlist'
);

-- ------------------------------------------------------------
-- Data-quality quarantine (Question 4 Part C): every row that
-- fails a Data Validation / Data Quality Validation check is moved
-- here instead of being silently dropped, superseding Mini Project
-- 01's staging.etl_rejected_records.
-- ------------------------------------------------------------
DROP TABLE IF EXISTS silver.dq_rejected CASCADE;
CREATE TABLE silver.dq_rejected (
    reject_id       BIGSERIAL PRIMARY KEY,
    source_table    VARCHAR(100) NOT NULL,
    source_pk       VARCHAR(100),
    reject_reason   VARCHAR(500) NOT NULL,
    batch_id        VARCHAR(150),
    record_payload  JSONB,
    rejected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_stream_cust_date ON silver.streaming_session(customer_id, activity_date);
CREATE INDEX IF NOT EXISTS idx_silver_stream_cont_date ON silver.streaming_session(content_id, activity_date);
CREATE INDEX IF NOT EXISTS idx_silver_rental_cust_date ON silver.rental(customer_id, rental_date);
CREATE INDEX IF NOT EXISTS idx_silver_rental_inv       ON silver.rental(inventory_id);
CREATE INDEX IF NOT EXISTS idx_silver_rental_content   ON silver.rental(content_id);
CREATE INDEX IF NOT EXISTS idx_silver_payment_cust      ON silver.payment(customer_id, activity_date);
CREATE INDEX IF NOT EXISTS idx_silver_ticket_cust       ON silver.support_ticket(customer_id, activity_date);
CREATE INDEX IF NOT EXISTS idx_silver_review_cust       ON silver.review(customer_id, activity_date);
CREATE INDEX IF NOT EXISTS idx_silver_review_content    ON silver.review(content_id, activity_date);
CREATE INDEX IF NOT EXISTS idx_silver_wishlist_cust     ON silver.wishlist(customer_id, activity_date);
CREATE INDEX IF NOT EXISTS idx_silver_wishlist_content  ON silver.wishlist(content_id, activity_date);
CREATE INDEX IF NOT EXISTS idx_dq_rejected_batch        ON silver.dq_rejected(batch_id);
