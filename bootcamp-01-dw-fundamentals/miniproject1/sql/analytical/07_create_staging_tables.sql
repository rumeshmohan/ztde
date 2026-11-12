-- ============================================================
-- STAGING LAYER
-- Landing zone for data extracted and cleansed by Apache NiFi.
-- NiFi validates mandatory fields, handles nulls, and removes
-- duplicates BEFORE writing into these tables.
--
-- Run this in: abc_hub_analytical
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- ------------------------------------------------------------
-- Reference / dimension source staging
-- ------------------------------------------------------------

DROP TABLE IF EXISTS staging.stg_customer CASCADE;
CREATE TABLE staging.stg_customer (
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
    record_source      VARCHAR(100) DEFAULT 'abc_hub_operational.customer'
);

DROP TABLE IF EXISTS staging.stg_content CASCADE;
CREATE TABLE staging.stg_content (
    content_id       BIGINT PRIMARY KEY,
    title            VARCHAR(500) NOT NULL,
    content_type     VARCHAR(50)  NOT NULL,
    release_date     DATE,
    duration_minutes INT,
    -- Named content_language, not language: LANGUAGE is a reserved word in
    -- Apache Calcite, the SQL engine NiFi's QueryRecord processor uses, so
    -- an unquoted 'language' column fails the transformation query. The
    -- warehouse column dim_content.language keeps its natural name; the
    -- rename is confined to the staging layer the pipeline queries.
    content_language VARCHAR(50),
    age_rating       VARCHAR(20),
    primary_genre    VARCHAR(100),
    load_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source    VARCHAR(100) DEFAULT 'abc_hub_operational.content'
);

DROP TABLE IF EXISTS staging.stg_device CASCADE;
CREATE TABLE staging.stg_device (
    device_id      BIGINT PRIMARY KEY,
    device_name    VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source  VARCHAR(100) DEFAULT 'abc_hub_operational.device'
);

DROP TABLE IF EXISTS staging.stg_subscription_plan CASCADE;
CREATE TABLE staging.stg_subscription_plan (
    plan_id        BIGINT PRIMARY KEY,
    plan_name      VARCHAR(100) NOT NULL,
    monthly_fee    DECIMAL(10,2) NOT NULL,
    video_quality  VARCHAR(50),
    max_devices    INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source  VARCHAR(100) DEFAULT 'abc_hub_operational.subscription_plan'
);

DROP TABLE IF EXISTS staging.stg_warehouse CASCADE;
CREATE TABLE staging.stg_warehouse (
    warehouse_id   BIGINT PRIMARY KEY,
    warehouse_name VARCHAR(200) NOT NULL,
    city_name      VARCHAR(100),
    country_name   VARCHAR(100),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source  VARCHAR(100) DEFAULT 'abc_hub_operational.warehouse'
);

DROP TABLE IF EXISTS staging.stg_inventory_item CASCADE;
CREATE TABLE staging.stg_inventory_item (
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
    record_source  VARCHAR(100) DEFAULT 'abc_hub_operational.inventory_item'
);

DROP TABLE IF EXISTS staging.stg_support_category CASCADE;
CREATE TABLE staging.stg_support_category (
    category_id    BIGINT PRIMARY KEY,
    category_name  VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source  VARCHAR(100) DEFAULT 'abc_hub_operational.support_category'
);

-- ------------------------------------------------------------
-- Transactional / fact source staging
-- ------------------------------------------------------------

DROP TABLE IF EXISTS staging.stg_streaming_session CASCADE;
CREATE TABLE staging.stg_streaming_session (
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
    record_source         VARCHAR(100) DEFAULT 'abc_hub_operational.streaming_session'
);

DROP TABLE IF EXISTS staging.stg_rental CASCADE;
CREATE TABLE staging.stg_rental (
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
    record_source  VARCHAR(100) DEFAULT 'abc_hub_operational.rental'
);

DROP TABLE IF EXISTS staging.stg_payment CASCADE;
CREATE TABLE staging.stg_payment (
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
    record_source   VARCHAR(100) DEFAULT 'abc_hub_operational.payment'
);

DROP TABLE IF EXISTS staging.stg_support_ticket CASCADE;
CREATE TABLE staging.stg_support_ticket (
    ticket_id      BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    category_id    BIGINT,
    opened_date    TIMESTAMP NOT NULL,
    closed_date    TIMESTAMP,
    activity_date  DATE      NOT NULL,
    priority       VARCHAR(20),
    status         VARCHAR(50) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source  VARCHAR(100) DEFAULT 'abc_hub_operational.support_ticket'
);

DROP TABLE IF EXISTS staging.stg_review CASCADE;
CREATE TABLE staging.stg_review (
    review_id      BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    content_id     BIGINT NOT NULL,
    rating         INT    NOT NULL,
    review_date    TIMESTAMP NOT NULL,
    activity_date  DATE      NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source  VARCHAR(100) DEFAULT 'abc_hub_operational.review'
);

DROP TABLE IF EXISTS staging.stg_wishlist CASCADE;
CREATE TABLE staging.stg_wishlist (
    wishlist_id    BIGINT PRIMARY KEY,
    customer_id    BIGINT NOT NULL,
    content_id     BIGINT NOT NULL,
    added_date     TIMESTAMP NOT NULL,
    activity_date  DATE      NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source  VARCHAR(100) DEFAULT 'abc_hub_operational.wishlist'
);

-- ------------------------------------------------------------
-- Rejected-records table: Task 2 "logging of failed records"
-- NiFi routes records failing validation here as well as to disk.
-- ------------------------------------------------------------

DROP TABLE IF EXISTS staging.etl_rejected_records CASCADE;
CREATE TABLE staging.etl_rejected_records (
    reject_id       BIGSERIAL PRIMARY KEY,
    source_table    VARCHAR(100),
    reject_reason   VARCHAR(500),
    record_payload  TEXT,
    reject_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- ETL audit log: records each pipeline run for traceability
-- ------------------------------------------------------------

DROP TABLE IF EXISTS staging.etl_audit_log CASCADE;
CREATE TABLE staging.etl_audit_log (
    audit_id       BIGSERIAL PRIMARY KEY,
    process_name   VARCHAR(200) NOT NULL,
    target_table   VARCHAR(100),
    rows_affected  BIGINT,
    run_status     VARCHAR(20),
    run_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes          VARCHAR(500)
);

-- ------------------------------------------------------------
-- Indexes to support the fact aggregation queries
-- ------------------------------------------------------------

CREATE INDEX idx_stg_stream_cust_date ON staging.stg_streaming_session(customer_id, activity_date);
CREATE INDEX idx_stg_stream_cont_date ON staging.stg_streaming_session(content_id, activity_date);
CREATE INDEX idx_stg_rental_cust_date ON staging.stg_rental(customer_id, rental_date);
CREATE INDEX idx_stg_rental_inv       ON staging.stg_rental(inventory_id);
CREATE INDEX idx_stg_rental_content   ON staging.stg_rental(content_id);
CREATE INDEX idx_stg_payment_cust     ON staging.stg_payment(customer_id, activity_date);
CREATE INDEX idx_stg_ticket_cust      ON staging.stg_support_ticket(customer_id, activity_date);
CREATE INDEX idx_stg_review_cust      ON staging.stg_review(customer_id, activity_date);
CREATE INDEX idx_stg_review_content   ON staging.stg_review(content_id, activity_date);
CREATE INDEX idx_stg_wishlist_cust    ON staging.stg_wishlist(customer_id, activity_date);
CREATE INDEX idx_stg_wishlist_content ON staging.stg_wishlist(content_id, activity_date);
