-- ============================================================
-- BRONZE LAYER
-- Raw, untransformed, one table per operational source table -
-- column-for-column copies of the operational schema. No joins, no
-- cleansing, no business rules: that is deliberately deferred to
-- Silver. Every row carries three audit columns (Week 07's own term
-- for this: "the audit columns that make a landing zone traceable"):
--   batch_id       - the Airflow run_id that landed this row
--   extracted_at   - when Bronze wrote the row
--   source_system  - which operational table it came from
--
-- Loaded by upsert-on-natural-key (see sql/bronze/extract/*.sql), so
-- Bronze itself holds one current raw row per source key rather than
-- a full change log - re-running the same extraction window is a
-- no-op, which is what keeps the whole pipeline idempotent end to end.
--
-- Run this in: abc_hub_analytical
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.raw_country CASCADE;
CREATE TABLE bronze.raw_country (
    country_id      BIGINT PRIMARY KEY,
    country_name    VARCHAR(100) NOT NULL,
    country_code    VARCHAR(5)   NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.country'
);

DROP TABLE IF EXISTS bronze.raw_city CASCADE;
CREATE TABLE bronze.raw_city (
    city_id         BIGINT PRIMARY KEY,
    country_id      BIGINT NOT NULL,
    city_name       VARCHAR(100) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.city'
);

DROP TABLE IF EXISTS bronze.raw_content_type CASCADE;
CREATE TABLE bronze.raw_content_type (
    content_type_id BIGINT PRIMARY KEY,
    content_type    VARCHAR(50) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.content_type'
);

DROP TABLE IF EXISTS bronze.raw_genre CASCADE;
CREATE TABLE bronze.raw_genre (
    genre_id        BIGINT PRIMARY KEY,
    genre_name      VARCHAR(50) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.genre'
);

DROP TABLE IF EXISTS bronze.raw_device CASCADE;
CREATE TABLE bronze.raw_device (
    device_id       BIGINT PRIMARY KEY,
    device_name     VARCHAR(50) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.device'
);

DROP TABLE IF EXISTS bronze.raw_subscription_plan CASCADE;
CREATE TABLE bronze.raw_subscription_plan (
    plan_id         BIGINT PRIMARY KEY,
    plan_name       VARCHAR(50) NOT NULL,
    monthly_fee     DECIMAL(10,2) NOT NULL,
    video_quality   VARCHAR(20),
    max_devices     INT,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.subscription_plan'
);

DROP TABLE IF EXISTS bronze.raw_support_category CASCADE;
CREATE TABLE bronze.raw_support_category (
    category_id     BIGINT PRIMARY KEY,
    category_name   VARCHAR(100) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.support_category'
);

DROP TABLE IF EXISTS bronze.raw_warehouse CASCADE;
CREATE TABLE bronze.raw_warehouse (
    warehouse_id    BIGINT PRIMARY KEY,
    city_id         BIGINT NOT NULL,
    warehouse_name  VARCHAR(150) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.warehouse'
);

DROP TABLE IF EXISTS bronze.raw_customer CASCADE;
CREATE TABLE bronze.raw_customer (
    customer_id       BIGINT PRIMARY KEY,
    customer_no       VARCHAR(20) NOT NULL,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(150),
    phone             VARCHAR(30),
    date_of_birth     DATE,
    gender            VARCHAR(20),
    registration_date DATE,
    status            VARCHAR(20),
    created_at        TIMESTAMP NOT NULL,
    updated_at        TIMESTAMP NOT NULL,
    batch_id          VARCHAR(150),
    extracted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system     VARCHAR(100) DEFAULT 'abc_hub_operational.customer'
);

DROP TABLE IF EXISTS bronze.raw_customer_address CASCADE;
CREATE TABLE bronze.raw_customer_address (
    address_id      BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    city_id         BIGINT NOT NULL,
    address_line    VARCHAR(255),
    postal_code     VARCHAR(20),
    address_type    VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.customer_address'
);

DROP TABLE IF EXISTS bronze.raw_customer_subscription CASCADE;
CREATE TABLE bronze.raw_customer_subscription (
    subscription_id BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    plan_id         BIGINT NOT NULL,
    start_date      DATE,
    end_date        DATE,
    status          VARCHAR(20),
    auto_renew      BOOLEAN,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.customer_subscription'
);

DROP TABLE IF EXISTS bronze.raw_content CASCADE;
CREATE TABLE bronze.raw_content (
    content_id       BIGINT PRIMARY KEY,
    content_type_id  BIGINT NOT NULL,
    title            VARCHAR(255) NOT NULL,
    release_date     DATE,
    duration_minutes INT,
    language         VARCHAR(50),
    age_rating       VARCHAR(10),
    created_at       TIMESTAMP NOT NULL,
    updated_at       TIMESTAMP NOT NULL,
    batch_id         VARCHAR(150),
    extracted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system    VARCHAR(100) DEFAULT 'abc_hub_operational.content'
);

DROP TABLE IF EXISTS bronze.raw_content_genre CASCADE;
CREATE TABLE bronze.raw_content_genre (
    content_id      BIGINT NOT NULL,
    genre_id        BIGINT NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.content_genre',
    PRIMARY KEY (content_id, genre_id)
);

DROP TABLE IF EXISTS bronze.raw_inventory_item CASCADE;
CREATE TABLE bronze.raw_inventory_item (
    inventory_id    BIGINT PRIMARY KEY,
    content_id      BIGINT NOT NULL,
    warehouse_id    BIGINT NOT NULL,
    barcode         VARCHAR(50),
    purchase_date   DATE,
    item_condition  VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.inventory_item'
);

DROP TABLE IF EXISTS bronze.raw_streaming_session CASCADE;
CREATE TABLE bronze.raw_streaming_session (
    stream_id             BIGINT PRIMARY KEY,
    customer_id           BIGINT NOT NULL,
    content_id            BIGINT NOT NULL,
    device_id             BIGINT NOT NULL,
    start_time            TIMESTAMP,
    end_time              TIMESTAMP,
    watch_duration        INT,
    completion_percentage DECIMAL(5,2),
    created_at            TIMESTAMP NOT NULL,
    updated_at            TIMESTAMP NOT NULL,
    batch_id              VARCHAR(150),
    extracted_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system         VARCHAR(100) DEFAULT 'abc_hub_operational.streaming_session'
);

DROP TABLE IF EXISTS bronze.raw_rental CASCADE;
CREATE TABLE bronze.raw_rental (
    rental_id       BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    inventory_id    BIGINT NOT NULL,
    rental_date     DATE,
    due_date        DATE,
    return_date     DATE,
    rental_fee      DECIMAL(10,2),
    late_fee        DECIMAL(10,2),
    status          VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.rental'
);

DROP TABLE IF EXISTS bronze.raw_payment CASCADE;
CREATE TABLE bronze.raw_payment (
    payment_id        BIGINT PRIMARY KEY,
    customer_id       BIGINT NOT NULL,
    rental_id         BIGINT,
    subscription_id   BIGINT,
    payment_method_id BIGINT,
    amount            DECIMAL(10,2),
    payment_date      TIMESTAMP,
    payment_type      VARCHAR(20),
    status            VARCHAR(20),
    created_at        TIMESTAMP NOT NULL,
    updated_at        TIMESTAMP NOT NULL,
    batch_id          VARCHAR(150),
    extracted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system     VARCHAR(100) DEFAULT 'abc_hub_operational.payment'
);

DROP TABLE IF EXISTS bronze.raw_support_ticket CASCADE;
CREATE TABLE bronze.raw_support_ticket (
    ticket_id       BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    category_id     BIGINT NOT NULL,
    opened_date     TIMESTAMP,
    closed_date     TIMESTAMP,
    priority        VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.support_ticket'
);

DROP TABLE IF EXISTS bronze.raw_review CASCADE;
CREATE TABLE bronze.raw_review (
    review_id       BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    content_id      BIGINT NOT NULL,
    rating          INT,
    review_text     VARCHAR(500),
    review_date     TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.review'
);

DROP TABLE IF EXISTS bronze.raw_wishlist CASCADE;
CREATE TABLE bronze.raw_wishlist (
    wishlist_id     BIGINT PRIMARY KEY,
    customer_id     BIGINT NOT NULL,
    content_id      BIGINT NOT NULL,
    added_date      TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    batch_id        VARCHAR(150),
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(100) DEFAULT 'abc_hub_operational.wishlist'
);

CREATE INDEX IF NOT EXISTS idx_bronze_customer_updated  ON bronze.raw_customer(updated_at);
CREATE INDEX IF NOT EXISTS idx_bronze_stream_updated     ON bronze.raw_streaming_session(updated_at);
CREATE INDEX IF NOT EXISTS idx_bronze_rental_updated     ON bronze.raw_rental(updated_at);
CREATE INDEX IF NOT EXISTS idx_bronze_payment_updated    ON bronze.raw_payment(updated_at);
