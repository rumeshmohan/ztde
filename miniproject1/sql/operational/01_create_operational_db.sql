-- ============================================================
-- ABC Hub Operational Database
-- Based on ERD provided in project documentation
-- Run this in: abc_hub_operational
-- ============================================================

-- ============================================================
-- LOOKUP / REFERENCE TABLES
-- ============================================================

CREATE TABLE country (
    country_id BIGSERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL
);

CREATE TABLE city (
    city_id BIGSERIAL PRIMARY KEY,
    country_id BIGINT NOT NULL REFERENCES country(country_id),
    city_name VARCHAR(100) NOT NULL
);

CREATE TABLE content_type (
    content_type_id BIGSERIAL PRIMARY KEY,
    content_type VARCHAR(50) NOT NULL
);

CREATE TABLE genre (
    genre_id BIGSERIAL PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL
);

CREATE TABLE artist (
    artist_id BIGSERIAL PRIMARY KEY,
    artist_name VARCHAR(200) NOT NULL,
    country VARCHAR(100)
);

CREATE TABLE device (
    device_id BIGSERIAL PRIMARY KEY,
    device_name VARCHAR(100) NOT NULL
);

CREATE TABLE subscription_plan (
    plan_id BIGSERIAL PRIMARY KEY,
    plan_name VARCHAR(100) NOT NULL,
    monthly_fee DECIMAL(10,2) NOT NULL,
    video_quality VARCHAR(50),
    max_devices INT
);

CREATE TABLE payment_method (
    payment_method_id BIGSERIAL PRIMARY KEY,
    method_name VARCHAR(100) NOT NULL
);

CREATE TABLE courier (
    courier_id BIGSERIAL PRIMARY KEY,
    courier_name VARCHAR(200) NOT NULL
);

CREATE TABLE support_category (
    category_id BIGSERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);

-- ============================================================
-- CORE BUSINESS TABLES
-- ============================================================

CREATE TABLE customer (
    customer_id BIGSERIAL PRIMARY KEY,
    customer_no VARCHAR(50) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    phone VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(20),
    registration_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL
);

CREATE TABLE customer_address (
    address_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    city_id BIGINT NOT NULL REFERENCES city(city_id),
    address_line VARCHAR(500),
    postal_code VARCHAR(20),
    address_type VARCHAR(50)
);

CREATE TABLE content (
    content_id BIGSERIAL PRIMARY KEY,
    content_type_id BIGINT NOT NULL REFERENCES content_type(content_type_id),
    title VARCHAR(500) NOT NULL,
    release_date DATE,
    duration_minutes INT,
    language VARCHAR(50),
    age_rating VARCHAR(20)
);

CREATE TABLE content_genre (
    content_id BIGINT NOT NULL REFERENCES content(content_id),
    genre_id BIGINT NOT NULL REFERENCES genre(genre_id),
    PRIMARY KEY (content_id, genre_id)
);

CREATE TABLE content_artist (
    content_id BIGINT NOT NULL REFERENCES content(content_id),
    artist_id BIGINT NOT NULL REFERENCES artist(artist_id),
    role VARCHAR(100),
    PRIMARY KEY (content_id, artist_id)
);

-- ============================================================
-- SUBSCRIPTION & STREAMING
-- ============================================================

CREATE TABLE customer_subscription (
    subscription_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    plan_id BIGINT NOT NULL REFERENCES subscription_plan(plan_id),
    start_date DATE NOT NULL,
    end_date DATE,
    status VARCHAR(20) NOT NULL,
    auto_renew BOOLEAN DEFAULT TRUE
);

CREATE TABLE streaming_session (
    stream_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    content_id BIGINT NOT NULL REFERENCES content(content_id),
    device_id BIGINT NOT NULL REFERENCES device(device_id),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    watch_duration INT,
    completion_percentage DECIMAL(5,2)
);

-- ============================================================
-- INVENTORY & RENTALS
-- ============================================================

CREATE TABLE warehouse (
    warehouse_id BIGSERIAL PRIMARY KEY,
    city_id BIGINT NOT NULL REFERENCES city(city_id),
    warehouse_name VARCHAR(200) NOT NULL
);

CREATE TABLE inventory_item (
    inventory_id BIGSERIAL PRIMARY KEY,
    content_id BIGINT NOT NULL REFERENCES content(content_id),
    warehouse_id BIGINT NOT NULL REFERENCES warehouse(warehouse_id),
    barcode VARCHAR(100),
    purchase_date DATE,
    item_condition VARCHAR(50),
    status VARCHAR(50) NOT NULL
);

CREATE TABLE rental (
    rental_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    inventory_id BIGINT NOT NULL REFERENCES inventory_item(inventory_id),
    rental_date DATE NOT NULL,
    due_date DATE NOT NULL,
    return_date DATE,
    rental_fee DECIMAL(10,2) NOT NULL,
    late_fee DECIMAL(10,2) DEFAULT 0,
    status VARCHAR(50) NOT NULL
);

CREATE TABLE delivery (
    delivery_id BIGSERIAL PRIMARY KEY,
    rental_id BIGINT NOT NULL REFERENCES rental(rental_id),
    courier_id BIGINT NOT NULL REFERENCES courier(courier_id),
    dispatch_date TIMESTAMP,
    delivered_date TIMESTAMP,
    returned_date TIMESTAMP,
    delivery_status VARCHAR(50) NOT NULL
);

-- ============================================================
-- PAYMENTS
-- ============================================================

CREATE TABLE payment (
    payment_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    rental_id BIGINT REFERENCES rental(rental_id),
    subscription_id BIGINT REFERENCES customer_subscription(subscription_id),
    payment_method_id BIGINT NOT NULL REFERENCES payment_method(payment_method_id),
    amount DECIMAL(10,2) NOT NULL,
    payment_date TIMESTAMP NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL
);

-- ============================================================
-- CUSTOMER ENGAGEMENT
-- ============================================================

CREATE TABLE review (
    review_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    content_id BIGINT NOT NULL REFERENCES content(content_id),
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_text TEXT,
    review_date TIMESTAMP NOT NULL
);

CREATE TABLE wishlist (
    wishlist_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    content_id BIGINT NOT NULL REFERENCES content(content_id),
    added_date TIMESTAMP NOT NULL
);

CREATE TABLE recommendation (
    recommendation_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    content_id BIGINT NOT NULL REFERENCES content(content_id),
    recommendation_date TIMESTAMP NOT NULL,
    algorithm_version VARCHAR(50),
    clicked BOOLEAN DEFAULT FALSE
);

CREATE TABLE support_ticket (
    ticket_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    category_id BIGINT NOT NULL REFERENCES support_category(category_id),
    opened_date TIMESTAMP NOT NULL,
    closed_date TIMESTAMP,
    priority VARCHAR(20) NOT NULL,
    status VARCHAR(50) NOT NULL
);

-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================

CREATE INDEX idx_customer_email ON customer(email);
CREATE INDEX idx_customer_status ON customer(status);
CREATE INDEX idx_streaming_customer ON streaming_session(customer_id);
CREATE INDEX idx_streaming_content ON streaming_session(content_id);
CREATE INDEX idx_streaming_start ON streaming_session(start_time);
CREATE INDEX idx_rental_customer ON rental(customer_id);
CREATE INDEX idx_rental_date ON rental(rental_date);
CREATE INDEX idx_payment_customer ON payment(customer_id);
CREATE INDEX idx_payment_date ON payment(payment_date);
CREATE INDEX idx_inventory_content ON inventory_item(content_id);
CREATE INDEX idx_inventory_warehouse ON inventory_item(warehouse_id);
