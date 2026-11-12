-- ============================================================
-- SCENARIO A: Customer Daily Activity
-- Grain: One record per customer per day
-- ============================================================

-- Fact Table: Customer Daily Activity
CREATE TABLE fact_customer_daily_activity (
    customer_key BIGINT NOT NULL REFERENCES dim_customer(customer_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
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

-- Indexes for common query patterns
CREATE INDEX idx_fact_cda_date ON fact_customer_daily_activity(date_key);
CREATE INDEX idx_fact_cda_customer ON fact_customer_daily_activity(customer_key);

-- ============================================================
-- BUSINESS MEASURE JUSTIFICATION:
-- ============================================================
-- streaming_session_count: Additive measure - count of streaming events per day
-- total_streaming_duration_minutes: Additive measure - total watch time in minutes
-- physical_rental_count: Additive measure - number of new rentals initiated
-- returned_item_count: Additive measure - number of items returned
-- total_amount_spent: Additive measure - total monetary transactions (rentals + subscriptions)
-- support_ticket_count: Additive measure - number of support interactions
-- wishlist_addition_count: Additive measure - content engagement metric
-- review_count: Additive measure - content feedback metric
-- avg_completion_percentage: Semi-additive - average stream completion (cannot be summed across customers)
--
-- All measures are numeric, continuously valued, and result from business events.
-- They belong in the fact table because they are measurements at the grain of
-- customer-day and can be aggregated across dimensions.
