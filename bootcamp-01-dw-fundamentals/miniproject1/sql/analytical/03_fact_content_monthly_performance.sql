-- ============================================================
-- SCENARIO B: Content Monthly Performance
-- Grain: One record per content item per month
-- ============================================================

-- Fact Table: Content Monthly Performance
CREATE TABLE fact_content_monthly_performance (
    content_key BIGINT NOT NULL REFERENCES dim_content(content_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),  -- first day of month
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

-- Indexes for common query patterns
CREATE INDEX idx_fact_cmp_date ON fact_content_monthly_performance(date_key);
CREATE INDEX idx_fact_cmp_content ON fact_content_monthly_performance(content_key);

-- ============================================================
-- BUSINESS MEASURE JUSTIFICATION:
-- ============================================================
-- total_stream_count: Additive - total number of streaming sessions for this content
-- total_rental_count: Additive - number of physical copies rented
-- total_revenue: Additive - revenue generated from rentals and associated fees
-- average_customer_rating: Semi-additive - cannot be summed; weighted avg needed across time
-- total_ratings_count: Additive - number of ratings received (supports avg calculation)
-- wishlist_addition_count: Additive - demand indicator for content
-- unique_streamers: Semi-additive - distinct customer count (cannot sum across months)
-- unique_renters: Semi-additive - distinct customers who rented
-- total_streaming_minutes: Additive - total engagement time
-- avg_completion_percentage: Semi-additive - average viewing completion
--
-- These measures quantify content performance from multiple angles:
-- engagement (streams, duration), commercial (revenue, rentals),
-- satisfaction (ratings), and demand (wishlists).
