-- ============================================================
-- SCENARIO C: Inventory Utilisation
-- Grain: One record per physical inventory item per day
-- ============================================================

-- Fact Table: Inventory Utilisation
CREATE TABLE fact_inventory_utilisation (
    inventory_key BIGINT NOT NULL REFERENCES dim_inventory_item(inventory_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    warehouse_key BIGINT NOT NULL REFERENCES dim_warehouse(warehouse_key),
    rental_count INT DEFAULT 0,
    return_count INT DEFAULT 0,
    is_available INT DEFAULT 1,  -- 1 = available, 0 = rented out
    is_rented_out INT DEFAULT 0, -- 1 = currently rented, 0 = in warehouse
    days_available INT DEFAULT 0,
    cumulative_days_rented INT DEFAULT 0,
    cumulative_total_days INT DEFAULT 0,
    utilisation_percentage DECIMAL(5,2) DEFAULT 0.00,
    PRIMARY KEY (inventory_key, date_key)
);

-- Indexes for common query patterns
CREATE INDEX idx_fact_iu_date ON fact_inventory_utilisation(date_key);
CREATE INDEX idx_fact_iu_inventory ON fact_inventory_utilisation(inventory_key);
CREATE INDEX idx_fact_iu_warehouse ON fact_inventory_utilisation(warehouse_key);

-- ============================================================
-- BUSINESS MEASURE JUSTIFICATION:
-- ============================================================
-- rental_count: Additive - number of times this item was rented on this day (usually 0 or 1)
-- return_count: Additive - number of times this item was returned on this day
-- is_available: Semi-additive (snapshot) - whether the item was available for rent
-- is_rented_out: Semi-additive (snapshot) - whether the item was currently with a customer
-- days_available: Additive - count of days item has been available (running total context)
-- cumulative_days_rented: Running total of days this item has been rented since purchase
-- cumulative_total_days: Running total of days since item was added to inventory
-- utilisation_percentage: Derived - (cumulative_days_rented / cumulative_total_days) * 100
--
-- These measures enable inventory management decisions:
-- - Which items are underutilised (low utilisation %)
-- - Which warehouses have idle stock
-- - Rental velocity and return patterns
-- - Capacity planning and procurement decisions
