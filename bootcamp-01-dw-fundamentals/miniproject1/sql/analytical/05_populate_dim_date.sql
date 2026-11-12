-- ============================================================
-- Populate Dim Date table (2020-01-01 to 2030-12-31)
-- ============================================================

INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month,
                      day_of_year, week_of_year, month_number, month_name,
                      quarter, year, is_weekend, is_holiday)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d AS full_date,
    EXTRACT(DOW FROM d)::INT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(DAY FROM d)::INT AS day_of_month,
    EXTRACT(DOY FROM d)::INT AS day_of_year,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    EXTRACT(MONTH FROM d)::INT AS month_number,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(YEAR FROM d)::INT AS year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS d;
