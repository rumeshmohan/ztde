-- gold.dim_date — static spine, generated once (Week 10 pattern), covering
-- well beyond the dataset's documented core months (2025-02 to 2026-05) to
-- be safe against the "thin edge months" the data guide warns about.
INSERT INTO gold.dim_date (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d::DATE,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TO_CHAR(d, 'FMMonth'),
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(ISODOW FROM d)::SMALLINT,
    TO_CHAR(d, 'FMDay'),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2024-01-01'::DATE, '2027-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;
