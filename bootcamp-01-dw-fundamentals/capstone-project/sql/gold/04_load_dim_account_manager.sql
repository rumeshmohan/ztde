-- gold.dim_account_manager — Type 1, idempotent upsert from silver.
-- Q6 fix: IS DISTINCT FROM guard (skip a true no-op) + RETURNING (xmax = 0)
-- so inserted/updated are honestly measurable (see 06's fuller comment).
WITH upserted AS (
    INSERT INTO gold.dim_account_manager (am_id, am_name, team, region, hire_date)
    SELECT am_id, am_name, team, region, hire_date FROM silver.account_managers
    ON CONFLICT (am_id) DO UPDATE
        SET am_name = EXCLUDED.am_name, team = EXCLUDED.team,
            region = EXCLUDED.region, hire_date = EXCLUDED.hire_date
    WHERE gold.dim_account_manager.am_name IS DISTINCT FROM EXCLUDED.am_name
       OR gold.dim_account_manager.team IS DISTINCT FROM EXCLUDED.team
       OR gold.dim_account_manager.region IS DISTINCT FROM EXCLUDED.region
       OR gold.dim_account_manager.hire_date IS DISTINCT FROM EXCLUDED.hire_date
    RETURNING (xmax = 0) AS is_insert
)
SELECT COUNT(*) FILTER (WHERE is_insert) AS inserted, COUNT(*) FILTER (WHERE NOT is_insert) AS updated
FROM upserted;
