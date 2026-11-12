-- gold.dim_advertiser — INCREMENTAL (ongoing) SCD2 maintenance (Q5).
--
-- Q4's 05_load_dim_advertiser.sql is a BULK-HISTORICAL bootstrap: it
-- truncates and rebuilds the whole table from the complete exploded
-- history every time, which is only safe for the very first load. Doing
-- that on Day 2 would REGENERATE every surrogate key via
-- GENERATED ALWAYS AS IDENTITY, silently invalidating every fact row's
-- advertiser_key — a serious referential-integrity bug, not just wasted
-- work. This script is the correct ongoing pattern instead: detect history
-- entries not yet represented in Gold, close out the affected CURRENT row
-- with an UPDATE (never delete/reinsert — the surrogate key must not
-- move), and INSERT only the new version (a new surrogate key, used only
-- by conversions from here forward; every existing fact row keeps pointing
-- at the unchanged old key).
--
-- CORRECTNESS NOTE (found by testing against a simulated AM change, not by
-- inspection): the first version of this script detected "new" entries by
-- comparing h.effective_from against dim_advertiser.effective_from — but
-- Q4's bootstrap SHIFTS every non-first version's stored effective_from
-- forward by one day (the transition-day tie-break), so it never equals
-- the raw source value, and EVERY multi-version advertiser's existing,
-- already-correct rows were wrongly matched as "new" and closed out (14 of
-- them, in testing — caught and repaired before this was trusted). Fixed
-- by matching on effective_to instead, which Q4's bootstrap never shifts
-- (only effective_from of the FOLLOWING version is shifted) — so it's a
-- stable, unambiguous key for "has this exact history entry already been
-- applied to Gold."
DROP TABLE IF EXISTS pg_temp.new_am_entries;
CREATE TEMP TABLE new_am_entries AS
SELECT h.advertiser_id, h.am_id, h.effective_from, h.effective_to_raw,
       COALESCE(h.effective_to_raw, DATE '9999-12-31') AS effective_to_normalised
FROM silver.advertiser_am_history h
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_advertiser da
    WHERE da.advertiser_id = h.advertiser_id
      AND da.effective_to = COALESCE(h.effective_to_raw, DATE '9999-12-31')
      AND da.advertiser_key <> -1
);

-- Step 1: close out the currently-current version for every advertiser
-- that has a new entry — an UPDATE, so advertiser_key is untouched.
UPDATE gold.dim_advertiser da
SET effective_to = ne.effective_from,
    is_current = FALSE
FROM new_am_entries ne
WHERE da.advertiser_id = ne.advertiser_id
  AND da.is_current
  AND da.advertiser_key <> -1;

-- Step 2: insert the new version(s). effective_from is pushed one day past
-- the row just closed (giving the boundary day to the OLD version, per the
-- Q4 §3 tie-break, verified against real embedded-AM evidence there).
INSERT INTO gold.dim_advertiser
    (advertiser_id, advertiser_name, vertical, status, billing_country,
     account_manager_key, effective_from, effective_to, is_current, version_number)
SELECT
    ne.advertiser_id,
    a.advertiser_name, a.vertical, a.status, a.billing_country,
    COALESCE(am.am_key, -1),
    (ne.effective_from + INTERVAL '1 day')::DATE,
    ne.effective_to_normalised,
    (ne.effective_to_raw IS NULL),
    (SELECT COALESCE(MAX(version_number), 0) + 1 FROM gold.dim_advertiser WHERE advertiser_id = ne.advertiser_id)
FROM new_am_entries ne
JOIN silver.advertisers a ON a.advertiser_id = ne.advertiser_id
LEFT JOIN gold.dim_account_manager am ON am.am_id = ne.am_id;

DROP TABLE IF EXISTS pg_temp.new_am_entries;
