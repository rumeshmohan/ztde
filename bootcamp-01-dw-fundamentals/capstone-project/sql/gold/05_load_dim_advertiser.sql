-- gold.dim_advertiser — the Type-2 hybrid SCD load (Q2 B.3/B.4).
-- Grain of THIS load = one row per (advertiser_id, effective_from) entry in
-- silver.advertiser_am_history — i.e. this is a BULK-HISTORICAL load from a
-- source that already hands us the full version history at once (MongoDB's
-- account_manager_history[]), not the "diff current vs incoming, expire +
-- insert" pattern Week 09/10 use for a source that only ever shows current
-- state. Both are valid SCD2 loading patterns; this dataset's shape calls
-- for this one. Q5 will need to extend this for detecting a NEW history
-- entry appended since the last run (the ongoing-incremental case), which
-- IS the Week 09/10-style pattern — deferred there deliberately, same
-- reasoning as Q4's relational-extraction scope note.
--
-- Transition-day tie-break (Q1 §4's predicted ambiguity, now hit for real:
-- MongoDB's from/to are DATE-only, and verified empirically that a version's
-- stated 'to' literally EQUALS the next version's stated 'from' — e.g.
-- ADV0008 v1.to = 2025-09-16, v2.from = 2025-09-16 — so a naive
-- [effective_from, effective_to] taken directly from the source makes two
-- consecutive versions share that boundary day, and a conversion ON it
-- joined BOTH rows, fanning out a duplicate key error on the Gold fact
-- load). Fixed here, not papered over at the fact-join, and in the
-- direction the EVIDENCE points, not the first direction tried: Q2 A.4's
-- reconciliation check found every one of the 23 same-day mismatches had
-- conversions_raw embedding the OLD AM for that day, not the new one — so
-- the boundary day belongs to the OLD version. Each version's stated
-- effective_from/effective_to are kept AS-IS (inclusive, matching the
-- source); instead, every NON-FIRST version's effective_from is pushed one
-- day past the PREVIOUS version's stated 'to', so the old version keeps the
-- shared day and the new version starts the day after. (First attempt at
-- this fix shifted the wrong side — caught by re-testing against CV0010738
-- below before moving on, not assumed correct.)
WITH history_with_prev AS (
    SELECT
        h.advertiser_id, h.am_id, h.am_name, h.effective_from, h.effective_to_raw,
        LAG(h.effective_to_raw) OVER (PARTITION BY h.advertiser_id ORDER BY h.effective_from) AS prev_effective_to
    FROM silver.advertiser_am_history h
)
INSERT INTO gold.dim_advertiser
    (advertiser_id, advertiser_name, vertical, status, billing_country,
     account_manager_key, effective_from, effective_to, is_current, version_number)
SELECT
    h.advertiser_id,
    a.advertiser_name, a.vertical, a.status, a.billing_country,
    COALESCE(am.am_key, -1) AS account_manager_key,
    CASE
        WHEN h.prev_effective_to IS NOT NULL THEN (h.prev_effective_to + INTERVAL '1 day')::DATE
        ELSE h.effective_from
    END AS effective_from,
    COALESCE(h.effective_to_raw, DATE '9999-12-31') AS effective_to,
    (h.effective_to_raw IS NULL) AS is_current,
    ROW_NUMBER() OVER (PARTITION BY h.advertiser_id ORDER BY h.effective_from) AS version_number
FROM history_with_prev h
JOIN silver.advertisers a ON a.advertiser_id = h.advertiser_id
LEFT JOIN gold.dim_account_manager am ON am.am_id = h.am_id;
