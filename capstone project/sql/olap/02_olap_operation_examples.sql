-- ============================================================================
-- PulseTrack — OLAP operations demonstrated against the cubes (Q8)
-- Roll-up/drill-down/slice/dice are trivial once the cube exists (filter on
-- the GROUPING flags + a WHERE — no new query pattern needed). The
-- remaining operations (Top-N, ranking, % contribution, MoM/QoQ/YoY, pivot)
-- are genuinely different query shapes; each is demonstrated here with a
-- real result, not just asserted as "supported."
-- ============================================================================

-- ---------------------------------------------------------------- 1. Roll-up / drill-down
-- Roll-up: total revenue by YEAR only (coarsest business level, finest? no
-- — coarsest date level with business fully rolled up).
SELECT '1a_rollup_year_total_revenue' AS op, year, revenue
FROM olap.mv_advertiser_cube
WHERE g_year=0 AND g_quarter=1 AND g_month=1 AND g_day=1
  AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
ORDER BY year;

-- Drill-down: the SAME advertiser, from year -> quarter -> month for one
-- specific advertiser (ADV0027, the top advertiser found earlier).
SELECT '1b_drilldown_adv0027_by_month' AS op, year, quarter, month, revenue
FROM olap.mv_advertiser_cube
WHERE advertiser_id = 'ADV0027' AND g_advertiser=0 AND g_offer=1 AND g_campaign=1 AND g_creative=1
  AND g_year=0 AND g_quarter=0 AND g_month=0 AND g_day=1
ORDER BY year, quarter, month
LIMIT 6;

-- ---------------------------------------------------------------- 2. Slice / dice
-- Slice: one fixed dimension value (year=2025) across all advertisers.
SELECT '2a_slice_year_2025' AS op, advertiser_id, advertiser_name, revenue
FROM olap.mv_advertiser_cube
WHERE year = 2025 AND g_year=0 AND g_quarter=1 AND g_month=1 AND g_day=1
  AND g_advertiser=0 AND g_offer=1 AND g_campaign=1 AND g_creative=1
ORDER BY revenue DESC LIMIT 5;

-- Dice: a sub-cube — two advertisers, two quarters, revenue only.
SELECT '2b_dice_two_advertisers_two_quarters' AS op, advertiser_id, year, quarter, revenue
FROM olap.mv_advertiser_cube
WHERE advertiser_id IN ('ADV0027','ADV0039') AND quarter IN (1,2)
  AND g_advertiser=0 AND g_offer=1 AND g_campaign=1 AND g_creative=1
  AND g_year=0 AND g_quarter=0 AND g_month=1 AND g_day=1
ORDER BY advertiser_id, year, quarter;

-- ---------------------------------------------------------------- 3. Pivot (advertiser x quarter, revenue)
-- NOTE: a PostgreSQL SELECT-list alias (q1, q2, ...) can be used BARE in
-- ORDER BY, but not inside a larger expression like (q1+q2+q3+q4) — that
-- form must resolve against the FROM/GROUP BY, not the alias list
-- (confirmed by testing, not assumed). Wrapped in a subquery so the pivoted
-- columns become real columns the outer ORDER BY can combine freely.
SELECT '3_pivot_advertiser_by_quarter' AS op, * FROM (
    SELECT advertiser_id,
        SUM(revenue) FILTER (WHERE quarter=1) AS q1,
        SUM(revenue) FILTER (WHERE quarter=2) AS q2,
        SUM(revenue) FILTER (WHERE quarter=3) AS q3,
        SUM(revenue) FILTER (WHERE quarter=4) AS q4
    FROM olap.mv_advertiser_cube
    WHERE year = 2025 AND g_year=0 AND g_quarter=0 AND g_month=1 AND g_day=1
      AND g_advertiser=0 AND g_offer=1 AND g_campaign=1 AND g_creative=1
    GROUP BY advertiser_id
) pivoted
ORDER BY (COALESCE(q1,0)+COALESCE(q2,0)+COALESCE(q3,0)+COALESCE(q4,0)) DESC
LIMIT 5;

-- ---------------------------------------------------------------- 4. Top-N and ranking
SELECT '4_top5_advertisers_by_revenue_alltime' AS op, advertiser_id, advertiser_name, revenue,
    RANK() OVER (ORDER BY revenue DESC) AS rank
FROM olap.mv_advertiser_cube
WHERE g_year=1 AND g_quarter=1 AND g_month=1 AND g_day=1
  AND g_advertiser=0 AND g_offer=1 AND g_campaign=1 AND g_creative=1
ORDER BY revenue DESC LIMIT 5;

-- ---------------------------------------------------------------- 5. Percentage contribution
-- Each campaign's share of ITS OWN advertiser's total revenue (Q1 §3.6:
-- ratio to the parent level total — a window function partitioned by the
-- parent, not a per-row average).
SELECT '5_campaign_pct_of_advertiser_revenue' AS op,
    advertiser_id, campaign_id, campaign_name, revenue,
    (revenue / NULLIF(SUM(revenue) OVER (PARTITION BY advertiser_id), 0) * 100)::NUMERIC(5,2) AS pct_of_advertiser
FROM olap.mv_advertiser_cube
-- g_offer is NOT set to 1 here: ROLLUP(advertiser_id, offer_name,
-- campaign_id, creative_id) nests strictly left-to-right, so "campaign
-- present" (g_campaign=0) already forces "offer present" (g_offer=0) too —
-- there is no valid grouping set with campaign present but offer rolled up
-- (confirmed by testing: adding g_offer=1 here returned 0 rows, not a
-- smaller-but-real result, which is what caught this).
WHERE advertiser_id = 'ADV0027' AND g_campaign=0 AND g_creative=1
  AND g_year=1 AND g_quarter=1 AND g_month=1 AND g_day=1
ORDER BY revenue DESC LIMIT 5;

-- ---------------------------------------------------------------- 6. Month-over-month, quarter-over-quarter, year-over-year
-- MoM: total revenue by (year,month), with the prior month via LAG() and
-- the percentage change — window functions over the cube's own pre-aggregated
-- rows, not a re-aggregation of the base fact.
SELECT '6a_mom_total_revenue' AS op, year, month, revenue,
    LAG(revenue) OVER (ORDER BY year, month) AS prior_month_revenue,
    ((revenue - LAG(revenue) OVER (ORDER BY year, month))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0) * 100)::NUMERIC(6,2) AS mom_pct_change
FROM olap.mv_advertiser_cube
-- g_quarter=0, not 1: ROLLUP(year,quarter,month,day) nests strictly, so
-- "month present" (g_month=0) already forces "quarter present" (g_quarter=0)
-- too — same class of mistake as query 5, caught the same way (0 rows,
-- not a smaller-but-real result).
WHERE g_year=0 AND g_quarter=0 AND g_month=0 AND g_day=1
  AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
ORDER BY year, month;

-- QoQ
SELECT '6b_qoq_total_revenue' AS op, year, quarter, revenue,
    LAG(revenue) OVER (ORDER BY year, quarter) AS prior_quarter_revenue,
    ((revenue - LAG(revenue) OVER (ORDER BY year, quarter))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, quarter), 0) * 100)::NUMERIC(6,2) AS qoq_pct_change
FROM olap.mv_advertiser_cube
WHERE g_year=0 AND g_quarter=0 AND g_month=1 AND g_day=1
  AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
ORDER BY year, quarter;

-- YoY: same quarter, year over year (Q1 vs Q1, Q2 vs Q2, ...) — partition
-- by quarter-of-year, order by year, so LAG() correctly compares the same
-- calendar quarter a year apart, not merely the previous row.
SELECT '6c_yoy_same_quarter' AS op, year, quarter, revenue,
    LAG(revenue) OVER (PARTITION BY quarter ORDER BY year) AS year_ago_same_quarter,
    ((revenue - LAG(revenue) OVER (PARTITION BY quarter ORDER BY year))
        / NULLIF(LAG(revenue) OVER (PARTITION BY quarter ORDER BY year), 0) * 100)::NUMERIC(6,2) AS yoy_pct_change
FROM olap.mv_advertiser_cube
WHERE g_year=0 AND g_quarter=0 AND g_month=1 AND g_day=1
  AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
ORDER BY quarter, year;
