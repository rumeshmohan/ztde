-- ============================================================================
-- PulseTrack — Business Analytics Using the Final Platform (Q9)
-- Every query below reads ONLY from mart.*/olap.* (the Data Mart and OLAP
-- cubes) — never bronze/silver, and never pulsetrack_raw. These are
-- verbatim Q1 §5's ten analytical questions, now actually answered.
-- ============================================================================

-- ---------------------------------------------------------------- Q1. Highest-revenue advertiser each quarter
SELECT 'Q1' AS question, year, quarter, advertiser_id, advertiser_name, revenue, rnk
FROM (
    SELECT year, quarter, advertiser_id, advertiser_name, revenue,
           RANK() OVER (PARTITION BY year, quarter ORDER BY revenue DESC) AS rnk
    FROM olap.mv_advertiser_cube
    WHERE g_year=0 AND g_quarter=0 AND g_month=1 AND g_day=1
      AND g_advertiser=0 AND g_offer=1 AND g_campaign=1 AND g_creative=1
      AND advertiser_id <> 'UNKNOWN'
) t
WHERE rnk = 1
ORDER BY year, quarter;

-- ---------------------------------------------------------------- Q2. Highest CTR, lowest conversion rate campaigns
SELECT 'Q2' AS question, campaign_id, campaign_name, advertiser_name, ctr, conversion_rate
FROM (
    SELECT campaign_id, campaign_name, advertiser_name, ctr, conversion_rate,
           NTILE(4) OVER (ORDER BY ctr DESC) AS ctr_quartile,
           NTILE(4) OVER (ORDER BY conversion_rate ASC) AS cvr_quartile
    FROM olap.mv_advertiser_cube
    WHERE g_year=1 AND g_quarter=1 AND g_month=1 AND g_day=1
      AND g_campaign=0 AND g_creative=1
      AND clicks >= 30   -- exclude near-zero-volume campaigns where ratios are noise, not signal
) t
WHERE ctr_quartile = 1 AND cvr_quartile = 1
ORDER BY ctr DESC
LIMIT 10;

-- ---------------------------------------------------------------- Q3. Highest attributed-revenue partners
SELECT 'Q3' AS question, partner_id, partner_name, attributed_revenue
FROM olap.mv_partner_cube
WHERE g_year=1 AND g_quarter=1 AND g_month=1 AND g_day=1
  AND g_partner=0 AND partner_id <> 'UNKNOWN'
ORDER BY attributed_revenue DESC
LIMIT 10;

-- ---------------------------------------------------------------- Q4. Revenue vs. previous month
-- NOT simply "the latest two months in the cube" — checked first, not
-- assumed: the naive latest-month figure came back as a -41.06% MoM
-- "collapse" (2026-06: $341,740 vs 2026-05: $579,852). Verified against
-- mart.fact_performance_daily directly: impressions/clicks continue at a
-- steady daily rate right through the dataset's last date (2026-06-29), but
-- revenue/conversions drop to EXACTLY 0.00 for every day from 2026-06-20
-- onward. This is the same conversion-attribution lag already quantified in
-- Q1 §4 (median 28 days, p90 45.7, max 60.1) hitting the trailing edge of
-- the dataset: this fact's date_key is the CLICK date, and conversions get
-- attributed back to it, so any click within ~28-60 days of the dataset's
-- extraction cutoff hasn't finished accumulating its conversions yet. It is
-- a data-maturity artifact, not a real business decline — reporting it
-- as-is would be wrong. Fixed by excluding any month not yet clear of the
-- documented median lag window before computing MoM (the real answer is
-- April-2026 -> May-2026: +3.98%, consistent with neighbouring months).
WITH bounds AS (
    SELECT MAX(full_date) AS max_d FROM gold.dim_date d
    JOIN mart.fact_performance_daily p ON p.date_key = d.date_key
),
mature_months AS (
    SELECT year, month, revenue
    FROM olap.mv_advertiser_cube, bounds
    WHERE g_year=0 AND g_quarter=0 AND g_month=0 AND g_day=1
      AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
      -- month-end must be at least 28 days (the documented median lag) before the dataset's max date
      AND (make_date(year, month, 1) + INTERVAL '1 month' - INTERVAL '1 day') <= (bounds.max_d - INTERVAL '28 days')
)
SELECT 'Q4' AS question, year, month, revenue, prior_month_revenue, mom_pct_change
FROM (
    SELECT year, month, revenue,
        LAG(revenue) OVER (ORDER BY year, month) AS prior_month_revenue,
        ((revenue - LAG(revenue) OVER (ORDER BY year, month))
            / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0) * 100)::NUMERIC(6,2) AS mom_pct_change
    FROM mature_months
) t
ORDER BY year DESC, month DESC
LIMIT 1;

-- context: the excluded, lag-distorted trailing months, shown explicitly so
-- the exclusion is a demonstrated decision, not a silently dropped result
SELECT 'Q4_excluded_immature_months_for_context' AS question, year, month, revenue::NUMERIC(12,2)
FROM olap.mv_advertiser_cube, (SELECT MAX(full_date) AS max_d FROM gold.dim_date d JOIN mart.fact_performance_daily p ON p.date_key = d.date_key) bounds
WHERE g_year=0 AND g_quarter=0 AND g_month=0 AND g_day=1
  AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
  AND (make_date(year, month, 1) + INTERVAL '1 month' - INTERVAL '1 day') > (bounds.max_d - INTERVAL '28 days')
ORDER BY year DESC, month DESC;

-- ---------------------------------------------------------------- Q5. Campaigns contributing >10% of total revenue
SELECT 'Q5' AS question, campaign_id, campaign_name, revenue,
    (revenue / (SELECT revenue FROM olap.mv_advertiser_cube
                WHERE g_year=1 AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
                  AND g_quarter=1 AND g_month=1 AND g_day=1) * 100)::NUMERIC(6,3) AS pct_of_total_revenue
FROM olap.mv_advertiser_cube
WHERE g_year=1 AND g_campaign=0 AND g_creative=1 AND g_quarter=1 AND g_month=1 AND g_day=1
  AND (revenue / (SELECT revenue FROM olap.mv_advertiser_cube
                  WHERE g_year=1 AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
                    AND g_quarter=1 AND g_month=1 AND g_day=1)) > 0.10
ORDER BY revenue DESC;

-- (also show the actual TOP campaign's share, whatever it is, so a "none exceed 10%"
-- result is a demonstrated fact, not a silently empty answer)
SELECT 'Q5_top_campaign_share_for_context' AS question, campaign_id, campaign_name, revenue,
    (revenue / (SELECT revenue FROM olap.mv_advertiser_cube
                WHERE g_year=1 AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
                  AND g_quarter=1 AND g_month=1 AND g_day=1) * 100)::NUMERIC(6,3) AS pct_of_total_revenue
FROM olap.mv_advertiser_cube
WHERE g_year=1 AND g_campaign=0 AND g_creative=1 AND g_quarter=1 AND g_month=1 AND g_day=1
ORDER BY revenue DESC LIMIT 3;

-- ---------------------------------------------------------------- Q6. Underperforming creatives (vs. their own campaign's average)
SELECT 'Q6' AS question, creative_id, creative_name, campaign_id, campaign_name,
    creative_cvr, campaign_cvr, (creative_cvr - campaign_cvr)::NUMERIC(6,4) AS gap
FROM (
    SELECT
        cr.creative_id, cr.creative_name, cr.campaign_id, cr.campaign_name,
        cr.conversion_rate AS creative_cvr,
        camp.conversion_rate AS campaign_cvr
    FROM olap.mv_advertiser_cube cr
    JOIN olap.mv_advertiser_cube camp
        ON camp.campaign_id = cr.campaign_id
       AND camp.g_campaign = 0 AND camp.g_creative = 1
       AND camp.g_year=1 AND camp.g_quarter=1 AND camp.g_month=1 AND camp.g_day=1
    WHERE cr.g_creative = 0
      AND cr.g_year=1 AND cr.g_quarter=1 AND cr.g_month=1 AND cr.g_day=1
      AND cr.clicks >= 20   -- exclude near-zero-volume creatives (ratio noise)
) t
WHERE creative_cvr < campaign_cvr
ORDER BY gap ASC, creative_id   -- stable tiebreak: several creatives share an identical gap (0% vs. the same campaign average) — confirmed all such ties fall safely inside the top 10, not at the boundary, but the display ORDER within them is otherwise non-deterministic across runs
LIMIT 10;

-- ---------------------------------------------------------------- Q7. Advertiser performance before/after an AM change
-- ADV0008's real, documented AM change (Q4 §3 / Q5 §4): AM008 "Emma
-- Hernandez" through 2025-09-16, AM011 "Wei Williams" from 2025-09-17.
-- Queried from mart.fact_performance_daily directly (the Data Mart, not
-- raw/bronze/silver — a legitimate layer of "the final platform" that this
-- specific cross-tab (advertiser x account_manager together) was never
-- built into a cube for, by design — see Q8 §2's cardinality reasoning).
--
-- NOTE (caught by testing, not assumed correct): the first version of this
-- query included revenue_per_click, which came back blank for BOTH rows.
-- Verified against raw sums: SUM(clicks)=0 for every account_manager_id
-- filter on this advertiser. This is by design, not a gap — mart §2
-- documents that account_manager_id is populated ONLY on conversion-sourced
-- rows; impression/click activity is recorded on separate rows with
-- account_manager_id IS NULL (AM isn't a dimension of a click/impression
-- event, only of the advertiser at conversion time). So "revenue per click,
-- filtered by AM" is asking for a cross-tab this grain cannot answer — the
-- right measure at this grain is revenue_per_conversion.
SELECT 'Q7' AS question,
    CASE WHEN account_manager_id = 'AM008' THEN 'before (Emma Hernandez)' ELSE 'after (Wei Williams)' END AS period,
    SUM(conversions) AS conversions, SUM(revenue)::NUMERIC(12,2) AS revenue,
    (SUM(revenue) / NULLIF(SUM(conversions), 0))::NUMERIC(8,2) AS revenue_per_conversion
FROM mart.fact_performance_daily
WHERE advertiser_id = 'ADV0008' AND account_manager_id IN ('AM008', 'AM011')
GROUP BY account_manager_id
ORDER BY period;

-- ---------------------------------------------------------------- Q8. % of total revenue attributable to each partner
SELECT 'Q8' AS question, partner_id, partner_name, attributed_revenue,
    (attributed_revenue / (SELECT revenue FROM olap.mv_advertiser_cube
                           WHERE g_year=1 AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1
                             AND g_quarter=1 AND g_month=1 AND g_day=1) * 100)::NUMERIC(6,3) AS pct_of_total_revenue
FROM olap.mv_partner_cube
WHERE g_year=1 AND g_quarter=1 AND g_month=1 AND g_day=1 AND g_partner=0
ORDER BY attributed_revenue DESC
LIMIT 10;

-- what fraction of TOTAL revenue is attributable to ANY partner at all (the
-- other side of the ~10% unattributable gap already known from Q1 §6/Q4 §4)
SELECT 'Q8_context_total_attributable_share' AS question,
    (SELECT SUM(attributed_revenue) FROM olap.mv_partner_cube WHERE g_partner=0 AND g_year=1 AND g_quarter=1 AND g_month=1 AND g_day=1) AS total_attributed_revenue,
    (SELECT revenue FROM olap.mv_advertiser_cube WHERE g_year=1 AND g_advertiser=1 AND g_offer=1 AND g_campaign=1 AND g_creative=1 AND g_quarter=1 AND g_month=1 AND g_day=1) AS total_revenue;

-- ---------------------------------------------------------------- Q9. Increasing impressions, declining conversions
-- Two-period trend per campaign: first half vs second half of the dataset's
-- date range (a real, simple trend comparison over the ~17-month window).
WITH bounds AS (
    SELECT MIN(full_date) AS min_d, MAX(full_date) AS max_d FROM gold.dim_date d
    JOIN mart.fact_performance_daily p ON p.date_key = d.date_key
),
midpoint AS (
    SELECT min_d + (max_d - min_d)/2 AS mid_d FROM bounds
),
by_period AS (
    SELECT p.campaign_id,
        SUM(p.impressions) FILTER (WHERE d.full_date < m.mid_d) AS impressions_h1,
        SUM(p.impressions) FILTER (WHERE d.full_date >= m.mid_d) AS impressions_h2,
        SUM(p.conversions) FILTER (WHERE d.full_date < m.mid_d) AS conversions_h1,
        SUM(p.conversions) FILTER (WHERE d.full_date >= m.mid_d) AS conversions_h2
    FROM mart.fact_performance_daily p
    JOIN gold.dim_date d ON d.date_key = p.date_key
    CROSS JOIN midpoint m
    GROUP BY p.campaign_id
)
SELECT 'Q9' AS question, c.campaign_id, c.campaign_name,
    b.impressions_h1, b.impressions_h2, b.conversions_h1, b.conversions_h2
FROM by_period b
JOIN mart.dim_campaign c ON c.campaign_id = b.campaign_id
WHERE b.impressions_h2 > b.impressions_h1 AND b.conversions_h2 < b.conversions_h1
  AND b.impressions_h1 > 0
ORDER BY (b.impressions_h2 - b.impressions_h1) DESC, c.campaign_id   -- stable tiebreak: same non-determinism class as Q6 — ties confirmed inside the top 10, not at the boundary
LIMIT 10;

-- ---------------------------------------------------------------- Q10. Revenue impact of late adjustments
SELECT 'Q10' AS question,
    SUM(revenue)::NUMERIC(12,2) AS total_revenue,
    SUM(adjusted_revenue)::NUMERIC(12,2) AS total_adjusted_revenue,
    (SUM(adjusted_revenue) - SUM(revenue))::NUMERIC(12,2) AS net_adjustment_impact,
    ((SUM(adjusted_revenue) - SUM(revenue)) / NULLIF(SUM(revenue),0) * 100)::NUMERIC(6,3) AS pct_impact
FROM mart.fact_performance_daily;
