-- ============================================================================
-- PulseTrack — Data Mart calculated metrics (Q7)
-- MEASURES (stored, additive: impressions/clicks/conversions/revenue/...)
-- vs CALCULATED METRICS (never stored — ratios computed from measures at
-- query time, since a ratio is not itself additive across dimensions).
-- Every formula matches Q1 §3 exactly; the attribution_percentage
-- aggregation rule (SUM/SUM, never AVG of per-row ratios — Q1 §3.4) is
-- enforced here, in the view definition, not left to whichever aggregate
-- function a BI tool defaults to.
--
-- Representative views only — the full roll-up-at-every-grain cube is Q8's
-- deliverable, built FROM this mart. These demonstrate the pattern at the
-- advertiser and partner grain (the two Q1 KPI areas that mix direct and
-- attributed measures) and rely on SQL's own SUM(x)/NULLIF(SUM(y),0)
-- composing correctly under any further GROUP BY a caller adds.
-- ============================================================================

CREATE OR REPLACE VIEW mart.vw_advertiser_performance AS
SELECT
    p.date_key, p.advertiser_id, a.advertiser_name, a.current_account_manager_name,
    SUM(p.impressions) AS impressions,
    SUM(p.clicks) AS clicks,
    SUM(p.conversions) AS conversions,
    SUM(p.revenue) AS revenue,
    SUM(p.adjusted_revenue) AS adjusted_revenue,
    -- calculated metrics: ratio of sums, never averaged per-row
    SUM(p.clicks)::NUMERIC / NULLIF(SUM(p.impressions), 0) AS ctr,
    SUM(p.conversions)::NUMERIC / NULLIF(SUM(p.clicks), 0) AS conversion_rate,
    SUM(p.revenue) / NULLIF(SUM(p.conversions), 0) AS revenue_per_conversion,
    SUM(p.revenue) / NULLIF(SUM(p.clicks), 0) AS revenue_per_click
FROM mart.fact_performance_daily p
JOIN mart.dim_advertiser a ON a.advertiser_id = p.advertiser_id
GROUP BY p.date_key, p.advertiser_id, a.advertiser_name, a.current_account_manager_name;

CREATE OR REPLACE VIEW mart.vw_campaign_performance AS
SELECT
    p.date_key, p.campaign_id, c.campaign_name, c.advertiser_id, c.advertiser_name,
    SUM(p.impressions) AS impressions,
    SUM(p.clicks) AS clicks,
    SUM(p.conversions) AS conversions,
    SUM(p.revenue) AS revenue,
    SUM(p.adjusted_revenue) AS adjusted_revenue,
    SUM(p.clicks)::NUMERIC / NULLIF(SUM(p.impressions), 0) AS ctr,
    SUM(p.conversions)::NUMERIC / NULLIF(SUM(p.clicks), 0) AS conversion_rate
FROM mart.fact_performance_daily p
JOIN mart.dim_campaign c ON c.campaign_id = p.campaign_id
GROUP BY p.date_key, p.campaign_id, c.campaign_name, c.advertiser_id, c.advertiser_name;

CREATE OR REPLACE VIEW mart.vw_creative_performance AS
SELECT
    p.date_key, p.creative_id, cr.creative_name, cr.campaign_id, cr.campaign_name,
    SUM(p.impressions) AS impressions,
    SUM(p.clicks) AS clicks,
    SUM(p.conversions) AS conversions,
    SUM(p.clicks)::NUMERIC / NULLIF(SUM(p.impressions), 0) AS ctr,
    SUM(p.conversions)::NUMERIC / NULLIF(SUM(p.clicks), 0) AS conversion_rate
FROM mart.fact_performance_daily p
JOIN mart.dim_creative cr ON cr.creative_id = p.creative_id
GROUP BY p.date_key, p.creative_id, cr.creative_name, cr.campaign_id, cr.campaign_name;

-- Partner Performance is the one KPI area that genuinely needs BOTH facts:
-- direct (last-touch) conversions from fact_performance_daily, and
-- fractional attributed conversions/revenue from fact_attribution_daily —
-- joined at (date, partner), NOT summed together (they answer different
-- questions, per Q1 §3.4: direct vs. attributed are two different KPIs).
-- "Reachable conversion revenue" per (date, partner): the total revenue of
-- every conversion that partner touched at all, at least once — the
-- correct denominator for attribution_percentage (Q1 §3.4: a partner's
-- share of the revenue from conversions it contributed to, not of ALL
-- revenue). Computed as its own aggregate here — joined normally below,
-- not as a per-row correlated subquery (which would re-scan Gold's bridge
-- for every mart row; exactly the ad-hoc-query cost this mart exists to
-- avoid — caught before this view was trusted, not after).
CREATE OR REPLACE VIEW mart.vw_partner_reachable_revenue AS
SELECT p.partner_id, fcv.date_key, SUM(fcv.revenue) AS reachable_revenue
FROM (
    -- DISTINCT (conversion, partner): a conversion touched by the same
    -- partner via 2+ touchpoints must count its revenue once, not per touch
    SELECT DISTINCT bt.conversion_key, fcl.partner_key
    FROM gold.bridge_conversion_touchpoint bt
    JOIN gold.fact_click fcl ON fcl.click_key = bt.click_key
) touched
JOIN gold.fact_conversion fcv ON fcv.conversion_key = touched.conversion_key
JOIN gold.dim_partner p ON p.partner_key = touched.partner_key
GROUP BY p.partner_id, fcv.date_key;

CREATE OR REPLACE VIEW mart.vw_partner_performance AS
SELECT
    COALESCE(perf.date_key, attr.date_key) AS date_key,
    COALESCE(perf.partner_id, attr.partner_id) AS partner_id,
    pt.partner_name,
    COALESCE(perf.clicks, 0) AS clicks,
    COALESCE(perf.conversions, 0) AS conversions,                 -- direct, last-touch
    COALESCE(attr.attributed_conversions, 0) AS attributed_conversions,  -- fractional, multi-touch
    COALESCE(attr.attributed_revenue, 0) AS attributed_revenue,
    -- attribution percentage: SUM(attributed revenue) / SUM(reachable
    -- conversion revenue) — ratio of sums (Q1 §3.4), never averaged per row.
    attr.attributed_revenue / NULLIF(rr.reachable_revenue, 0) AS attribution_percentage
FROM (SELECT date_key, partner_id, SUM(clicks) AS clicks, SUM(conversions) AS conversions
      FROM mart.fact_performance_daily GROUP BY date_key, partner_id) perf
FULL OUTER JOIN (SELECT date_key, partner_id, SUM(attributed_conversions) AS attributed_conversions, SUM(attributed_revenue) AS attributed_revenue
                 FROM mart.fact_attribution_daily GROUP BY date_key, partner_id) attr
    ON attr.date_key = perf.date_key AND attr.partner_id = perf.partner_id
JOIN mart.dim_partner pt ON pt.partner_id = COALESCE(perf.partner_id, attr.partner_id)
LEFT JOIN mart.vw_partner_reachable_revenue rr
    ON rr.partner_id = COALESCE(perf.partner_id, attr.partner_id) AND rr.date_key = COALESCE(perf.date_key, attr.date_key);

CREATE OR REPLACE VIEW mart.vw_account_manager_performance AS
SELECT
    p.date_key, p.account_manager_id, am.am_name,
    SUM(p.conversions) AS conversions,
    SUM(p.revenue) AS revenue,
    SUM(p.adjusted_revenue) AS adjusted_revenue
FROM mart.fact_performance_daily p
JOIN mart.dim_account_manager am ON am.am_id = p.account_manager_id
WHERE p.account_manager_id IS NOT NULL   -- excludes impression/click-only rows (N/A, not a real AM gap)
GROUP BY p.date_key, p.account_manager_id, am.am_name;
