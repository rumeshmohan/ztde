-- gold.fact_impression / fact_click — factless funnel facts. Unresolved
-- DIMENSION references route to the -1 Unknown member (never dropped, Q2
-- B.3). An unparseable/blank event TIMESTAMP is different: date_key is the
-- fact's own grain-defining attribute (NOT NULL, no "Unknown Date" member
-- makes sense the way "Unknown Partner" does), and it's non-trivial here —
-- 8,968/300,000 impressions (2.99%) and 4,396/150,000 clicks (2.93%) have no
-- parseable timestamp. These rows are explicitly quarantined, not silently
-- filtered out by a bare WHERE clause.

-- Idempotent (Q5): NOT EXISTS guards mean a rerun — whether a clean daily
-- run or a resume after a partial failure — never re-quarantines the same
-- row twice and never errors on a fact row already loaded.
INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'gold_load', 'silver.impressions', impression_id,
       'unparseable/blank impression_time — cannot assign a date_key', impression_id
FROM silver.impressions i
WHERE i.impression_time IS NULL
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'silver.impressions' AND q.source_pk = i.impression_id);

INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
SELECT 'gold_load', 'silver.clicks', click_id,
       'unparseable/blank click_time — cannot assign a date_key', click_id
FROM silver.clicks c
WHERE c.click_time IS NULL
  AND NOT EXISTS (SELECT 1 FROM audit.quarantine q WHERE q.source_table = 'silver.clicks' AND q.source_pk = c.click_id);

INSERT INTO gold.fact_impression
    (impression_id, date_key, campaign_key, creative_key, partner_key,
     country, device, impression_time)
SELECT
    i.impression_id,
    TO_CHAR(i.impression_time, 'YYYYMMDD')::INT,
    COALESCE(cmp.campaign_key, -1),
    COALESCE(cre.creative_key, -1),
    COALESCE(par.partner_key, -1),
    i.country, i.device, i.impression_time
FROM silver.impressions i
LEFT JOIN gold.dim_campaign cmp ON cmp.campaign_id = i.campaign_id
LEFT JOIN gold.dim_creative cre ON cre.creative_id = i.creative_id
LEFT JOIN gold.dim_partner par ON par.partner_id = i.partner_prefix
WHERE i.impression_time IS NOT NULL   -- no date_key possible without a date; see Q4 report note
ON CONFLICT (impression_id) DO NOTHING;

INSERT INTO gold.fact_click
    (click_id, impression_id, date_key, campaign_key, creative_key, partner_key,
     country, device, click_time)
SELECT
    c.click_id, c.impression_id,
    TO_CHAR(c.click_time, 'YYYYMMDD')::INT,
    COALESCE(cmp.campaign_key, -1),
    COALESCE(cre.creative_key, -1),
    COALESCE(par.partner_key, -1),
    c.country, c.device, c.click_time
FROM silver.clicks c
LEFT JOIN gold.dim_campaign cmp ON cmp.campaign_id = c.campaign_id
LEFT JOIN gold.dim_creative cre ON cre.creative_id = c.creative_id
LEFT JOIN gold.dim_partner par ON par.partner_id = c.partner_prefix
WHERE c.click_time IS NOT NULL
ON CONFLICT (click_id) DO NOTHING;
