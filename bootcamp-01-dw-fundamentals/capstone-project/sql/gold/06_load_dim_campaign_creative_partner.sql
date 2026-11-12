-- gold.dim_campaign / dim_creative / dim_partner — all Type 1, idempotent
-- upserts (Q5: a rerun — whether a clean daily run or recovery after a
-- partial failure — must not error on rows already loaded, and should
-- refresh their attributes rather than silently no-op on stale ones).
--
-- Q6 fix: an earlier version of this script's ON CONFLICT DO UPDATE had no
-- WHERE guard, so Postgres wrote a new MVCC tuple version for every
-- conflicting row on every run regardless of whether any value actually
-- changed — which also meant "updated_count" could never be measured
-- honestly (everything looked "updated" every time). Fixed with an
-- IS DISTINCT FROM guard (skips a real no-op) and RETURNING (xmax = 0) to
-- report true inserted vs. updated counts back to the audit log — the
-- brief's "updated count" field, actually measured, not a placeholder 0.

WITH upserted_campaign AS (
    INSERT INTO gold.dim_campaign
        (campaign_id, campaign_name, advertiser_id, offer_id, offer_name,
         default_payout, payout_type, price_type, start_date, end_date)
    SELECT
        c.campaign_id, c.campaign_name, c.advertiser_id, c.offer_id,
        o.offer_name, o.default_payout, o.payout_type,
        c.price_type, c.start_date, c.end_date
    FROM silver.campaigns c
    LEFT JOIN silver.offers o ON o.offer_id = c.offer_id
    ON CONFLICT (campaign_id) DO UPDATE SET
        campaign_name = EXCLUDED.campaign_name, advertiser_id = EXCLUDED.advertiser_id,
        offer_id = EXCLUDED.offer_id, offer_name = EXCLUDED.offer_name,
        default_payout = EXCLUDED.default_payout, payout_type = EXCLUDED.payout_type,
        price_type = EXCLUDED.price_type, start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date
    WHERE gold.dim_campaign.campaign_name IS DISTINCT FROM EXCLUDED.campaign_name
       OR gold.dim_campaign.advertiser_id IS DISTINCT FROM EXCLUDED.advertiser_id
       OR gold.dim_campaign.offer_id IS DISTINCT FROM EXCLUDED.offer_id
       OR gold.dim_campaign.offer_name IS DISTINCT FROM EXCLUDED.offer_name
       OR gold.dim_campaign.default_payout IS DISTINCT FROM EXCLUDED.default_payout
       OR gold.dim_campaign.payout_type IS DISTINCT FROM EXCLUDED.payout_type
       OR gold.dim_campaign.price_type IS DISTINCT FROM EXCLUDED.price_type
       OR gold.dim_campaign.start_date IS DISTINCT FROM EXCLUDED.start_date
       OR gold.dim_campaign.end_date IS DISTINCT FROM EXCLUDED.end_date
    RETURNING (xmax = 0) AS is_insert
),
upserted_creative AS (
    INSERT INTO gold.dim_creative (creative_id, campaign_id, creative_name, format, size, landing_url)
    SELECT creative_id, campaign_id, creative_name, format, size, landing_url
    FROM silver.creatives
    ON CONFLICT (creative_id) DO UPDATE SET
        campaign_id = EXCLUDED.campaign_id, creative_name = EXCLUDED.creative_name,
        format = EXCLUDED.format, size = EXCLUDED.size, landing_url = EXCLUDED.landing_url
    WHERE gold.dim_creative.campaign_id IS DISTINCT FROM EXCLUDED.campaign_id
       OR gold.dim_creative.creative_name IS DISTINCT FROM EXCLUDED.creative_name
       OR gold.dim_creative.format IS DISTINCT FROM EXCLUDED.format
       OR gold.dim_creative.size IS DISTINCT FROM EXCLUDED.size
       OR gold.dim_creative.landing_url IS DISTINCT FROM EXCLUDED.landing_url
    RETURNING (xmax = 0) AS is_insert
),
upserted_partner AS (
    INSERT INTO gold.dim_partner
        (partner_id, partner_name, channels, primary_channel, media_buyer,
         join_date, payment_terms, source_system)
    SELECT partner_id, partner_name, channels, primary_channel, media_buyer,
           join_date, payment_terms, source_system
    FROM silver.partners
    ON CONFLICT (partner_id) DO UPDATE SET
        partner_name = EXCLUDED.partner_name, channels = EXCLUDED.channels,
        primary_channel = EXCLUDED.primary_channel, media_buyer = EXCLUDED.media_buyer,
        join_date = EXCLUDED.join_date, payment_terms = EXCLUDED.payment_terms,
        source_system = EXCLUDED.source_system
    WHERE gold.dim_partner.partner_name IS DISTINCT FROM EXCLUDED.partner_name
       OR gold.dim_partner.channels IS DISTINCT FROM EXCLUDED.channels
       OR gold.dim_partner.primary_channel IS DISTINCT FROM EXCLUDED.primary_channel
       OR gold.dim_partner.media_buyer IS DISTINCT FROM EXCLUDED.media_buyer
       OR gold.dim_partner.join_date IS DISTINCT FROM EXCLUDED.join_date
       OR gold.dim_partner.payment_terms IS DISTINCT FROM EXCLUDED.payment_terms
       OR gold.dim_partner.source_system IS DISTINCT FROM EXCLUDED.source_system
    RETURNING (xmax = 0) AS is_insert
)
SELECT
    (SELECT COUNT(*) FILTER (WHERE is_insert) FROM upserted_campaign) AS campaign_inserted,
    (SELECT COUNT(*) FILTER (WHERE NOT is_insert) FROM upserted_campaign) AS campaign_updated,
    (SELECT COUNT(*) FILTER (WHERE is_insert) FROM upserted_creative) AS creative_inserted,
    (SELECT COUNT(*) FILTER (WHERE NOT is_insert) FROM upserted_creative) AS creative_updated,
    (SELECT COUNT(*) FILTER (WHERE is_insert) FROM upserted_partner) AS partner_inserted,
    (SELECT COUNT(*) FILTER (WHERE NOT is_insert) FROM upserted_partner) AS partner_updated;
