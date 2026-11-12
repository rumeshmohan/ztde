-- ============================================================================
-- PulseTrack — Silver layer: schema + shared parsing functions
-- Reused across every entity load (Q3's "Maintainability": one regex-tested
-- function beats eleven near-identical CASE expressions). Formats handled
-- match the data guide's own "Parsing cheat-sheet" exactly.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- silver.parse_money: "$1,234.56" / "USD 12.34" / "12.34 USD" / "1234.56" /
-- "$ 1,234.56" / "N/A" / blank / negative — strip everything except digits,
-- '.', '-', then cast. Deliberately permissive (never raises): an
-- unparseable value becomes NULL, not a failed batch.
CREATE OR REPLACE FUNCTION silver.parse_money(raw TEXT)
RETURNS NUMERIC AS $$
DECLARE
    cleaned TEXT;
BEGIN
    IF raw IS NULL THEN RETURN NULL; END IF;
    cleaned := TRIM(raw);
    IF cleaned = '' OR UPPER(cleaned) = 'N/A' THEN RETURN NULL; END IF;
    cleaned := REGEXP_REPLACE(cleaned, '[^0-9.\-]', '', 'g');
    IF cleaned = '' OR cleaned = '-' THEN RETURN NULL; END IF;
    RETURN cleaned::NUMERIC;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- silver.parse_flexible_date: handles every format the guide documents —
-- 'YYYY-MM-DD [HH:MM:SS]', 'MM/DD/YYYY[ HH:MM]', 'DD-MM-YYYY' (dash = day
-- first, per the guide), 'Mon DD, YYYY', ISO with a 'Z' or +HH:MM/named-tz
-- suffix, and epoch seconds/millis.
--
-- CORRECTNESS NOTE (found by re-verifying resolved-vs-embedded account
-- managers against Gold data, not by inspection): an earlier version of
-- this function tried TO_TIMESTAMP() with each format in turn and returned
-- the first one that didn't raise. That is unsafe in Postgres specifically
-- because TO_TIMESTAMP() treats punctuation in the format string as "any
-- separator" — format 'MM/DD/YYYY' happily matches the DASH-separated input
-- "03-11-2025" and silently returns 11 March instead of the correct (per
-- the data guide: dash = DD-MM-YYYY) 3 November, with NO error, for every
-- day-of-month <= 12 (where both readings are valid dates). This is exactly
-- the kind of silent, undetectable-by-exception bug a "try each format,
-- keep the first success" cascade cannot catch. Fixed by classifying the
-- exact shape with a regex FIRST, then calling the one matching format —
-- never letting Postgres guess based on a punctuation-insensitive template.
CREATE OR REPLACE FUNCTION silver.parse_flexible_date(raw TEXT)
RETURNS TIMESTAMP AS $$
DECLARE
    s TEXT;
BEGIN
    IF raw IS NULL THEN RETURN NULL; END IF;
    s := TRIM(raw);
    IF s = '' OR UPPER(s) = 'N/A' THEN RETURN NULL; END IF;

    -- epoch millis / seconds
    IF s ~ '^\d{13}$' THEN
        RETURN TO_TIMESTAMP(s::BIGINT / 1000.0);
    END IF;
    IF s ~ '^\d{10}$' THEN
        RETURN TO_TIMESTAMP(s::BIGINT);
    END IF;

    -- strip a trailing named timezone abbreviation or +HH:MM/-HH:MM offset
    s := REGEXP_REPLACE(s, '\s*(UTC|GMT|EST|EDT|PST|PDT|CST|CDT)$', '');
    s := REGEXP_REPLACE(s, '([+-]\d{2}:\d{2})$', '');
    s := REGEXP_REPLACE(s, 'Z$', '');
    s := TRIM(s);

    -- classify by exact shape, THEN parse with the one correct format —
    -- never a try-cascade across formats that share a separator class.
    IF s ~ '^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}$' THEN
        RETURN TO_TIMESTAMP(REPLACE(s, 'T', ' '), 'YYYY-MM-DD HH24:MI:SS');
    ELSIF s ~ '^\d{4}-\d{2}-\d{2}$' THEN
        RETURN TO_TIMESTAMP(s, 'YYYY-MM-DD');
    ELSIF s ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$' THEN
        RETURN TO_TIMESTAMP(s, 'MM/DD/YYYY HH24:MI:SS');
    ELSIF s ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$' THEN
        RETURN TO_TIMESTAMP(s, 'MM/DD/YYYY HH24:MI');
    ELSIF s ~ '^\d{2}/\d{2}/\d{4}$' THEN
        RETURN TO_TIMESTAMP(s, 'MM/DD/YYYY');
    ELSIF s ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$' THEN
        RETURN TO_TIMESTAMP(s, 'DD-MM-YYYY HH24:MI:SS');
    ELSIF s ~ '^\d{2}-\d{2}-\d{4}$' THEN
        RETURN TO_TIMESTAMP(s, 'DD-MM-YYYY');
    ELSIF s ~ '^[A-Za-z]{3} \d{1,2}, \d{4}$' THEN
        RETURN TO_TIMESTAMP(s, 'FMMon DD, YYYY');
    ELSIF s ~ '^\d{1,2} [A-Za-z]{3} \d{4}$' THEN
        RETURN TO_TIMESTAMP(s, 'FMDD Mon YYYY');
    END IF;

    RETURN NULL;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- silver.canonical_country: "US"/"USA"/"United States"/... -> ISO-ish label.
-- A small, explicit lookup rather than a heuristic — extend as new source
-- variants are discovered (Q6's ongoing DQ monitoring).
CREATE OR REPLACE FUNCTION silver.canonical_country(raw TEXT)
RETURNS TEXT AS $$
    SELECT CASE UPPER(TRIM(raw))
        WHEN 'US' THEN 'US' WHEN 'USA' THEN 'US' WHEN 'UNITED STATES' THEN 'US'
        WHEN 'UK' THEN 'GB' WHEN 'GB' THEN 'GB' WHEN 'UNITED KINGDOM' THEN 'GB'
        WHEN 'CA' THEN 'CA' WHEN 'CANADA' THEN 'CA'
        ELSE NULLIF(UPPER(TRIM(raw)), '')
    END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION silver.canonical_device(raw TEXT)
RETURNS TEXT AS $$
    SELECT CASE LOWER(TRIM(raw))
        WHEN 'mobile' THEN 'mobile' WHEN 'mob' THEN 'mobile'
        WHEN 'desktop' THEN 'desktop'
        WHEN 'tablet' THEN 'tablet'
        WHEN 'ios' THEN 'mobile' WHEN 'android' THEN 'mobile'
        ELSE NULLIF(LOWER(TRIM(raw)), '')
    END;
$$ LANGUAGE sql IMMUTABLE;

-- silver.canonical_conversion_status: the source DDL states "~10
-- spellings/codes for approved/pending/reversed" — the live dataset has
-- EXACTLY 10 (verified, Q6): Pending/pending, "1", approved/app/Approved/
-- APPROVED, returned/Reversed/reversed. Nine of those ten are unambiguous
-- text variants; "1" is a bare numeric code with no data dictionary to
-- confirm its meaning. Mapped to 'approved' as a documented ASSUMPTION, not
-- a silent guess: its count (9,702) sits closest to the approved cluster
-- (9,470-9,536) of the three, and 1-as-success/approved is the common
-- convention for this kind of status flag. Before this fix, "1" fell
-- through to 'unknown' — 9,702 conversions (11.2% of the dataset) were
-- silently misclassified until Q6's data-quality check caught it.
CREATE OR REPLACE FUNCTION silver.canonical_conversion_status(raw TEXT)
RETURNS TEXT AS $$
    SELECT CASE LOWER(TRIM(raw))
        WHEN 'approved' THEN 'approved' WHEN 'app' THEN 'approved'
        WHEN 'confirmed' THEN 'approved' WHEN 'valid' THEN 'approved'
        WHEN '1' THEN 'approved'   -- documented assumption — see comment above
        WHEN 'pending' THEN 'pending' WHEN 'pend' THEN 'pending'
        WHEN 'processing' THEN 'pending' WHEN 'in_review' THEN 'pending'
        WHEN 'reversed' THEN 'reversed' WHEN 'returned' THEN 'reversed'
        WHEN 'rejected' THEN 'reversed' WHEN 'chargeback' THEN 'reversed'
        WHEN 'cancelled' THEN 'reversed' WHEN 'canceled' THEN 'reversed'
        ELSE 'unknown'
    END;
$$ LANGUAGE sql IMMUTABLE;

-- silver.is_truthy: normalises the mixed boolean/flag encoding used by both
-- is_unique (1/0/true/false/Y/N/blank) and is_test (test/1/true/0/blank —
-- the source DDL's own comment: "mixed flag values (test/1/true/0/blank) ->
-- filter TRUTHY rows out", so the literal word 'test' counts as truthy).
-- COALESCE(..., FALSE) is deliberate and load-bearing: a NULL/blank value
-- must mean "not truthy", never propagate as SQL NULL — `WHERE NOT
-- silver.is_truthy(x)` would otherwise silently exclude every row where x
-- is blank (NULL NOT NULL = NULL = excluded by WHERE), which is backwards
-- for is_test (blank must mean "not a test row", i.e. KEEP it). Caught by
-- testing this function's output against real data before trusting it.
CREATE OR REPLACE FUNCTION silver.is_truthy(raw TEXT)
RETURNS BOOLEAN AS $$
    SELECT COALESCE(LOWER(TRIM(raw)) IN ('1', 'true', 'y', 'yes', 't', 'test'), FALSE);
$$ LANGUAGE sql IMMUTABLE;

-- silver.parse_mongo_date: MongoDB export dates are inconsistently
-- represented — sometimes a plain "YYYY-MM-DD" string, sometimes
-- {"$date": "2025-09-14T05:19:24Z"} (verified empirically: e.g.
-- advertiser_accounts.createdAt is a plain string in 17/60 docs and a
-- {"$date":...} object in the other 43). Handles both.
CREATE OR REPLACE FUNCTION silver.parse_mongo_date(val JSONB)
RETURNS TIMESTAMP AS $$
BEGIN
    IF val IS NULL THEN RETURN NULL; END IF;
    IF jsonb_typeof(val) = 'object' THEN
        RETURN silver.parse_flexible_date(val->>'$date');
    ELSIF jsonb_typeof(val) = 'string' THEN
        RETURN silver.parse_flexible_date(val #>> '{}');
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- silver.canonical_partner_prefix: the canonical 5-digit numeric prefix from
-- a compound ("<prefix>_<subid>"), bare, "plat<hex>", or null partner_id.
-- "platXXXX" values are genuinely unattributable to any of the 640 known
-- prefixes (Q2 A.5) — returned as NULL so the caller routes to the -1
-- Unknown Partner member, not silently coerced into a wrong prefix.
CREATE OR REPLACE FUNCTION silver.canonical_partner_prefix(raw TEXT)
RETURNS TEXT AS $$
DECLARE
    s TEXT;
    m TEXT[];
BEGIN
    IF raw IS NULL THEN RETURN NULL; END IF;
    s := TRIM(raw);
    IF s = '' OR LOWER(s) LIKE 'plat%' THEN RETURN NULL; END IF;
    m := REGEXP_MATCH(s, '^(\d{5})');
    IF m IS NULL THEN RETURN NULL; END IF;
    RETURN m[1];
END;
$$ LANGUAGE plpgsql IMMUTABLE;
