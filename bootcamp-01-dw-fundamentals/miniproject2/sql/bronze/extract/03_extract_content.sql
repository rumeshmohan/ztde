-- ============================================================
-- BRONZE EXTRACT - Content domain
-- Task: extract_content  (Extract stage, Airflow TaskGroup)
-- Same full-extract-as-of-{{ data_interval_end }} + upsert pattern as
-- 01_extract_reference.sql (see that file's header for why this is a
-- full extract, not a windowed one).
-- ============================================================

INSERT INTO bronze.raw_content (
    content_id, content_type_id, title, release_date, duration_minutes,
    language, age_rating, created_at, updated_at, batch_id
)
SELECT
    content_id, content_type_id, title, release_date, duration_minutes,
    language, age_rating, created_at, updated_at, '{{ run_id }}'
FROM source.content
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (content_id) DO UPDATE SET
    content_type_id = EXCLUDED.content_type_id, title = EXCLUDED.title,
    release_date = EXCLUDED.release_date, duration_minutes = EXCLUDED.duration_minutes,
    language = EXCLUDED.language, age_rating = EXCLUDED.age_rating,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;

-- content_genre is a pure many-to-many bridge with no independent
-- update semantics beyond "does this pairing exist" - re-extracting
-- it on the same watermark window is naturally idempotent via the
-- composite-key ON CONFLICT below.
INSERT INTO bronze.raw_content_genre (content_id, genre_id, created_at, updated_at, batch_id)
SELECT content_id, genre_id, created_at, updated_at, '{{ run_id }}'
FROM source.content_genre
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (content_id, genre_id) DO UPDATE SET
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;
