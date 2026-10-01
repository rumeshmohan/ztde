-- ============================================================
-- SILVER TRANSFORM - Content domain
-- Task: transform_content  (Silver Transformation stage)
-- primary_genre: the alphabetically first genre, same rule Mini
-- Project 01 used, now evaluated against bronze.raw_content_genre
-- instead of the live operational content_genre table.
-- ============================================================

INSERT INTO silver.content (
    content_id, title, content_type, release_date, duration_minutes,
    content_language, age_rating, primary_genre, batch_id
)
SELECT
    c.content_id, c.title, ct.content_type, c.release_date, c.duration_minutes,
    c.language, c.age_rating,
    (
        SELECT MIN(g.genre_name)
        FROM bronze.raw_content_genre cg
        JOIN bronze.raw_genre g ON g.genre_id = cg.genre_id
        WHERE cg.content_id = c.content_id
    ) AS primary_genre,
    c.batch_id
FROM bronze.raw_content c
JOIN bronze.raw_content_type ct ON ct.content_type_id = c.content_type_id
WHERE c.batch_id = '{{ run_id }}'
ON CONFLICT (content_id) DO UPDATE SET
    title = EXCLUDED.title, content_type = EXCLUDED.content_type,
    release_date = EXCLUDED.release_date, duration_minutes = EXCLUDED.duration_minutes,
    content_language = EXCLUDED.content_language, age_rating = EXCLUDED.age_rating,
    primary_genre = EXCLUDED.primary_genre,
    batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;
