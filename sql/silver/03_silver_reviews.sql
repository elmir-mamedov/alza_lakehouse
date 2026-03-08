DROP TABLE IF EXISTS silver_reviews;

CREATE TABLE silver_reviews AS
SELECT
    commodity_id AS product_id,
    elem->>'name' AS author,
    (elem->>'rating')::numeric AS rating,
    elem->>'description' AS review_text,
    elem->>'reviewDate' AS review_date,
    elem->'positives' AS pros,
    elem->'negatives' AS cons,
    (elem->>'likeCount')::int AS like_count,
    (elem->>'isTranslated')::boolean AS is_translated,
    elem->>'commodityName' AS product_name
FROM bronze_raw_responses,
     jsonb_array_elements(raw_response->'value') AS elem
WHERE endpoint = 'reviews'
    AND http_status = 200
    AND raw_response IS NOT NULL;