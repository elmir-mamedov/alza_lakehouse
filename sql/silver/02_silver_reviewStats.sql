DROP TABLE IF EXISTS silver_review_stats;

CREATE TABLE silver_review_stats AS
SELECT
    commodity_id AS product_id,
    raw_response->>'name' AS name,
    (raw_response->>'ratingAverage')::numeric AS avg_rating,
    (raw_response->>'ratingCount')::int AS rating_count,
    (raw_response->>'reviewCount')::int AS review_count,
    (raw_response->>'recommendationRate')::numeric AS recommendation_rate,
    raw_response->>'purchaseCountFormatted' AS purchase_count_text,
    (raw_response->'complaint'->>'rate')::numeric AS complaint_rate,
    raw_response->'complaint'->>'description' AS complaint_description
FROM bronze_raw_responses
WHERE endpoint = 'reviewStats'
    AND http_status = 200
    AND raw_response IS NOT NULL;