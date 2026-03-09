-- sql/gold/02_gold_review_trends.sql

DROP TABLE IF EXISTS gold_review_trends;

CREATE TABLE gold_review_trends AS
SELECT
    date_trunc('month', review_date::timestamp) AS month,
    count(*) AS review_count,
    round(avg(rating)::numeric, 2) AS avg_rating,
    count(*) FILTER (WHERE rating >= 4) AS positive_reviews,
    count(*) FILTER (WHERE rating <= 2) AS negative_reviews,
    count(DISTINCT product_id) AS products_reviewed
FROM silver_reviews
WHERE review_date IS NOT NULL
GROUP BY date_trunc('month', review_date::timestamp)
ORDER BY month;