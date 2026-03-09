-- sql/gold/01_gold_category_stats.sql

DROP TABLE IF EXISTS gold_category_stats;

CREATE TABLE gold_category_stats AS
SELECT
    p.parent_category,
    p.category,
    count(*) AS product_count,
    round(avg(p.price)::numeric, 2) AS avg_price,
    round(min(p.price)::numeric, 2) AS min_price,
    round(max(p.price)::numeric, 2) AS max_price,
    round(avg(p.rating)::numeric, 2) AS avg_rating,
    sum(p.sales) AS total_sales,
    round(avg(rs.complaint_rate)::numeric, 4) AS avg_complaint_rate,
    round(avg(rs.recommendation_rate)::numeric, 2) AS avg_recommendation_rate,
    sum(rs.review_count) AS total_reviews
FROM silver_products p
LEFT JOIN silver_review_stats rs ON p.product_id = rs.product_id
WHERE p.price > 0
GROUP BY p.parent_category, p.category
ORDER BY product_count DESC;