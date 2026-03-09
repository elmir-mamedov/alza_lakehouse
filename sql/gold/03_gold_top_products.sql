-- sql/gold/03_gold_top_products.sql

DROP TABLE IF EXISTS gold_top_products;

CREATE TABLE gold_top_products AS
SELECT
    p.product_id,
    p.name,
    p.price,
    p.category,
    p.parent_category,
    p.in_stock,
    rs.avg_rating,
    rs.review_count,
    rs.recommendation_rate,
    rs.complaint_rate,
    -- Weighted score: balances rating with review volume
    round(
        (rs.avg_rating * 0.7 + least(rs.review_count::numeric / 100, 1) * 5 * 0.3)::numeric,
        2
    ) AS quality_score
FROM silver_products p
JOIN silver_review_stats rs ON p.product_id = rs.product_id
WHERE rs.review_count >= 5 AND rs.avg_rating IS NOT NULL
ORDER BY quality_score DESC;