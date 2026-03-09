-- sql/gold/04_gold_complaint_analysis.sql

DROP TABLE IF EXISTS gold_complaint_analysis;

CREATE TABLE gold_complaint_analysis AS
SELECT
    p.parent_category,
    p.category,
    p.product_id,
    p.name,
    p.price,
    rs.avg_rating,
    rs.review_count,
    rs.complaint_rate,
    rs.complaint_description,
    -- Flag high-risk products
    CASE
        WHEN rs.complaint_rate > 0.10 THEN 'high'
        WHEN rs.complaint_rate > 0.05 THEN 'medium'
        WHEN rs.complaint_rate > 0.02 THEN 'low'
        ELSE 'minimal'
    END AS risk_level
FROM silver_products p
JOIN silver_review_stats rs ON p.product_id = rs.product_id
WHERE rs.complaint_rate IS NOT NULL
ORDER BY rs.complaint_rate DESC;