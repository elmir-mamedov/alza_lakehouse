-- sql/gold/05_gold_ml_price_features.sql
--
-- Gold layer feature table for price prediction model.
-- Joins silver_products with silver_review_stats to create
-- a single flat table with all features needed for ML.
--
-- Target variable: price
-- Excludes: products with null/zero price, products without breadcrumb
--
-- Strong predictors: category hierarchy, producer, eshop, sales, warranty
-- Moderate predictors: rating, review stats, complaint rate, discount

DROP TABLE IF EXISTS gold_ml_price_features;

CREATE TABLE gold_ml_price_features AS
SELECT
    -- ID
    p.product_id,

    -- Target
    p.price,

    -- Strong predictors
    p.breadcrumb_section,
    p.breadcrumb_category,
    p.producer_id,
    p.eshop,
    p.sales,
    p.warranty,

    -- Moderate predictors
    p.rating,
    p.rating_count,
    p.in_stock,
    rs.review_count,
    rs.avg_rating,
    rs.recommendation_rate,
    rs.complaint_rate

FROM silver_product p
LEFT JOIN silver_review_stats rs ON p.product_id = rs.product_id
WHERE p.price > 0
    AND p.price IS NOT NULL
    AND p.breadcrumb_section IS NOT NULL;