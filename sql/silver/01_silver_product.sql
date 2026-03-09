DROP TABLE IF EXISTS silver_product;

CREATE TABLE silver_product AS
SELECT
    commodity_id AS product_id,
    raw_response->'data'->>'name' AS name,
    raw_response->'data'->>'code' AS code,
    (raw_response->'data'->>'gaPrice')::numeric AS price,
    (raw_response->'data'->>'cpriceNoCurrency')::numeric AS original_price,
    (raw_response->'data'->>'categoryName') AS category,
    (raw_response->'data'->>'gaCategory') AS parent_category,
    (raw_response->'data'->>'rating')::numeric AS rating,
    (raw_response->'data'->>'ratingCount')::int AS rating_count,
    (raw_response->'data'->>'sales')::int AS sales,
    (raw_response->'data'->>'warranty') AS warranty,
    (raw_response->'data'->>'is_in_stock')::boolean AS in_stock,
    raw_response->'data'->>'avail' AS availability,
    raw_response->'data'->>'spec' AS spec_summary,
    raw_response->'data'->>'eshop' AS eshop,
    (raw_response->'data'->>'producerId')::int AS producer_id,
    (raw_response->'data'->>'categoryId')::int AS category_id,
    -- Breadcrumb
    raw_response->'data'->'breadcrumb'->0->'category'->>'name' AS breadcrumb_section,
    raw_response->'data'->'breadcrumb'->1->'category'->>'name' AS breadcrumb_category,
    raw_response->'data'->'breadcrumb'->2->'category'->>'name' AS breadcrumb_subcategory
FROM bronze_raw_responses
WHERE endpoint = 'productDetail'
    AND http_status = 200
    AND raw_response IS NOT NULL;