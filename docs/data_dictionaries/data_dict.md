# Data Dictionary

## Bronze Layer

### bronze_raw_responses

Raw, untransformed API responses from Alza.cz. One row per API call. 
This is the single source of truth — no data is lost or modified at ingestion.

| Column | Type | Description |
|---|---|---|
| id | bigserial | Auto-incrementing primary key |
| commodity_id | integer | Alza product ID |
| source_url | text | Original product page URL from url_queue |
| endpoint | text | API endpoint name: `reviewStats`, `reviews`, or `productDetail` |
| request_params | jsonb | Query parameters sent with the request (e.g. ucik, pgrik, limit, offset) |
| http_status | integer | HTTP response code (200 = success), null on network failure |
| raw_response | jsonb | Complete API response stored as-is |
| scraped_at | timestamptz | Timestamp of when the row was inserted |
| batch_id | text | Unique identifier for the scraping session (e.g. `20260309T143022Z`) |

### url_queue

Product URLs discovered from Alza's XML sitemap. Used to track scraping progress.

| Column | Type | Description |
|---|---|---|
| url | text | Full product page URL (primary key) |
| processed | boolean | Whether all 3 endpoints have been scraped for this product |
| created_at | timestamp | When the URL was added to the queue |
| commodity_id | integer | Alza product ID extracted from the URL |

---

## Silver Layer

### silver_products

Cleaned, flattened product data extracted from the `productDetail` API endpoint.

| Column | Type | Source JSON path | Description |
|---|---|---|---|
| product_id | integer | `commodity_id` (table metadata) | Alza product ID |
| name | text | `data.name` | Product name |
| code | text | `data.code` | Alza internal product code |
| price | numeric | `data.gaPrice` | Current price in CZK (numeric, no formatting) |
| original_price | numeric | `data.cpriceNoCurrency` | Price before discount, null if no discount |
| category | text | `data.categoryName` | Lowest-level category (e.g. "Cordless") |
| parent_category | text | `data.gaCategory` | Top-level section (e.g. "House, Hobby and Garden") |
| rating | numeric | `data.rating` | Average star rating (0–5) |
| rating_count | integer | `data.ratingCount` | Number of star ratings |
| sales | integer | `data.sales` | Total units sold |
| warranty | text | `data.warranty` | Warranty period (e.g. "24 months") |
| in_stock | boolean | `data.is_in_stock` | Whether product is currently available |
| availability | text | `data.avail` | Stock text (e.g. "In stock > 5 pcs") |
| spec_summary | text | `data.spec` | One-line spec string from Alza |
| eshop | text | `data.eshop` | Which Alza sub-store (e.g. "Hobby", "Alza", "Trendy") |
| producer_id | integer | `data.producerId` | Manufacturer ID |
| category_id | integer | `data.categoryId` | Numeric category ID |
| breadcrumb_section | text | `data.breadcrumb[0].category.name` | Level 0 — top section |
| breadcrumb_category | text | `data.breadcrumb[1].category.name` | Level 1 — mid category |
| breadcrumb_subcategory | text | `data.breadcrumb[2].category.name` | Level 2 — sub category |

### silver_review_stats

Per-product review summary extracted from the `reviewStats` API endpoint.

| Column | Type | Source JSON path | Description |
|---|---|---|---|
| product_id | integer | `commodity_id` (table metadata) | Alza product ID |
| name | text | `name` | Product name |
| avg_rating | numeric | `ratingAverage` | Average star rating (0–5) |
| rating_count | integer | `ratingCount` | Total number of star ratings |
| review_count | integer | `reviewCount` | Number of text reviews |
| recommendation_rate | numeric | `recommendationRate` | Fraction of reviews that are positive (0–1) |
| purchase_count_text | text | `purchaseCountFormatted` | Purchase volume bucket (e.g. "500+", "1 000+") |
| complaint_rate | numeric | `complaint.rate` | Product return/complaint rate (0–1) |
| complaint_description | text | `complaint.description` | Complaint level label (Czech, e.g. "nízká reklamovanost") |

### silver_reviews

Individual customer reviews extracted from the `reviews` API endpoint. One row per review.

| Column | Type | Source JSON path | Description |
|---|---|---|---|
| product_id | integer | `commodity_id` (table metadata) | Alza product ID |
| author | text | `value[].name` | Reviewer name and city (e.g. "Jaroslav, Havířov 1") |
| rating | numeric | `value[].rating` | Star rating given by reviewer (1–5) |
| review_text | text | `value[].description` | Free-text review body (often empty) |
| pros | jsonb | `value[].positives` | Array of positive points (Czech text) |
| cons | jsonb | `value[].negatives` | Array of negative points (Czech text) |
| review_date | text | `value[].reviewDate` | ISO timestamp of when the review was posted |
| like_count | integer | `value[].likeCount` | Number of "helpful" votes |
| is_translated | boolean | `value[].isTranslated` | Whether the review was machine-translated |
| product_name | text | `value[].commodityName` | Product name as shown in the review |

---

## Gold Layer

### gold_category_stats

Aggregated product and review metrics per category. One row per parent_category + category combination.

| Column | Type | Description |
|---|---|---|
| parent_category | text | Top-level section |
| category | text | Product category |
| product_count | integer | Number of products in this category |
| avg_price | numeric | Average product price (CZK) |
| min_price | numeric | Cheapest product in category |
| max_price | numeric | Most expensive product in category |
| avg_rating | numeric | Average star rating across products |
| total_sales | integer | Sum of units sold |
| avg_complaint_rate | numeric | Average complaint rate |
| avg_recommendation_rate | numeric | Average recommendation rate |
| total_reviews | integer | Sum of text reviews |

### gold_top_products

Products ranked by a quality score that balances rating with review volume. Filtered to products with 5+ reviews.

| Column | Type | Description |
|---|---|---|
| product_id | integer | Alza product ID |
| name | text | Product name |
| price | numeric | Current price (CZK) |
| category | text | Product category |
| parent_category | text | Top-level section |
| in_stock | boolean | Stock availability |
| avg_rating | numeric | Average star rating |
| review_count | integer | Number of text reviews |
| recommendation_rate | numeric | Fraction of positive reviews |
| complaint_rate | numeric | Return/complaint rate |
| quality_score | numeric | Weighted score: 70% rating + 30% review volume (capped at 100 reviews) |

### gold_review_trends

Monthly aggregation of review activity across all products.

| Column | Type | Description |
|---|---|---|
| month | timestamp | First day of the month |
| review_count | integer | Total reviews posted that month |
| avg_rating | numeric | Average rating that month |
| positive_reviews | integer | Reviews with rating >= 4 |
| negative_reviews | integer | Reviews with rating <= 2 |
| products_reviewed | integer | Distinct products that received reviews |

### gold_complaint_analysis

Product-level complaint risk assessment. Only includes products with a known complaint rate.

| Column | Type | Description |
|---|---|---|
| parent_category | text | Top-level section |
| category | text | Product category |
| product_id | integer | Alza product ID |
| name | text | Product name |
| price | numeric | Current price (CZK) |
| avg_rating | numeric | Average star rating |
| review_count | integer | Number of text reviews |
| complaint_rate | numeric | Return/complaint rate |
| complaint_description | text | Complaint level label (Czech) |
| risk_level | text | Derived flag: `high` (>10%), `medium` (>5%), `low` (>2%), `minimal` |

---

## ML

### ml_price_features

Feature table for price prediction model. Joins silver_products with silver_review_stats. Excludes products with zero or null price.

| Column | Type | Description |
|---|---|---|
| product_id | integer | Alza product ID |
| price | numeric | Target variable — current price (CZK) |
| category | text | Product category |
| parent_category | text | Top-level section |
| breadcrumb_section | text | Breadcrumb level 0 |
| rating | numeric | Product rating |
| rating_count | integer | Number of star ratings |
| sales | integer | Units sold |
| in_stock | boolean | Stock availability |
| warranty | text | Warranty period |
| eshop | text | Alza sub-store |
| avg_rating | numeric | Average rating from review stats |
| review_count | integer | Number of text reviews |
| recommendation_rate | numeric | Fraction of positive reviews |
| complaint_rate | numeric | Return/complaint rate |
| purchase_count_text | text | Purchase volume bucket |
| rating_review_gap | integer | Difference between review_count and rating_count |
| discount_pct | numeric | Discount percentage from original price |

---

## Data not extracted into silver (remains in bronze only)

The following data exists in the raw `productDetail` JSON but was not pulled into the silver layer:

- **Structured specs** (`parameterGroups`) — full key-value product parameters (e.g. Battery voltage: 12V, Weight: 1.3kg)
- **Images** (`imgs`) — up to 11 image URLs per product in multiple resolutions
- **Related products** (`related_commodity`) — up to 16 recommended products with names, prices, ratings
- **Accessories/services** (`accessories`) — extended warranty, insurance, return policy options and prices
- **SEO categories** (`seo_categories`) — category tags and producer associations
- **Variant info** (`variantGroups`, `productVariantsInfo`) — color/size variants and their IDs
- **Pricing details** (`priceInfo`, `priceInfoV2`, `priceInfoV3`) — VAT breakdowns, instalment pricing, delayed payment
- **Delivery info** (`deliveryAvailabilities`) — fastest delivery time
- **Discount labels** (`labels`) — discount badge info
- **Producer website** (`links`) — manufacturer URLs
- **Discussion count** (`discussionPostCount`) — number of Q&A posts