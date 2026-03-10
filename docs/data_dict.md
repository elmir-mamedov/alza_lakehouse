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

### silver_product

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

### silver_review

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


### gold_top_product

---

## ML

### ml_price_features

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