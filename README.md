# Alza lakehouse
<img width="1035" height="354" alt="image" src="docs/wordcloud.png" />

# Alza Product & Review Scraper → Lakehouse Pipeline

End-to-end data project: scrape product data from [Alza.cz](https://www.alza.cz) APIs, store raw responses in PostgreSQL (bronze layer), and build a medallion-architecture pipeline for analytics — with a sentiment analysis model on top.

## What it does

1. **Scrapes** ~5 000 products from Alza's frontend APIs (product details, review stats, individual reviews)
2. **Stores** raw JSON responses in a PostgreSQL bronze table — one row per API call
3. **Transforms** data through a medallion architecture (bronze → silver → gold) using PySpark + Delta Lake

## Project structure

```
├── get_data/
│   ├── collect_urls.py            # Crawl Alza sitemap for product URLs
│   ├── sitemap.py                 # Sitemap XML parser
│   ├── extract_commodity_id.sql   # SQL to extract product IDs from URLs
│   ├── scrape_batch_bronze.py     # Main scraper — hits 3 API endpoints per product
│   └── db.py                      # PostgreSQL connection + url_queue helpers
├── eda_reviews.ipynb              # Exploratory analysis on scraped reviews
├── DATABRICKS_PREP_PLAN.md        # Roadmap for PySpark + Delta Lake exercises
├── Dockerfile
├── pyproject.toml
└── requirements.txt
```

## Data architecture

**Bronze layer** — raw API responses stored in `bronze_raw_responses` table:

| Endpoint | What it contains |
|---|---|
| `productDetail` | Price, category, specs, availability, breadcrumb, related products |
| `reviewStats` | Average rating, rating breakdown, recommendation rate, complaint rate |
| `reviews` | Individual review text, author rating, date, pros/cons |

Each row stores the full JSON response with metadata (commodity_id, endpoint, http_status, scraped_at, batch_id).

**Silver / Gold layers** — planned via PySpark + Delta Lake (see `DATABRICKS_PREP_PLAN.md`).

## Scraping details

- Discovers product URLs from Alza's XML sitemap
- Extracts commodity IDs and queues them in PostgreSQL (`url_queue` table)
- Hits 3 API endpoints per product with randomized throttling to avoid detection
- Uses `curl_cffi` with Chrome impersonation for anti-bot bypass
- Handles pagination for reviews (100 per page)
- Stores every raw response — no data is lost or transformed at ingestion

## Setup

```bash
# Clone and install
git clone <repo-url>
cd API_prj
uv sync  # or: pip install -r requirements.txt

# Set up .env with your PostgreSQL connection
echo "DATABASE_URL=postgresql://user:pass@localhost:5432/alza_db" > .env

# Initialize the database
python -c "from get_data.db import init_db; init_db()"

# Collect product URLs from sitemap
python get_data/collect_urls.py

# Run the scraper
python get_data/scrape_batch_bronze.py
```

## Tech stack

Python 3.14, PostgreSQL, psycopg 3, curl_cffi, pandas

## Status

- [x] Sitemap crawling + URL queue
- [x] Bronze scraper (reviewStats, reviews, productDetail)
- [ ] Silver layer (PySpark cleaning + flattening)
- [ ] Gold layer (analytical aggregations)
- [ ] Delta Lake exercises

#### Data & Ethics Note

--- Data was scraped from Alza.cz for personal research and model
training purposes only. No data is redistributed, published, or used commercially.
The scraper mimics respectful browsing behavior with randomized delays to avoid 
server load. ---