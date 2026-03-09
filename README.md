# Alza lakehouse
<img width="1035" height="354" alt="image" src="docs/wordcloud.png" />

# Alza Product & Review Scraper → Lakehouse Pipeline

End-to-end data project: scrape product data from [Alza.cz](https://www.alza.cz) APIs, store raw responses in PostgreSQL (bronze layer), and build a medallion-architecture pipeline for analytics — with a sentiment analysis model on top.

## What it does

1. **Scrapes** 10 000 + products from Alza's frontend APIs (product details, review stats, individual reviews)
2. **Stores** raw JSON responses in a PostgreSQL bronze table — one row per API call
3. **Transforms** data through a medallion architecture (bronze → silver → gold) using PySpark + Delta Lake

## Project structure

```mermaid
flowchart TB
 subgraph Scraping["Scraping"]
        B[("url_queue")]
        A["Alza.cz Sitemap"]
  end
 subgraph subGraph1["Bronze Layer"]
        C[("bronze_raw_responses")]
  end
 subgraph subGraph2["Silver Layer"]
        G[("silver_review_stats")]
        H[("silver_review")]
        I[("silver_product")]
  end
 subgraph subGraph3["Gold Layer"]
        J[("gold_product")]
  end
 subgraph ML["ML"]
        N["ml_price_features"]
        O["Price Prediction Model"]
  end
    A -- "collect_urls.py" --> B
    B -- "scrape_batch_bronze.py" --> C
    C -- "01_silver_reviewStats.sql" --> G
    C -- "03_silver_reviews.sql" --> H
    C -- "01_silver_products.sql" --> I
    G -- gold SQL scripts --> J
    H -- gold SQL scripts --> J
    I -- gold SQL scripts --> J
    J --> N
    N --> O

    style A fill:#4a9eff,stroke:#2d7cd6,color:#fff
    style B fill:#4a9eff,stroke:#2d7cd6,color:#fff
    style C fill:#cd7f32,stroke:#a6652a,color:#fff
    style G fill:#c0c0c0,stroke:#999,color:#000
    style H fill:#c0c0c0,stroke:#999,color:#000
    style I fill:#c0c0c0,stroke:#999,color:#000
    style J fill:#ffd700,stroke:#cca900,color:#000
    style N fill:#9b59b6,stroke:#7d3c98,color:#fff
    style O fill:#9b59b6,stroke:#7d3c98,color:#fff
```

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
