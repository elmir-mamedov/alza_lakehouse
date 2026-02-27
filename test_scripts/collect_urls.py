"""collect_urls.py
Collect listing URLs from Bezrealitky and store them in the database queue.
"""
import asyncio
from db import get_connection
from sitemap import fetch_xml, parse_sitemap
from psycopg.rows import dict_row
import psycopg # popular PostgreSQL adapter for the Python.
import os
from dotenv import load_dotenv


# Load .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

SITEMAP_URLS = [
    "https://www.alza.cz/_sitemap-live-product.xml",
    "https://www.alza.cz/_sitemap-reviews.xml",
]

# Connect to PostgreSQL
def get_connection():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

async def collect():
    all_urls = []

    for sitemap_url in SITEMAP_URLS:
        xml = await fetch_xml(sitemap_url)
        urls = parse_sitemap(xml)

        if urls and urls[0].endswith(".xml"):
            for sub_sitemap_url in urls:
                sub_xml = await fetch_xml(sub_sitemap_url)
                all_urls.extend(parse_sitemap(sub_xml))
        else:
            all_urls.extend(urls)

    print(f"Discovered {len(all_urls)} URLs")

    # Insert URLs into url_queue

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO url_queue (url)
                VALUES (%s)
                ON CONFLICT (url) DO NOTHING;
                """,
                [(url,) for url in all_urls]
            )
            inserted = cur.rowcount
        conn.commit()

    print(f"Inserted {inserted} new URLs.")


if __name__ == "__main__":
    asyncio.run(collect())
