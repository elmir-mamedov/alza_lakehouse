"""scrape_batch.py
Scrape reviews for a batch of products from url_queue and save to reviews.csv
"""
import csv
import time
from curl_cffi.requests import Session
from db import get_connection

BATCH_SIZE = 100
OUTPUT_FILE = "batch_of_reviews.csv"

import time
import random


def fetch_reviews(session, commodity_id: int) -> list[dict]:
    reviews = []
    limit = 100
    offset = 0
    max_retries = 3

    while True:
        url = (
            f"https://webapi.alza.cz/api/catalog/v2/commodities/"
            f"{commodity_id}/reviews?country=cz&limit={limit}&offset={offset}"
        )
        for attempt in range(max_retries):
            try:
                response = session.get(url, impersonate="chrome120", timeout=10)
                if not response.text.strip():
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt + random.uniform(1, 3)
                        print(f"  Empty response, retrying in {wait:.1f}s...")
                        time.sleep(wait)
                        continue
                    else:
                        return reviews  # give up after retries
                data = response.json()
                break
            except Exception as e:
                print(f"  ERROR: {e}")
                return reviews

        for review in data.get("value", []):
            for text in review.get("positives", []):
                if text.strip():
                    reviews.append({"text": text.strip(), "label": 1})
            for text in review.get("negatives", []):
                if text.strip():
                    reviews.append({"text": text.strip(), "label": 0})

        offset += limit
        if offset >= data.get("paging", {}).get("size", 0):
            break

    return reviews

def main():
    all_reviews = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Fetch unprocessed URLs that have a commodity_id
            cur.execute(
                """
                SELECT url, commodity_id FROM url_queue
                WHERE processed = false
                  AND commodity_id IS NOT NULL
                LIMIT %s;
                """,
                (BATCH_SIZE,),
            )
            rows = cur.fetchall()

    print(f"Fetched {len(rows)} URLs to process")

    with Session() as session:
        for i, row in enumerate(rows, 1):
            url = row["url"]
            commodity_id = row["commodity_id"]
            print(f"[{i}/{len(rows)}] Scraping commodity {commodity_id} — {url}")
            reviews = fetch_reviews(session, commodity_id)
            print(f"  → {len(reviews)} reviews")
            all_reviews.extend(reviews)

            # Mark as processed
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE url_queue SET processed = true WHERE url = %s;",
                        (url,),
                    )
                conn.commit()

            # Normal browse pace
            time.sleep(random.uniform(4.5, 8.0))

            # Occasionally simulate "reading" a product page
            if random.random() < 0.15:  # 15% of the time
                pause = random.uniform(8.0, 20.0)
                print(f"  [human pause: {pause:.1f}s]")
                time.sleep(pause)

    # Append to CSV (create with header if new file)
    write_header = False
    try:
        with open(OUTPUT_FILE, "r") as f:
            write_header = f.readline() == ""
    except FileNotFoundError:
        write_header = True

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["text", "label"])
        if write_header:
            writer.writeheader()
        writer.writerows(all_reviews)

    print(f"\nDone. {len(all_reviews)} reviews appended to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()