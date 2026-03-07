"""scrape_batch_bronze.py
Bronze layer: scrape raw JSON responses for a batch of products
from url_queue and persist them as-is to a bronze schema in the DB.

No parsing, no flattening — just raw API responses with metadata.
"""
import json
import time
import random
import re
from datetime import datetime, timezone

from curl_cffi.requests import Session
from db import get_connection

BATCH_SIZE =10000

# ---------------------------------------------------------------------------
# Bronze table setup
# ---------------------------------------------------------------------------

CREATE_BRONZE_TABLE = """
CREATE TABLE IF NOT EXISTS bronze_raw_responses (
    id              BIGSERIAL PRIMARY KEY,
    commodity_id    INTEGER NOT NULL,
    source_url      TEXT NOT NULL,
    endpoint        TEXT NOT NULL,          -- 'reviewStats' | 'reviews'
    request_params  JSONB NOT NULL DEFAULT '{}',
    http_status     INTEGER,
    raw_response    JSONB,
    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    batch_id        TEXT
);
"""

CREATE_BRONZE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_bronze_commodity
    ON bronze_raw_responses (commodity_id, endpoint);
"""


def ensure_bronze_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_BRONZE_TABLE)
        cur.execute(CREATE_BRONZE_INDEX)
    conn.commit()


# ---------------------------------------------------------------------------
# Tracking params (unchanged)
# ---------------------------------------------------------------------------

def get_tracking_params(session) -> tuple[str, str]:
    r = session.get("https://www.alza.cz", impersonate="chrome120")
    ucik = re.search(r"ucik=(u_[a-z0-9_]+)", r.text)
    pgrik = re.search(r"pgrik=(p_[a-z0-9_]+)", r.text)
    return (
        ucik.group(1) if ucik else "",
        pgrik.group(1) if pgrik else "",
    )


# ---------------------------------------------------------------------------
# Raw fetchers — return (http_status, raw_json | None)
# ---------------------------------------------------------------------------

def fetch_raw_product_detail(session, commodity_id: int):
    """Fetch product detail endpoint, return (status_code, parsed_json | None)."""
    url = (
        f"https://www.alza.cz/Services/RestService.svc/v13/product/"
        f"{commodity_id}?country=cz"
    )
    try:
        resp = session.get(url, impersonate="chrome120", timeout=10)
        body = resp.text.strip()
        return resp.status_code, json.loads(body) if body else None
    except Exception as e:
        print(f"  ERROR fetching productDetail: {e}")
        return None, None

def fetch_raw_review_stats(session, commodity_id: int, ucik: str, pgrik: str):
    """Fetch reviewStats endpoint, return (status_code, parsed_json | None)."""
    url = (
        f"https://webapi.alza.cz/api/catalog/v2/commodities/"
        f"{commodity_id}/reviewStats?country=CZ&ucik={ucik}&pgrik={pgrik}"
    )
    try:
        resp = session.get(url, impersonate="chrome120", timeout=10)
        body = resp.text.strip()
        return resp.status_code, json.loads(body) if body else None
    except Exception as e:
        print(f"  ERROR fetching reviewStats: {e}")
        return None, None


def fetch_raw_reviews(session, commodity_id: int):
    """
    Paginate through the reviews endpoint.
    Yields (status_code, raw_json, request_params) per page so every
    page is stored individually in bronze.
    """
    limit = 100
    offset = 0
    max_retries = 3

    while True:
        url = (
            f"https://webapi.alza.cz/api/catalog/v2/commodities/"
            f"{commodity_id}/reviews?country=cz&limit={limit}&offset={offset}"
        )
        params = {"limit": limit, "offset": offset}

        for attempt in range(max_retries):
            try:
                resp = session.get(url, impersonate="chrome120", timeout=10)
                body = resp.text.strip()

                if not body:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt + random.uniform(1, 3)
                        print(f"  Empty response, retrying in {wait:.1f}s...")
                        time.sleep(wait)
                        continue
                    else:
                        return

                data = json.loads(body)
                yield resp.status_code, data, params
                break

            except Exception as e:
                print(f"  ERROR: {e}")
                return

        # Decide whether to continue pagination
        total = data.get("paging", {}).get("size", 0)
        offset += limit
        if offset >= total:
            break


# ---------------------------------------------------------------------------
# Bronze persistence
# ---------------------------------------------------------------------------

def insert_bronze_row(cur, *, commodity_id, source_url, endpoint,
                      request_params, http_status, raw_response, batch_id):
    cur.execute(
        """
        INSERT INTO bronze_raw_responses
            (commodity_id, source_url, endpoint, request_params,
             http_status, raw_response, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """,
        (
            commodity_id,
            source_url,
            endpoint,
            json.dumps(request_params),
            http_status,
            json.dumps(raw_response) if raw_response is not None else None,
            batch_id,
        ),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Bronze batch: {batch_id}")

    with get_connection() as conn:
        ensure_bronze_table(conn)

        with conn.cursor() as cur:
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

    stats_count = 0
    reviews_page_count = 0

    with Session() as session:
        ucik, pgrik = get_tracking_params(session)
        print(f"Got ucik: {ucik} | pgrik: {pgrik}")

        for i, row in enumerate(rows, 1):
            url = row["url"]
            commodity_id = row["commodity_id"]
            print(f"[{i}/{len(rows)}] commodity {commodity_id} — {url}")

            with get_connection() as conn:
                with conn.cursor() as cur:

                    # --- reviewStats (one request) ---
                    status, raw = fetch_raw_review_stats(
                        session, commodity_id, ucik, pgrik
                    )
                    insert_bronze_row(
                        cur,
                        commodity_id=commodity_id,
                        source_url=url,
                        endpoint="reviewStats",
                        request_params={"ucik": ucik, "pgrik": pgrik},
                        http_status=status,
                        raw_response=raw,
                        batch_id=batch_id,
                    )
                    stats_count += 1
                    name = (raw or {}).get("name", "?")
                    print(f"  → stats saved ({name})")

                    # --- reviews (paginated) ---
                    page_n = 0
                    for status, raw, params in fetch_raw_reviews(
                        session, commodity_id
                    ):
                        insert_bronze_row(
                            cur,
                            commodity_id=commodity_id,
                            source_url=url,
                            endpoint="reviews",
                            request_params=params,
                            http_status=status,
                            raw_response=raw,
                            batch_id=batch_id,
                        )
                        page_n += 1
                        reviews_page_count += 1

                    print(f"  → {page_n} review pages saved")

                    # --- productDetail (one request) ---
                    status, raw = fetch_raw_product_detail(
                        session, commodity_id
                    )
                    insert_bronze_row(
                        cur,
                        commodity_id=commodity_id,
                        source_url=url,
                        endpoint="productDetail",
                        request_params={},
                        http_status=status,
                        raw_response=raw,
                        batch_id=batch_id,
                    )
                    detail_name = (raw or {}).get("data", {}).get("name", "?")
                    print(f"  → detail saved ({detail_name})")

                    # Mark processed
                    cur.execute(
                        "UPDATE url_queue SET processed = true WHERE url = %s;",
                        (url,),
                    )

                conn.commit()

            # Throttle
            time.sleep(random.uniform(4.5, 8.0))
            if random.random() < 0.15:
                pause = random.uniform(8.0, 20.0)
                print(f"  [human pause: {pause:.1f}s]")
                time.sleep(pause)

    print(f"\nDone. {stats_count} stat responses, "
          f"{reviews_page_count} review pages → bronze_raw_responses")


if __name__ == "__main__":
    main()