#!/usr/bin/env python3
"""
Periodic snapshot fetcher for active Polymarket markets.
Fetches current odds (yes/no price, bid/ask, volume, liquidity)
and stores every snapshot in active_market_snapshots table.

Designed to run every 5 minutes via cron.
"""

import requests
import json
import time
import logging
import sys
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from db_setup import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "snapshot.log")
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com"
HEADERS = {"User-Agent": "PolymarketFlowTrader/1.0"}
PAGE_SIZE = 500
REQUEST_DELAY = 0.2


def to_decimal(val):
    """Safely convert to Decimal, return None on failure."""
    if val is None or val == "" or val == "null":
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None


def to_datetime(date_str):
    """Parse ISO datetime string to Python datetime (timezone-aware)."""
    if not date_str:
        return None
    try:
        if date_str.endswith("Z"):
            date_str = date_str[:-1] + "+00:00"
        return datetime.fromisoformat(date_str)
    except Exception:
        return None


def extract_prices(market_data):
    """Extract YES and NO prices from outcomePrices JSON."""
    raw = market_data.get("outcomePrices")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None, None
    if not isinstance(raw, (list, tuple)) or len(raw) < 2:
        return None, None
    return to_decimal(raw[0]), to_decimal(raw[1])


def fetch_active_markets_page(offset=0):
    """Fetch one page of active (not closed) markets from Gamma API."""
    params = {
        "active": "true",
        "closed": "false",
        "limit": PAGE_SIZE,
        "offset": offset,
    }
    resp = requests.get(f"{GAMMA_URL}/markets", params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def store_snapshots(conn, snapshots):
    """Bulk insert snapshots into MySQL."""
    if not snapshots:
        return 0
    cursor = conn.cursor()
    sql = """
        INSERT INTO active_market_snapshots
            (condition_id, snapshot_time, question, slug, end_date,
             yes_price, no_price, best_bid, best_ask, spread,
             volume, liquidity, hours_to_close)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            yes_price = VALUES(yes_price),
            no_price = VALUES(no_price),
            best_bid = VALUES(best_bid),
            best_ask = VALUES(best_ask),
            spread = VALUES(spread),
            volume = VALUES(volume),
            liquidity = VALUES(liquidity),
            hours_to_close = VALUES(hours_to_close)
    """
    cursor.executemany(sql, snapshots)
    cursor.close()
    return len(snapshots)


def run_snapshot(max_pages=None):
    """
    Main snapshot routine:
    1. Paginate through ALL active markets
    2. Extract current prices, bid/ask, volume, liquidity
    3. Calculate hours_to_close
    4. Bulk insert into active_market_snapshots

    Args:
        max_pages: optional cap on pages. None = fetch all.
    """
    conn = get_connection("polymarket")
    offset = 0
    page_count = 0
    total = 0
    batch = []

    snapshot_time = datetime.now(timezone.utc)
    logger.info(f"Snapshot started at {snapshot_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    while True:
        try:
            markets = fetch_active_markets_page(offset)
        except Exception as e:
            logger.error(f"Fetch failed at offset={offset}: {e}")
            break

        if not markets:
            break

        for m in markets:
            condition_id = m.get("conditionId") or m.get("id", "")
            if not condition_id:
                continue

            yes_price, no_price = extract_prices(m)
            best_bid = to_decimal(m.get("bestBid"))
            best_ask = to_decimal(m.get("bestAsk"))
            spread = None
            if best_bid is not None and best_ask is not None:
                spread = best_ask - best_bid

            end_date = to_datetime(m.get("endDate"))
            hours_to_close = None
            if end_date:
                delta = end_date - snapshot_time
                hours_to_close = Decimal(str(round(delta.total_seconds() / 3600, 2)))

            batch.append((
                condition_id,
                snapshot_time,
                m.get("question", ""),
                m.get("slug", ""),
                end_date,
                yes_price,
                no_price,
                best_bid,
                best_ask,
                spread,
                to_decimal(m.get("volume", 0)) or Decimal("0"),
                to_decimal(m.get("liquidity", 0)) or Decimal("0"),
                hours_to_close,
            ))

        total += len(markets)
        offset += PAGE_SIZE
        page_count += 1

        if max_pages and page_count >= max_pages:
            logger.info(f"Reached max_pages={max_pages}, stopping")
            break

        time.sleep(REQUEST_DELAY)

    # Bulk insert
    if batch:
        n = store_snapshots(conn, batch)
        conn.commit()
        logger.info(f"Snapshot complete: {n} markets stored, {total} total fetched")
    else:
        logger.warning("No active markets found")

    conn.close()
    return total


if __name__ == "__main__":
    # Allow optional --max-pages=N arg from command line
    mp = None
    for arg in sys.argv[1:]:
        if arg.startswith("--max-pages="):
            mp = int(arg.split("=")[1])
    run_snapshot(max_pages=mp)
