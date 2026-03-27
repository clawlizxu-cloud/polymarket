#!/usr/bin/env python3
"""
Periodic snapshot fetcher for active Polymarket markets.
Fetches current odds (yes/no price, bid/ask, volume, liquidity)
and stores every snapshot in active_market_snapshots table.

Designed to run every 10 minutes via cron.
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


CRYPTO_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency",
    "solana", "sol", "xrp", "ripple", "dogecoin", "doge", "bnb",
    "cardano", "ada", "polygon", "matic", "avalanche", "avax",
    "chainlink", "link", "litecoin", "ltc", "polkadot", "dot",
    "shiba", "pepe", "memecoin", "defi", "nft", "blockchain",
    "token", "binance", "coinbase", "crypto.com", "stablecoin",
    "usdt", "usdc", "tether",
]

CRYPTO_TAGS = {"crypto", "cryptocurrency", "bitcoin", "ethereum"}


def is_crypto_market(market_data):
    """Detect if a market is crypto-related based on question text and tags."""
    question = (market_data.get("question") or "").lower()
    slug = (market_data.get("slug") or "").lower()
    text = question + " " + slug

    # Check tags
    tags_raw = market_data.get("tags") or market_data.get("tag") or []
    if isinstance(tags_raw, str):
        try:
            tags_raw = json.loads(tags_raw)
        except Exception:
            tags_raw = []
    tags_lower = {str(t).lower() for t in tags_raw}
    if tags_lower & CRYPTO_TAGS:
        return True

    # Check question/slug text
    for kw in CRYPTO_KEYWORDS:
        if kw in text:
            return True

    return False


def run_snapshot(max_pages=None, max_hours=72, min_hours=-1, min_volume=5000):
    """
    Main snapshot routine:
    1. Paginate through ALL active markets
    2. Filter: min_hours <= hours_to_close <= max_hours AND volume >= min_volume AND not crypto
    3. Extract current prices, bid/ask, volume, liquidity
    4. Bulk insert into active_market_snapshots

    Args:
        max_pages: optional cap on pages. None = fetch all.
        max_hours: only keep markets closing within this many hours (default 72 = 3 days).
        min_hours: allow markets up to this many hours past deadline (default -1 = allow 1h past).
        min_volume: only keep markets with volume >= this (default 5000).
    """
    conn = get_connection("polymarket")
    offset = 0
    page_count = 0
    total = 0
    crypto_skipped = 0
    batch = []

    snapshot_time = datetime.now(timezone.utc)
    logger.info(f"Snapshot started at {snapshot_time.strftime('%Y-%m-%d %H:%M:%S')} UTC | filter: {min_hours}h<=hours<={max_hours}h, vol>={min_volume}, no_crypto")

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

            # Filter: skip crypto markets
            if is_crypto_market(m):
                crypto_skipped += 1
                continue

            # Filter: skip markets outside deadline or below volume threshold
            market_volume = to_decimal(m.get("volume", 0)) or Decimal("0")
            if max_hours is not None and (hours_to_close is None or hours_to_close > max_hours):
                continue
            if min_hours is not None and hours_to_close is not None and hours_to_close < min_hours:
                continue
            if min_volume is not None and market_volume < min_volume:
                continue

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
                market_volume,
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
        logger.info(f"Snapshot complete: {n} markets stored (after filters), {total} total fetched, {crypto_skipped} crypto excluded")
    else:
        logger.warning(f"No active markets found after filters | {total} fetched | {crypto_skipped} crypto excluded")

    conn.close()
    # Check for big price movers vs previous snapshot
    try:
        from alert import detect_big_movers, format_alerts
        movers = detect_big_movers()
        alert_text = format_alerts(movers)
        if alert_text:
            logger.info(f"\n{alert_text}")
            # Write alert to a file so cron job can pick it up
            alert_file = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "latest_alert.txt"
            )
            with open(alert_file, "w") as f:
                f.write(alert_text)
    except Exception as e:
        logger.warning(f"Alert check failed: {e}")

    return total


if __name__ == "__main__":
    # Allow optional --max-pages=N arg from command line
    mp = None
    for arg in sys.argv[1:]:
        if arg.startswith("--max-pages="):
            mp = int(arg.split("=")[1])
    run_snapshot(max_pages=mp)
