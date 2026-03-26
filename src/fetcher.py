#!/usr/bin/env python3
"""
Polymarket Data Fetcher
Pulls market data from the Gamma API + price history from CLOB API.
Stores everything in MySQL for backtesting.
"""

import requests
import json
import time
import logging
import sys
import os
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_setup import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "fetcher.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
HEADERS = {"User-Agent": "PolymarketFlowTrader/1.0"}
PAGE_SIZE = 100
REQUEST_DELAY = 0.3


def parse_price(price_str):
    if price_str is None:
        return None
    try:
        return Decimal(str(price_str))
    except Exception:
        return None


def parse_end_date(date_str):
    if not date_str:
        return None
    try:
        if date_str.endswith("Z"):
            date_str = date_str[:-1] + "+00:00"
        return datetime.fromisoformat(date_str)
    except Exception:
        return None


def store_market(conn, market_data):
    """Store a single market in MySQL, including clob_token_ids."""
    cursor = conn.cursor()
    condition_id = market_data.get("conditionId") or market_data.get("id", "")
    if not condition_id:
        return

    outcome_prices = market_data.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None

    clob_token_ids = market_data.get("clobTokenIds")
    if isinstance(clob_token_ids, str):
        try:
            clob_token_ids = json.loads(clob_token_ids)
        except Exception:
            clob_token_ids = None

    best_bid = parse_price(market_data.get("bestBid"))
    best_ask = parse_price(market_data.get("bestAsk"))
    spread = None
    if best_bid is not None and best_ask is not None:
        spread = best_ask - best_bid

    sql = """
        INSERT INTO markets
            (condition_id, question, slug, group_type, category,
             end_date, image, active, closed, volume, liquidity,
             outcome_prices, clob_token_ids, best_bid, best_ask, spread, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            volume = VALUES(volume),
            liquidity = VALUES(liquidity),
            outcome_prices = VALUES(outcome_prices),
            clob_token_ids = VALUES(clob_token_ids),
            best_bid = VALUES(best_bid),
            best_ask = VALUES(best_ask),
            spread = VALUES(spread),
            fetched_at = VALUES(fetched_at),
            active = VALUES(active),
            closed = VALUES(closed)
    """
    end_date = parse_end_date(market_data.get("endDate"))
    values = (
        condition_id,
        market_data.get("question", ""),
        market_data.get("slug", ""),
        market_data.get("groupItemTitle") or "market",
        market_data.get("category", ""),
        end_date,
        market_data.get("image", ""),
        market_data.get("active", True),
        market_data.get("closed", False),
        Decimal(str(market_data.get("volume", 0) or 0)),
        Decimal(str(market_data.get("liquidity", 0) or 0)),
        json.dumps(outcome_prices) if outcome_prices else None,
        json.dumps(clob_token_ids) if clob_token_ids else None,
        best_bid,
        best_ask,
        spread,
        datetime.now(),
    )
    cursor.execute(sql, values)
    cursor.close()


def store_outcome(conn, market_data):
    """Store market outcome if resolved."""
    cursor = conn.cursor()
    condition_id = market_data.get("conditionId") or market_data.get("id", "")
    if not condition_id:
        return

    resolved = market_data.get("closed", False)
    outcome_prices = market_data.get("outcomePrices")
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None

    winning_outcome = None
    resolution_price = None
    if resolved and outcome_prices and len(outcome_prices) >= 2:
        yes_price = parse_price(outcome_prices[0])
        no_price = parse_price(outcome_prices[1])
        if yes_price is not None and yes_price > Decimal("0.5"):
            winning_outcome = "YES"
            resolution_price = yes_price
        elif no_price is not None and no_price > Decimal("0.5"):
            winning_outcome = "NO"
            resolution_price = no_price

    sql = """
        INSERT INTO outcomes (condition_id, resolved, winning_outcome, resolved_at, resolution_price)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            resolved = VALUES(resolved),
            winning_outcome = VALUES(winning_outcome),
            resolved_at = VALUES(resolved_at),
            resolution_price = VALUES(resolution_price)
    """
    values = (
        condition_id,
        resolved,
        winning_outcome,
        datetime.now() if resolved else None,
        resolution_price,
    )
    cursor.execute(sql, values)
    cursor.close()


def fetch_gamma_markets(active=True, closed=False, limit=200, offset=0):
    """Fetch markets from Gamma API."""
    params = {
        "active": str(active).lower(),
        "closed": str(closed).lower(),
        "limit": limit,
        "offset": offset,
    }
    resp = requests.get(f"{GAMMA_URL}/markets", params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_price_history(token_id, interval="max", fidelity=None):
    """
    Fetch price history from CLOB API for a single token_id.
    Returns list of {t: unix_timestamp, p: price}.
    """
    params = {"market": token_id, "interval": interval}
    if fidelity:
        params["fidelity"] = fidelity
    try:
        resp = requests.get(f"{CLOB_URL}/prices-history", params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("history", [])
    except Exception as e:
        logger.debug(f"Price history failed for {token_id[:20]}...: {e}")
        return []


def store_price_history(conn, condition_id, token_id, history_points):
    """Store price history points in MySQL."""
    if not history_points:
        return 0
    cursor = conn.cursor()
    sql = """
        INSERT INTO price_history (condition_id, token_id, timestamp, price)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE price = VALUES(price)
    """
    batch = []
    for pt in history_points:
        ts = datetime.fromtimestamp(pt["t"])
        batch.append((condition_id, token_id, ts, Decimal(str(pt["p"]))))

    if batch:
        cursor.executemany(sql, batch)
    cursor.close()
    return len(batch)


def fetch_all_active():
    """Fetch all active markets and store in DB."""
    conn = get_connection("polymarket")
    total = 0
    offset = 0

    while True:
        logger.info(f"Fetching active markets offset={offset}...")
        try:
            markets = fetch_gamma_markets(active=True, closed=False, limit=PAGE_SIZE, offset=offset)
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            break

        if not markets:
            break

        for m in markets:
            try:
                store_market(conn, m)
                store_outcome(conn, m)
                total += 1
            except Exception as e:
                logger.error(f"Store failed for {m.get('conditionId', '?')}: {e}")

        conn.commit()
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    logger.info(f"✅ Stored {total} active markets.")
    conn.close()
    return total


def fetch_all_closed(limit=5000):
    """Fetch closed/resolved markets for backtesting ground truth."""
    conn = get_connection("polymarket")
    total = 0
    offset = 0

    while offset < limit:
        logger.info(f"Fetching closed markets offset={offset}...")
        try:
            markets = fetch_gamma_markets(active=False, closed=True, limit=PAGE_SIZE, offset=offset)
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            break

        if not markets:
            break

        for m in markets:
            try:
                store_market(conn, m)
                store_outcome(conn, m)
                total += 1
            except Exception as e:
                logger.error(f"Store failed for {m.get('conditionId', '?')}: {e}")

        conn.commit()
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    logger.info(f"✅ Stored {total} closed markets with outcomes.")
    conn.close()
    return total


def fetch_price_histories(min_volume=0, only_active=False, max_markets=None):
    """
    Fetch price history from CLOB API for markets that have clob_token_ids.
    Stores results in price_history table.
    """
    conn = get_connection("polymarket")
    cursor = conn.cursor(dictionary=True)

    # Get markets with token IDs
    sql = """
        SELECT condition_id, question, clob_token_ids, volume, active, closed, end_date
        FROM markets
        WHERE clob_token_ids IS NOT NULL
    """
    conditions = []
    params = []

    if min_volume > 0:
        conditions.append("volume >= %s")
        params.append(min_volume)

    if only_active:
        conditions.append("active = TRUE AND closed = FALSE")

    if conditions:
        sql += " AND " + " AND ".join(conditions)

    sql += " ORDER BY volume DESC"
    if max_markets:
        sql += f" LIMIT {max_markets}"

    cursor.execute(sql, params)
    markets = cursor.fetchall()
    logger.info(f"Found {len(markets)} markets with token IDs to fetch price history")

    total_points = 0
    fetched_markets = 0
    skipped_markets = 0

    for i, m in enumerate(markets):
        token_ids = json.loads(m["clob_token_ids"]) if isinstance(m["clob_token_ids"], str) else m["clob_token_ids"]
        if not token_ids or len(token_ids) < 1:
            skipped_markets += 1
            continue

        # Fetch price history for the YES token (index 0)
        yes_token = token_ids[0]
        history = fetch_price_history(yes_token, interval="max")

        if history:
            n = store_price_history(conn, m["condition_id"], yes_token, history)
            total_points += n
            fetched_markets += 1
            if fetched_markets % 50 == 0:
                conn.commit()
                logger.info(f"  Progress: {fetched_markets}/{len(markets)} markets, {total_points} points")
        else:
            skipped_markets += 1

        time.sleep(REQUEST_DELAY)

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"✅ Price history: {fetched_markets} markets, {total_points} total points, {skipped_markets} skipped")
    return fetched_markets, total_points


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "all"

    if action == "active":
        fetch_all_active()
    elif action == "closed":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
        fetch_all_closed(limit)
    elif action == "history":
        min_vol = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
        only_active = "--active" in sys.argv
        max_m = None
        for arg in sys.argv:
            if arg.startswith("--max="):
                max_m = int(arg.split("=")[1])
        fetch_price_histories(min_volume=min_vol, only_active=only_active, max_markets=max_m)
    elif action == "all":
        logger.info("=== Full fetch: active + closed + price history ===")
        n_active = fetch_all_active()
        n_closed = fetch_all_closed()
        n_hist, n_pts = fetch_price_histories(min_volume=10000)
        logger.info(f"=== Done: {n_active} active, {n_closed} closed, {n_hist} histories ({n_pts} pts) ===")
    else:
        print(f"Usage: {sys.argv[0]} [active|closed|history|all]")
        print("  history [--active] [--max=N] [min_volume]")
