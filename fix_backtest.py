#!/usr/bin/env python3
"""
Fix backtest pipeline:
1. Re-fetch closed markets to get clob_token_ids (Gamma API)
2. Fetch CLOB price history for resolved markets
3. Run backtest on real data
"""

import sys
import os
import json
import time
import logging
from datetime import datetime, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
from db_setup import get_connection
from fetcher import (
    fetch_gamma_markets, store_market, store_outcome,
    fetch_price_history, store_price_history,
    GAMMA_URL, CLOB_URL, HEADERS, PAGE_SIZE, REQUEST_DELAY
)
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "logs", "fix.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def step1_refetch_closed_markets():
    """Re-fetch closed markets to get clob_token_ids."""
    conn = get_connection("polymarket")
    total = 0
    offset = 0
    max_offset = 10000

    while offset < max_offset:
        logger.info(f"Re-fetching closed markets offset={offset}...")
        try:
            markets = fetch_gamma_markets(active=False, closed=True, limit=PAGE_SIZE, offset=offset)
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            time.sleep(5)
            continue

        if not markets:
            break

        for m in markets:
            try:
                store_market(conn, m)
                store_outcome(conn, m)
                total += 1
            except Exception as e:
                logger.error(f"Store failed: {e}")

        conn.commit()
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    # Also re-fetch active+closed (markets that are both)
    offset = 0
    while offset < 5000:
        logger.info(f"Re-fetching active+closed offset={offset}...")
        try:
            markets = fetch_gamma_markets(active=True, closed=True, limit=PAGE_SIZE, offset=offset)
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            time.sleep(5)
            continue

        if not markets:
            break

        for m in markets:
            try:
                store_market(conn, m)
                store_outcome(conn, m)
                total += 1
            except Exception as e:
                logger.error(f"Store failed: {e}")

        conn.commit()
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    conn.close()
    logger.info(f"✅ Re-fetched {total} closed markets")
    return total


def step2_check_tokens():
    """Check how many resolved markets now have tokens."""
    conn = get_connection("polymarket")
    c = conn.cursor()

    c.execute("""
        SELECT COUNT(*) FROM markets m
        JOIN outcomes o ON m.condition_id = o.condition_id
        WHERE o.winning_outcome IS NOT NULL AND o.winning_outcome != ''
        AND m.clob_token_ids IS NOT NULL AND m.clob_token_ids != 'null' AND m.clob_token_ids != '[]'
    """)
    resolved_with_tokens = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM outcomes WHERE winning_outcome IS NOT NULL AND winning_outcome != ''")
    total_resolved = c.fetchone()[0]

    c.execute("SELECT COUNT(DISTINCT condition_id) FROM price_history")
    with_history = c.fetchone()[0]

    conn.close()
    logger.info(f"Resolved: {total_resolved} | With tokens: {resolved_with_tokens} | With history: {with_history}")
    return resolved_with_tokens


def step3_fetch_price_history(min_volume=10000, max_markets=None):
    """
    Fetch price history for resolved markets that have tokens.
    Prioritize high-volume markets.
    """
    conn = get_connection("polymarket")
    cursor = conn.cursor(dictionary=True)

    sql = """
        SELECT m.condition_id, m.question, m.clob_token_ids, m.volume, o.winning_outcome
        FROM markets m
        INNER JOIN outcomes o ON m.condition_id = o.condition_id
        WHERE o.winning_outcome IS NOT NULL AND o.winning_outcome != ''
        AND m.clob_token_ids IS NOT NULL AND m.clob_token_ids != 'null' AND m.clob_token_ids != '[]'
        AND m.volume >= %s
        AND m.condition_id NOT IN (SELECT DISTINCT condition_id FROM price_history)
        ORDER BY m.volume DESC
    """
    if max_markets:
        sql += f" LIMIT {max_markets}"

    cursor.execute(sql, (min_volume,))
    markets = cursor.fetchall()
    logger.info(f"Found {len(markets)} resolved markets with tokens to fetch history")

    total_points = 0
    fetched = 0
    skipped = 0
    errors = 0

    for i, m in enumerate(markets):
        token_ids = json.loads(m["clob_token_ids"]) if isinstance(m["clob_token_ids"], str) else m["clob_token_ids"]
        if not token_ids or len(token_ids) < 1:
            skipped += 1
            continue

        yes_token = token_ids[0]
        try:
            history = fetch_price_history(yes_token, interval="max")
        except Exception as e:
            logger.debug(f"Error fetching {m['condition_id'][:20]}: {e}")
            errors += 1
            time.sleep(2)
            continue

        if history:
            n = store_price_history(conn, m["condition_id"], yes_token, history)
            total_points += n
            fetched += 1
            if fetched % 100 == 0:
                conn.commit()
                logger.info(f"  Progress: {fetched}/{len(markets)} markets, {total_points} points, {skipped} skipped, {errors} errors")
        else:
            skipped += 1

        time.sleep(REQUEST_DELAY)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"✅ History: {fetched} markets, {total_points} points, {skipped} skipped, {errors} errors")
    return fetched, total_points


def step4_verify():
    """Check final data state for backtesting."""
    conn = get_connection("polymarket")
    c = conn.cursor()

    c.execute("""
        SELECT COUNT(DISTINCT ph.condition_id)
        FROM price_history ph
        JOIN outcomes o ON ph.condition_id = o.condition_id
        WHERE o.winning_outcome IS NOT NULL AND o.winning_outcome != ''
    """)
    resolved_with_history = c.fetchone()[0]

    c.execute("""
        SELECT COUNT(*) FROM price_history ph
        JOIN outcomes o ON ph.condition_id = o.condition_id
        WHERE o.winning_outcome IS NOT NULL AND o.winning_outcome != ''
    """)
    total_points = c.fetchone()[0]

    conn.close()
    logger.info(f"✅ Resolved markets with price_history: {resolved_with_history}")
    logger.info(f"   Total price points for resolved: {total_points}")
    return resolved_with_history


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "all"

    if action == "refetch":
        step1_refetch_closed_markets()
        step2_check_tokens()
    elif action == "history":
        min_vol = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
        max_m = int(sys.argv[3]) if len(sys.argv) > 3 else None
        step3_fetch_price_history(min_volume=min_vol, max_markets=max_m)
    elif action == "verify":
        step4_verify()
    elif action == "all":
        logger.info("=== Step 1: Re-fetch closed markets ===")
        step1_refetch_closed_markets()

        logger.info("=== Step 2: Check token coverage ===")
        step2_check_tokens()

        logger.info("=== Step 3: Fetch price history for resolved markets ===")
        step3_fetch_price_history(min_volume=1000, max_markets=2000)

        logger.info("=== Step 4: Verify ===")
        step4_verify()

        logger.info("=== Done! Ready for backtest. ===")
    else:
        print(f"Usage: {__file__} [refetch|history|verify|all]")
