#!/usr/bin/env python3
"""
Price change alert: detect markets where YES price moved >5%
between two consecutive snapshots.
"""

import json
import logging
import os
from decimal import Decimal

from db_setup import get_connection

logger = logging.getLogger(__name__)

# Absolute threshold for YES price change (e.g. 0.35 -> 0.40 = 5%)
PRICE_CHANGE_THRESHOLD = Decimal("0.25")

# Minimum volume for alert (skip illiquid markets where 1 trade moves 10%)
MIN_ALERT_VOLUME = 1000

# Sports keywords — exclude traditional sports + esports from alerts
SPORTS_KEYWORDS = [
    # Traditional sports
    "nfl", "nba", "mlb", "nhl", "soccer", "football", "basketball",
    "baseball", "hockey", "tennis", "ufc", "mma", "boxing", "cricket",
    "rugby", "golf", "f1", "formula 1", "premier league", "la liga",
    "bundesliga", "serie a", "ligue 1", "champions league", "world cup",
    "euros", "copa america", "super bowl", "grand slam", "wimbledon",
    "masters", "open championship", "playoffs", "finals", "semifinal",
    "quarterfinal", "match", "game", "vs", "versus",
    # Teams / leagues (common patterns)
    "devils", "predators", "lakers", "celtics", "yankees", "patriots",
    "manchester", "barcelona", "real madrid", "bayern", "psg", "liverpool",
    "arsenal", "chelsea", "tottenham", "juventus", "inter milan",
    # Esports
    "valorant", "cs2", "csgo", "cs:", "dota", "league of legends", "lol",
    "overwatch", "call of duty", "cod", "fortnite", "apex legends",
    "starcraft", "rocket league", "rainbow six", "pubg", "mobile legends",
    "counter-strike", "esport", "e-sport", "map 1", "map 2", "map 3",
    # Common match phrases
    "who will win", "first half", "second half", "half time",
    "overtime", "penalty", "red card", "yellow card", "goal",
    "spread:", "point spread", "moneyline", "over/under",
    "odds", "line ", "lines", "parlay",
]

SPORTS_TAGS = {"sports", "football", "basketball", "baseball", "hockey", "soccer", "esports", "ufc", "boxing"}

# Weather / temperature keywords — exclude from alerts
WEATHER_KEYWORDS = [
    "°c", "°f", "temperature", "weather",
]


def is_weather_market(question: str) -> bool:
    """Detect if a market question is weather/temperature-related."""
    q = (question or "").lower()
    for kw in WEATHER_KEYWORDS:
        if kw in q:
            return True
    return False


def is_sports_market(question: str) -> bool:
    """Detect if a market question is sports-related."""
    q = (question or "").lower()
    for kw in SPORTS_KEYWORDS:
        if kw in q:
            return True
    return False


def load_dedup_state():
    """Load recently-alerted condition_ids with their last-seen prices from state file."""
    state_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "alert_dedup.json"
    )
    try:
        with open(state_file) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_dedup_state(state):
    """Save dedup state — keep last-seen prices for each market."""
    state_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "alert_dedup.json"
    )
    with open(state_file, "w") as f:
        json.dump(state, f)


def detect_big_movers(threshold=None):
    """
    Compare the latest two snapshots. For each market present in both,
    check if |yes_price_new - yes_price_old| >= threshold.

    Returns list of dicts with market info and price change.
    """
    if threshold is None:
        threshold = PRICE_CHANGE_THRESHOLD
    else:
        threshold = Decimal(str(threshold))

    conn = get_connection("polymarket")
    cursor = conn.cursor(dictionary=True)

    # Get the two most recent snapshot times
    cursor.execute("""
        SELECT DISTINCT snapshot_time
        FROM active_market_snapshots
        ORDER BY snapshot_time DESC
        LIMIT 2
    """)
    rows = cursor.fetchall()

    if len(rows) < 2:
        logger.info("Not enough snapshots to compare (need at least 2)")
        cursor.close()
        conn.close()
        return []

    t_new = rows[0]["snapshot_time"]
    t_old = rows[1]["snapshot_time"]

    # Load dedup state
    dedup = load_dedup_state()

    # Join latest with previous snapshot on condition_id
    cursor.execute("""
        SELECT
            n.condition_id,
            n.question,
            n.yes_price AS new_yes,
            n.no_price  AS new_no,
            n.volume    AS new_volume,
            n.hours_to_close,
            o.yes_price AS old_yes,
            o.no_price  AS old_no
        FROM active_market_snapshots n
        JOIN active_market_snapshots o
            ON n.condition_id = o.condition_id
        WHERE n.snapshot_time = %s
          AND o.snapshot_time = %s
          AND n.yes_price IS NOT NULL
          AND o.yes_price IS NOT NULL
    """, (t_new, t_old))

    movers = []
    for row in cursor.fetchall():
        # Skip sports-related markets
        if is_sports_market(row["question"]):
            continue

        # Skip weather/temperature markets
        if is_weather_market(row["question"]):
            continue

        new_yes = row["new_yes"]
        old_yes = row["old_yes"]
        change = new_yes - old_yes
        abs_change = abs(change)

        # Skip low-volume noise
        volume = float(row["new_volume"]) if row["new_volume"] else 0
        if volume < MIN_ALERT_VOLUME:
            continue

        # Skip recently-alerted markets
        cid = row["condition_id"]
        if cid in dedup:
            continue

        if abs_change >= threshold:
            # Skip if price hasn't changed since last alert (no new movement)
            last_alerted_price = dedup.get(cid)
            if last_alerted_price is not None and float(new_yes) == last_alerted_price:
                continue

            direction = "📈" if change > 0 else "📉"
            movers.append({
                "question": row["question"],
                "condition_id": cid,
                "old_yes": float(old_yes),
                "new_yes": float(new_yes),
                "change": float(change),
                "abs_change": float(abs_change),
                "direction": direction,
                "volume": volume,
                "hours_to_close": float(row["hours_to_close"]) if row["hours_to_close"] else None,
            })

    # Sort by absolute change descending
    movers.sort(key=lambda x: x["abs_change"], reverse=True)

    # Update dedup state: store current YES price for each alerted market
    for m in movers:
        dedup[m["condition_id"]] = m["new_yes"]
    save_dedup_state(dedup)

    cursor.close()
    conn.close()

    logger.info(f"Checked {t_old} → {t_new}: found {len(movers)} big movers (threshold={threshold})")
    return movers


def format_time_left(hours_to_close):
    """Format hours remaining into a human-readable string."""
    if hours_to_close is None:
        return "⏰ 截止时间未知"
    total_min = int(hours_to_close * 60)
    if total_min < 60:
        return f"⏰ 剩余 {total_min} 分钟"
    h = int(hours_to_close)
    m = total_min - h * 60
    if m > 0:
        return f"⏰ 剩余 {h}h {m}m"
    return f"⏰ 剩余 {h}h"


def format_alerts(movers):
    """Format movers into a Telegram-ready string. Self-contained — no summarization needed."""
    if not movers:
        return ""

    lines = [f"🚨 {len(movers)} markets moved ≥25%:\n"]
    for i, m in enumerate(movers, 1):
        time_left = format_time_left(m['hours_to_close'])
        vol = f"${m['volume']:,.0f}"
        sign = "+" if m['change'] > 0 else ""
        lines.append(
            f"{i}. {m['direction']} {m['question']}\n"
            f"   YES {m['old_yes']:.1%}→{m['new_yes']:.1%} ({sign}{m['change']:.1%}) | {vol} | {time_left}"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    movers = detect_big_movers()
    alert = format_alerts(movers)
    if alert:
        print(alert)
    else:
        print("No big movers detected.")
