#!/usr/bin/env python3
"""
Polymarket Flow Trading Backtester — REAL PRICE DATA
Strategy: Bet on markets about to end, high popularity, high win probability.

Uses actual CLOB price history for entry prices.
"""

import json
import math
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
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "backtest.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

DEFAULT_PARAMS = {
    "strategy_name": "flow_trade_real",
    "popularity_threshold": 10000,
    "hours_before_deadline": 48,
    "min_probability": 0.80,
    "bet_size": 100.0,
    "commission_rate": 0.0,
    "lookback_days": 3650,
}


def get_price_at_time(conn, condition_id, target_time):
    """
    Get the closest price to a target time from price_history.
    Returns the price just BEFORE target_time (our entry price).
    """
    cursor = conn.cursor(dictionary=True)

    # Get the last price before or at target_time
    sql = """
        SELECT price, timestamp
        FROM price_history
        WHERE condition_id = %s AND timestamp <= %s
        ORDER BY timestamp DESC
        LIMIT 1
    """
    cursor.execute(sql, (condition_id, target_time))
    row = cursor.fetchone()
    cursor.close()

    if row:
        return float(row["price"]), row["timestamp"]

    # Fallback: get the first available price after target_time
    cursor = conn.cursor(dictionary=True)
    sql2 = """
        SELECT price, timestamp
        FROM price_history
        WHERE condition_id = %s AND timestamp >= %s
        ORDER BY timestamp ASC
        LIMIT 1
    """
    cursor.execute(sql2, (condition_id, target_time))
    row = cursor.fetchone()
    cursor.close()

    if row:
        return float(row["price"]), row["timestamp"]

    return None, None


def get_market_price_at_deadline(conn, condition_id, deadline_hours):
    """
    Get the price at a specific time before market end.
    Returns (yes_price, price_timestamp) or (None, None).
    """
    cursor = conn.cursor(dictionary=True)

    # Get market end_date
    cursor.execute("SELECT end_date FROM markets WHERE condition_id = %s", (condition_id,))
    row = cursor.fetchone()
    cursor.close()

    if not row or not row["end_date"]:
        return None, None

    end_date = row["end_date"]
    entry_time = end_date - timedelta(hours=deadline_hours)

    return get_price_at_time(conn, condition_id, entry_time)


def run_backtest(params=None):
    """
    Run backtest using REAL price history data.

    Logic:
    1. Get all resolved markets with outcomes
    2. For each, look up the YES price at (end_date - hours_before_deadline)
    3. If price >= min_probability → bet YES
    4. If price <= (1 - min_probability) → bet NO
    5. Resolve against actual outcome
    """
    p = {**DEFAULT_PARAMS, **(params or {})}
    conn = get_connection("polymarket")
    cursor = conn.cursor(dictionary=True)

    cutoff_past = datetime.now() - timedelta(days=p["lookback_days"])

    # Get resolved markets that have price history
    sql = """
        SELECT m.condition_id, m.question, m.volume, m.end_date,
               o.winning_outcome, o.resolution_price,
               COUNT(ph.id) as price_points
        FROM markets m
        INNER JOIN outcomes o ON m.condition_id = o.condition_id
        INNER JOIN price_history ph ON m.condition_id = ph.condition_id
        WHERE o.resolved = TRUE
          AND o.winning_outcome IS NOT NULL
          AND m.end_date IS NOT NULL
          AND m.end_date >= %s
          AND m.end_date <= %s
          AND m.volume >= %s
        GROUP BY m.condition_id, m.question, m.volume, m.end_date,
                 o.winning_outcome, o.resolution_price
        HAVING price_points >= 5
        ORDER BY m.end_date DESC
    """
    cursor.execute(sql, (cutoff_past, datetime.now(), p["popularity_threshold"]))
    markets = cursor.fetchall()
    cursor.close()

    logger.info(f"Backtest: {len(markets)} resolved markets with price history")

    trades = []
    skipped_no_price = 0
    skipped_no_signal = 0

    for m in markets:
        condition_id = m["condition_id"]
        winning_outcome = m["winning_outcome"]
        is_yes_winner = (winning_outcome == "YES")

        # Get entry price at hours_before_deadline
        entry_price, price_ts = get_market_price_at_deadline(
            conn, condition_id, p["hours_before_deadline"]
        )

        if entry_price is None or entry_price <= 0.001 or entry_price >= 0.999:
            skipped_no_price += 1
            continue

        # Strategy decision
        bet_yes = entry_price >= p["min_probability"]
        bet_no = entry_price <= (1 - p["min_probability"])

        if not bet_yes and not bet_no:
            skipped_no_signal += 1
            continue

        # Calculate PnL
        if bet_yes:
            quantity = p["bet_size"] / entry_price
            if is_yes_winner:
                pnl = quantity * (1.0 - entry_price)
                result = "WIN"
            else:
                pnl = -quantity * entry_price
                result = "LOSS"
            side = "BUY_YES"
        else:
            no_price = 1.0 - entry_price
            quantity = p["bet_size"] / no_price
            if not is_yes_winner:
                pnl = quantity * (1.0 - no_price)
                result = "WIN"
            else:
                pnl = -quantity * no_price
                result = "LOSS"
            side = "BUY_NO"
            entry_price = no_price

        pnl -= quantity * entry_price * p["commission_rate"]

        trade = {
            "condition_id": condition_id,
            "question": m["question"],
            "side": side,
            "entry_price": round(entry_price, 6),
            "quantity": round(quantity, 4),
            "entry_time": price_ts,
            "exit_price": 1.0 if result == "WIN" else 0.0,
            "exit_time": m["end_date"],
            "pnl": round(pnl, 6),
            "result": result,
            "volume": float(m["volume"]),
        }
        trades.append(trade)

    conn.close()
    logger.info(f"Trades: {len(trades)} | No price: {skipped_no_price} | No signal: {skipped_no_signal}")

    return calculate_metrics(trades, p)


def calculate_metrics(trades, params):
    """Calculate performance metrics from trades."""
    if not trades:
        return {
            "strategy_name": params["strategy_name"],
            "params": params,
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0,
            "total_pnl": 0,
            "avg_pnl": 0,
            "roi_pct": 0,
            "max_drawdown": 0,
            "sharpe_ratio": 0,
            "sortino_ratio": 0,
            "profit_factor": 0,
            "trades": [],
        }

    wins = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    pnls = [t["pnl"] for t in trades]

    total_pnl = sum(pnls)
    win_rate = len(wins) / len(trades)
    avg_pnl = total_pnl / len(trades)

    # Equity curve & max drawdown
    cumulative = []
    running = 0
    for pnl in pnls:
        running += pnl
        cumulative.append(running)

    peak = cumulative[0]
    max_dd = 0
    for c in cumulative:
        if c > peak:
            peak = c
        dd = peak - c
        if dd > max_dd:
            max_dd = dd

    # Sharpe ratio (annualized)
    if len(pnls) > 1:
        mean_pnl = avg_pnl
        std_pnl = (sum((x - mean_pnl) ** 2 for x in pnls) / len(pnls)) ** 0.5
        sharpe = (mean_pnl / std_pnl * math.sqrt(365)) if std_pnl > 0 else 0
    else:
        sharpe = 0

    # Sortino ratio
    downside = [p for p in pnls if p < 0]
    if downside:
        downside_std = (sum(x ** 2 for x in downside) / len(downside)) ** 0.5
        sortino = (avg_pnl / downside_std * math.sqrt(365)) if downside_std > 0 else 0
    else:
        sortino = float("inf") if avg_pnl > 0 else 0

    # Profit factor
    gross_profit = sum(t["pnl"] for t in wins) if wins else 0
    gross_loss = abs(sum(t["pnl"] for t in losses)) if losses else 1
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # ROI
    total_invested = sum(t["entry_price"] * t["quantity"] for t in trades)
    roi = (total_pnl / total_invested * 100) if total_invested > 0 else 0

    return {
        "strategy_name": params["strategy_name"],
        "params": params,
        "total_trades": len(trades),
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "win_rate": round(win_rate, 4),
        "total_pnl": round(total_pnl, 6),
        "avg_pnl": round(avg_pnl, 6),
        "roi_pct": round(roi, 2),
        "max_drawdown": round(max_dd, 6),
        "sharpe_ratio": round(sharpe, 4),
        "sortino_ratio": round(sortino, 4) if sortino != float("inf") else 999.99,
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else 999.99,
        "trades": trades,
    }


def save_backtest(result):
    """Save backtest results to MySQL."""
    conn = get_connection("polymarket")
    cursor = conn.cursor()

    sql = """
        INSERT INTO backtest_runs
            (strategy_name, params, total_trades, winning_trades, losing_trades,
             win_rate, total_pnl, avg_pnl, max_drawdown, sharpe_ratio)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (
        result["strategy_name"],
        json.dumps(result["params"]),
        result["total_trades"],
        result["winning_trades"],
        result["losing_trades"],
        result["win_rate"],
        result["total_pnl"],
        result["avg_pnl"],
        result["max_drawdown"],
        result["sharpe_ratio"],
    ))

    backtest_id = cursor.lastrowid

    if result["trades"]:
        trade_sql = """
            INSERT INTO trades
                (backtest_id, condition_id, side, entry_price, quantity,
                 entry_time, exit_price, exit_time, pnl, result)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for t in result["trades"]:
            cursor.execute(trade_sql, (
                backtest_id,
                t["condition_id"],
                t["side"],
                t["entry_price"],
                t["quantity"],
                t.get("entry_time"),
                t["exit_price"],
                t.get("exit_time"),
                t["pnl"],
                t["result"],
            ))

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Backtest #{backtest_id} saved. {result['total_trades']} trades.")
    return backtest_id


if __name__ == "__main__":
    params = {}
    if "--json" in sys.argv:
        idx = sys.argv.index("--json")
        params = json.loads(sys.argv[idx + 1])

    result = run_backtest(params)

    print("\n" + "=" * 60)
    print("📊 BACKTEST RESULTS (REAL PRICE DATA)")
    print("=" * 60)
    print(f"  Strategy:       {result['strategy_name']}")
    print(f"  Total Trades:   {result['total_trades']}")
    print(f"  Wins:           {result['winning_trades']}")
    print(f"  Losses:         {result['losing_trades']}")
    print(f"  Win Rate:       {result['win_rate']:.2%}")
    print(f"  Total PnL:      ${result['total_pnl']:.2f}")
    print(f"  Avg PnL/Trade:  ${result['avg_pnl']:.2f}")
    print(f"  ROI:            {result['roi_pct']:.2f}%")
    print(f"  Max Drawdown:   ${result['max_drawdown']:.2f}")
    print(f"  Sharpe Ratio:   {result['sharpe_ratio']:.4f}")
    print(f"  Sortino Ratio:  {result.get('sortino_ratio', 0):.4f}")
    print(f"  Profit Factor:  {result.get('profit_factor', 0):.4f}")
    print("=" * 60)

    if result["total_trades"] > 0:
        save_backtest(result)
