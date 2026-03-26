#!/usr/bin/env python3
"""
Parameter Analysis for Flow Trading Strategy (REAL PRICES)
Sweeps across popularity threshold, time to deadline, and odds
to find optimal parameter combinations.
"""

import json
import itertools
import logging
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_setup import get_connection
from backtest import run_backtest, save_backtest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "analysis.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

QUICK_GRID = {
    "popularity_threshold": [5000, 10000, 50000],
    "hours_before_deadline": [6, 24, 48],
    "min_probability": [0.70, 0.80, 0.90],
}

FULL_GRID = {
    "popularity_threshold": [1000, 5000, 10000, 25000, 50000, 100000],
    "hours_before_deadline": [1, 3, 6, 12, 24, 48, 72, 168],
    "min_probability": [0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95],
}

# Alias for backward compat
PARAM_GRID = FULL_GRID


def run_parameter_sweep(grid=None):
    """Run backtest for every combination in the parameter grid."""
    if grid is None:
        grid = FULL_GRID

    combos = list(itertools.product(
        grid["popularity_threshold"],
        grid["hours_before_deadline"],
        grid["min_probability"],
    ))
    total = len(combos)
    logger.info(f"Starting parameter sweep: {total} combinations (REAL prices)")

    results = []

    for i, (pop_thresh, hours, min_prob) in enumerate(combos):
        logger.info(f"[{i+1}/{total}] pop={pop_thresh}, hours={hours}, prob={min_prob}")

        params = {
            "popularity_threshold": pop_thresh,
            "hours_before_deadline": hours,
            "min_probability": min_prob,
        }

        try:
            result = run_backtest(params)
            results.append({
                "pop_threshold": pop_thresh,
                "hours_before": hours,
                "min_probability": min_prob,
                "win_rate": result["win_rate"],
                "total_return": result["total_pnl"],
                "sharpe": result["sharpe_ratio"],
                "max_drawdown": result["max_drawdown"],
                "num_trades": result["total_trades"],
                "roi_pct": result.get("roi_pct", 0),
                "sortino": result.get("sortino_ratio", 0),
                "profit_factor": result.get("profit_factor", 0),
            })
        except Exception as e:
            logger.error(f"Failed: {e}")

    save_analysis_results(results)
    print_analysis(results)
    return results


def save_analysis_results(results):
    """Save sweep results to parameter_analysis table."""
    conn = get_connection("polymarket")
    cursor = conn.cursor()

    for r in results:
        sql = """
            INSERT INTO parameter_analysis
                (pop_threshold, hours_before, min_probability,
                 win_rate, total_return, sharpe, max_drawdown, num_trades)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            r["pop_threshold"],
            r["hours_before"],
            r["min_probability"],
            r["win_rate"],
            r["total_return"],
            r["sharpe"],
            r["max_drawdown"],
            r["num_trades"],
        ))

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Saved {len(results)} analysis results to DB.")


def print_analysis(results):
    """Print formatted analysis results."""
    if not results:
        print("No results to display.")
        return

    by_sharpe = sorted(results, key=lambda x: x["sharpe"], reverse=True)
    by_return = sorted(results, key=lambda x: x["total_return"], reverse=True)
    by_winrate = sorted(
        [r for r in results if r["num_trades"] >= 5],
        key=lambda x: x["win_rate"],
        reverse=True,
    )

    print("\n" + "=" * 80)
    print("📈 PARAMETER ANALYSIS (REAL PRICE DATA)")
    print("=" * 80)

    print("\n🏆 TOP 10 BY SHARPE RATIO (Risk-Adjusted)")
    print("-" * 80)
    print(f"{'PopThresh':>10} {'Hours':>6} {'Prob':>6} {'Trades':>7} "
          f"{'WinRate':>8} {'Return':>10} {'Sharpe':>8} {'MaxDD':>10} {'ROI%':>7}")
    print("-" * 80)
    for r in by_sharpe[:10]:
        print(f"{r['pop_threshold']:>10} {r['hours_before']:>6} {r['min_probability']:>6.2f} "
              f"{r['num_trades']:>7} {r['win_rate']:>8.2%} "
              f"${r['total_return']:>9.2f} {r['sharpe']:>8.4f} "
              f"${r['max_drawdown']:>9.2f} {r['roi_pct']:>6.1f}%")

    print(f"\n💰 TOP 10 BY TOTAL RETURN")
    print("-" * 80)
    for r in by_return[:10]:
        print(f"{r['pop_threshold']:>10} {r['hours_before']:>6} {r['min_probability']:>6.2f} "
              f"{r['num_trades']:>7} {r['win_rate']:>8.2%} "
              f"${r['total_return']:>9.2f} {r['sharpe']:>8.4f} "
              f"${r['max_drawdown']:>9.2f} {r['roi_pct']:>6.1f}%")

    if by_winrate:
        print(f"\n🎯 TOP 10 BY WIN RATE (min 5 trades)")
        print("-" * 80)
        for r in by_winrate[:10]:
            print(f"{r['pop_threshold']:>10} {r['hours_before']:>6} {r['min_probability']:>6.2f} "
                  f"{r['num_trades']:>7} {r['win_rate']:>8.2%} "
                  f"${r['total_return']:>9.2f} {r['sharpe']:>8.4f} "
                  f"${r['max_drawdown']:>9.2f} {r['roi_pct']:>6.1f}%")

    # Parameter impact
    print(f"\n📊 PARAMETER IMPACT ANALYSIS")
    print("=" * 60)

    print("\n1️⃣  Popularity Threshold Impact:")
    pop_groups = {}
    for r in results:
        pop_groups.setdefault(r["pop_threshold"], []).append(r)
    for pop in sorted(pop_groups.keys()):
        g = pop_groups[pop]
        avg_ret = sum(r["total_return"] for r in g) / len(g)
        avg_win = sum(r["win_rate"] for r in g) / len(g)
        avg_trades = sum(r["num_trades"] for r in g) / len(g)
        print(f"   ${pop:>7}: Avg Return=${avg_ret:>8.2f}  "
              f"Avg WinRate={avg_win:.2%}  Avg Trades={avg_trades:.0f}")

    print("\n2️⃣  Hours Before Deadline Impact:")
    hour_groups = {}
    for r in results:
        hour_groups.setdefault(r["hours_before"], []).append(r)
    for h in sorted(hour_groups.keys()):
        g = hour_groups[h]
        avg_ret = sum(r["total_return"] for r in g) / len(g)
        avg_win = sum(r["win_rate"] for r in g) / len(g)
        avg_trades = sum(r["num_trades"] for r in g) / len(g)
        print(f"   {h:>4}h: Avg Return=${avg_ret:>8.2f}  "
              f"Avg WinRate={avg_win:.2%}  Avg Trades={avg_trades:.0f}")

    print("\n3️⃣  Min Probability Impact:")
    prob_groups = {}
    for r in results:
        prob_groups.setdefault(r["min_probability"], []).append(r)
    for p in sorted(prob_groups.keys()):
        g = prob_groups[p]
        avg_ret = sum(r["total_return"] for r in g) / len(g)
        avg_win = sum(r["win_rate"] for r in g) / len(g)
        avg_trades = sum(r["num_trades"] for r in g) / len(g)
        print(f"   {p:.0%}: Avg Return=${avg_ret:>8.2f}  "
              f"Avg WinRate={avg_win:.2%}  Avg Trades={avg_trades:.0f}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    quick = "--quick" in sys.argv
    if quick:
        run_parameter_sweep(QUICK_GRID)
    else:
        run_parameter_sweep(FULL_GRID)
