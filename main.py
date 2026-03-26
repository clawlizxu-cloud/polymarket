#!/usr/bin/env python3
"""
Polymarket Flow Trading — Main Entry Point

Usage:
    python main.py setup                # Initialize database
    python main.py fetch                # Fetch active + closed markets + price history
    python main.py fetch active         # Fetch active markets only
    python main.py fetch closed         # Fetch closed markets (backtest ground truth)
    python main.py fetch history        # Fetch price history from CLOB API
    python main.py backtest             # Run single backtest (real prices)
    python main.py backtest --json '{"min_probability": 0.85}'
    python main.py analyze              # Full parameter sweep
    python main.py analyze --quick      # Quick parameter sweep
    python main.py snapshot              # One-shot snapshot of active market odds
    python main.py status               # Show DB stats
"""

import sys
import os
import json

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from db_setup import setup_database, get_connection


def cmd_setup():
    setup_database()


def cmd_fetch(args):
    from fetcher import fetch_all_active, fetch_all_closed, fetch_price_histories
    if not args:
        n_active = fetch_all_active()
        n_closed = fetch_all_closed()
        n_hist, n_pts = fetch_price_histories(min_volume=10000)
        print(f"\nDone: {n_active} active + {n_closed} closed + {n_hist} histories ({n_pts} pts).")
    elif args[0] == "active":
        fetch_all_active()
    elif args[0] == "closed":
        limit = int(args[1]) if len(args) > 1 else 5000
        fetch_all_closed(limit)
    elif args[0] == "history":
        min_vol = int(args[1]) if len(args) > 1 else 10000
        only_active = "--active" in args
        max_m = None
        for arg in args:
            if arg.startswith("--max="):
                max_m = int(arg.split("=")[1])
        n_m, n_pts = fetch_price_histories(min_volume=min_vol, only_active=only_active, max_markets=max_m)
        print(f"\nDone: {n_m} markets, {n_pts} price points.")
    else:
        print(f"Unknown: {args[0]}. Use: active | closed | history")


def cmd_backtest(args):
    from backtest import run_backtest, save_backtest
    params = {}
    if "--json" in args:
        idx = args.index("--json")
        params = json.loads(args[idx + 1])

    print(f"Running backtest (REAL prices) with params: {params or 'defaults'}")
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
    print(f"  ROI:            {result.get('roi_pct', 0):.2f}%")
    print(f"  Max Drawdown:   ${result['max_drawdown']:.2f}")
    print(f"  Sharpe Ratio:   {result['sharpe_ratio']:.4f}")
    print(f"  Sortino Ratio:  {result.get('sortino_ratio', 0):.4f}")
    print(f"  Profit Factor:  {result.get('profit_factor', 0):.4f}")
    print("=" * 60)

    if result["total_trades"] > 0:
        save_backtest(result)
    else:
        print("\n⚠️  No qualifying trades. Need price history data first.")
        print("  Run: python main.py fetch history")


def cmd_analyze(args):
    from analysis import run_parameter_sweep, QUICK_GRID, FULL_GRID
    if "--quick" in args:
        run_parameter_sweep(QUICK_GRID)
    else:
        run_parameter_sweep(FULL_GRID)


def cmd_status():
    conn = get_connection("polymarket")
    cursor = conn.cursor()

    tables = ["markets", "price_history", "outcomes", "trades", "backtest_runs", "parameter_analysis"]
    print("\n📦 Database Status")
    print("=" * 50)
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:>25}: {count:>8,} rows")

    # Markets with token IDs
    cursor.execute("SELECT COUNT(*) FROM markets WHERE clob_token_ids IS NOT NULL")
    with_tokens = cursor.fetchone()[0]
    print(f"\n  Markets with token IDs: {with_tokens:,}")

    # Price history stats
    cursor.execute("""
        SELECT COUNT(DISTINCT condition_id), MIN(timestamp), MAX(timestamp), SUM(price) 
        FROM price_history
    """)
    row = cursor.fetchone()
    if row and row[0]:
        print(f"  Markets with price history: {row[0]:,}")
        print(f"  Price data range: {row[1]} → {row[2]}")

    # Recent backtests
    cursor.execute("""
        SELECT id, strategy_name, total_trades, win_rate, total_pnl, created_at
        FROM backtest_runs ORDER BY created_at DESC LIMIT 5
    """)
    backtests = cursor.fetchall()
    if backtests:
        print("\n  Recent Backtests:")
        for b in backtests:
            print(f"    #{b[0]} {b[1]} | {b[2]} trades | "
                  f"win={float(b[3]):.1%} | PnL=${float(b[4]):.2f} | {b[5]}")

    cursor.close()
    conn.close()


def getarg(name, default=None):
    """Extract --name=value from sys.argv."""
    prefix = f"--{name}="
    for a in sys.argv:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    args = sys.argv[2:]

    if cmd == "setup":
        cmd_setup()
    elif cmd == "fetch":
        cmd_fetch(args)
    elif cmd == "backtest":
        cmd_backtest(args)
    elif cmd == "analyze":
        cmd_analyze(args)
    elif cmd == "status":
        cmd_status()
    elif cmd == "snapshot":
        from snapshot import run_snapshot
        run_snapshot(
            max_hours=float(getarg("max-hours", "72")),
            min_hours=float(getarg("min-hours", "-1")),
            min_volume=float(getarg("min-vol", "5000")),
        )
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
