# Polymarket Flow Trading Bot 🎯

Strategy: **Bet on markets about to end, high popularity, win probability > 80%.**

Low odds, high success rate. Edge comes from:
- Markets that are "priced in" near resolution
- High-volume markets with reliable price discovery
- Flow-based momentum as bettors pile into the obvious outcome

## Quick Start

```bash
cd ~/Desktop/polymarket
pip install -r requirements.txt

# 1. Set up database
python main.py setup

# 2. Fetch market data
python main.py fetch           # active + closed
python main.py fetch active    # just active
python main.py fetch closed    # just closed (for backtesting)

# 3. Run backtest
python main.py backtest

# 4. Parameter analysis (find optimal settings)
python main.py analyze --quick   # fast sweep
python main.py analyze           # full sweep (slow)

# 5. Check status
python main.py status
```

## Strategy Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `popularity_threshold` | 10,000 | Min volume ($) to qualify |
| `hours_before_deadline` | 48 | Max hours before market closes to enter |
| `min_probability` | 0.80 | Min YES price (= implied win probability) |
| `min_liquidity` | 5,000 | Min liquidity ($) |
| `bet_size` | 100 | Simulated $ per trade |
| `min_spread` | 0.02 | Skip if spread too tight |
| `max_spread` | 0.25 | Skip if spread too wide |

## Architecture

```
polymarket/
├── main.py              # CLI entry point
├── requirements.txt
├── README.md
├── src/
│   ├── db_setup.py      # MySQL schema
│   ├── fetcher.py       # Polymarket Gamma API data fetcher
│   ├── backtest.py      # Backtesting engine
│   └── analysis.py      # Parameter sweep & analysis
├── logs/                # Runtime logs
└── data/                # Exports / cache
```

## Database (MySQL)

- `markets` — raw market data from API
- `price_history` — historical price snapshots
- `outcomes` — resolved market outcomes (ground truth)
- `trades` — simulated/backtest trades
- `backtest_runs` — backtest run metadata
- `parameter_analysis` — parameter sweep results

## Parameter Analysis

The analyzer sweeps across:
- **Popularity threshold:** $1K → $100K
- **Hours before deadline:** 1h → 168h (1 week)
- **Min probability:** 60% → 95%

Output: ranked tables showing how each parameter affects return, win rate, Sharpe ratio, and drawdown.

## Data Source

Uses the Polymarket Gamma API (public, no auth required):
- `https://gamma-api.polymarket.com/events`
- `https://gamma-api.polymarket.com/markets`
