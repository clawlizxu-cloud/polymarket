#!/usr/bin/env python3
"""
MySQL Database Setup for Polymarket Flow Trading
Creates tables for markets, trades, backtests, and parameter analysis.
"""

import mysql.connector
from mysql.connector import Error
import os

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "big_smart",
    "password": "big_smart",
    "database": "polymarket",
}

SCHEMA_SQL = """
-- Raw market data from Polymarket Gamma API
CREATE TABLE IF NOT EXISTS markets (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    condition_id    VARCHAR(128) NOT NULL UNIQUE,
    question        TEXT NOT NULL,
    slug            VARCHAR(512),
    group_type      VARCHAR(256),          -- 'event' or 'market'
    category        VARCHAR(128),
    end_date        DATETIME NOT NULL,
    image           TEXT,
    active          BOOLEAN DEFAULT TRUE,
    closed          BOOLEAN DEFAULT FALSE,
    volume          DECIMAL(20,2) DEFAULT 0,
    liquidity       DECIMAL(20,2) DEFAULT 0,
    outcome_prices  JSON,                 -- raw price array
    best_bid        DECIMAL(10,6),
    best_ask        DECIMAL(10,6),
    spread          DECIMAL(10,6),
    fetched_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_end_date (end_date),
    INDEX idx_active_closed (active, closed),
    INDEX idx_volume (volume),
    INDEX idx_fetched (fetched_at)
) ENGINE=InnoDB;

-- Historical price snapshots for backtesting
CREATE TABLE IF NOT EXISTS price_history (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    condition_id    VARCHAR(128) NOT NULL,
    token_id        VARCHAR(128),
    timestamp       DATETIME NOT NULL,
    price           DECIMAL(10,6) NOT NULL,
    volume_24h      DECIMAL(20,2),
    liquidity       DECIMAL(20,2),
    INDEX idx_condition_ts (condition_id, timestamp),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB;

-- Market outcomes (ground truth for backtesting)
CREATE TABLE IF NOT EXISTS outcomes (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    condition_id    VARCHAR(128) NOT NULL UNIQUE,
    resolved        BOOLEAN DEFAULT FALSE,
    winning_outcome VARCHAR(16),           -- 'YES' or 'NO'
    resolved_at     DATETIME,
    resolution_price DECIMAL(10,6),
    INDEX idx_resolved (resolved)
) ENGINE=InnoDB;

-- Simulated / actual trades
CREATE TABLE IF NOT EXISTS trades (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    backtest_id     BIGINT,
    condition_id    VARCHAR(128) NOT NULL,
    side            ENUM('BUY','SELL') DEFAULT 'BUY',
    entry_price     DECIMAL(10,6) NOT NULL,
    quantity        DECIMAL(20,2) NOT NULL DEFAULT 1.0,
    entry_time      DATETIME NOT NULL,
    exit_price      DECIMAL(10,6),
    exit_time       DATETIME,
    pnl             DECIMAL(20,6),
    result          ENUM('WIN','LOSS','PENDING') DEFAULT 'PENDING',
    INDEX idx_backtest (backtest_id),
    INDEX idx_condition (condition_id)
) ENGINE=InnoDB;

-- Backtest runs metadata
CREATE TABLE IF NOT EXISTS backtest_runs (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    strategy_name   VARCHAR(128) NOT NULL,
    params          JSON NOT NULL,         -- strategy parameters
    start_date      DATE,
    end_date        DATE,
    total_trades    INT DEFAULT 0,
    winning_trades  INT DEFAULT 0,
    losing_trades   INT DEFAULT 0,
    win_rate        DECIMAL(8,4),
    total_pnl       DECIMAL(20,6),
    avg_pnl         DECIMAL(20,6),
    max_drawdown    DECIMAL(20,6),
    sharpe_ratio    DECIMAL(10,4),
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_strategy (strategy_name),
    INDEX idx_created (created_at)
) ENGINE=InnoDB;

-- Parameter sweep results
CREATE TABLE IF NOT EXISTS parameter_analysis (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    backtest_id     BIGINT,
    pop_threshold   DECIMAL(10,2),         -- popularity threshold
    hours_before    DECIMAL(10,2),         -- hours before deadline
    min_probability DECIMAL(10,4),         -- min win probability
    min_odds        DECIMAL(10,4),         -- min odds to enter
    win_rate        DECIMAL(8,4),
    total_return    DECIMAL(20,6),
    sharpe          DECIMAL(10,4),
    max_drawdown    DECIMAL(20,6),
    num_trades      INT,
    INDEX idx_backtest (backtest_id),
    INDEX idx_params (pop_threshold, hours_before, min_probability)
) ENGINE=InnoDB;

-- Periodic snapshots of active market odds (every 5 min)
CREATE TABLE IF NOT EXISTS active_market_snapshots (
    condition_id    VARCHAR(128) NOT NULL,
    snapshot_time   DATETIME(3) NOT NULL,
    question        TEXT,
    slug            VARCHAR(512),
    end_date        DATETIME,
    yes_price       DECIMAL(10,6),
    no_price        DECIMAL(10,6),
    best_bid        DECIMAL(10,6),
    best_ask        DECIMAL(10,6),
    spread          DECIMAL(10,6),
    volume          DECIMAL(20,2),
    liquidity       DECIMAL(20,2),
    hours_to_close  DECIMAL(10,2),
    PRIMARY KEY (condition_id, snapshot_time),
    INDEX idx_snapshot_time (snapshot_time),
    INDEX idx_volume (volume),
    INDEX idx_end_date (end_date),
    INDEX idx_hours_close (hours_to_close)
) ENGINE=InnoDB;
"""


def get_connection(db=None):
    """Get MySQL connection."""
    cfg = dict(DB_CONFIG)
    if db:
        cfg["database"] = db
    return mysql.connector.connect(**cfg)


def setup_database():
    """Create all tables."""
    conn = get_connection("polymarket")
    cursor = conn.cursor()
    for statement in SCHEMA_SQL.split(";"):
        stmt = statement.strip()
        if stmt:
            cursor.execute(stmt)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Database schema created/verified.")


if __name__ == "__main__":
    setup_database()
