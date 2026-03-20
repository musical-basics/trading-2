"""
db_init.py — Level 1 Database Initialization

Creates the SQLite database with four tables:
  - daily_bars: Raw EOD market data
  - strategy_signals: Computed SMA crossover signals
  - pullback_signals: Computed RSI pullback signals
  - paper_executions: Execution ledger for paper trades

All tables use CREATE TABLE IF NOT EXISTS for idempotency.
"""

import sqlite3
import os
from src.config import DB_PATH


def init_db():
    """Initialize the SQLite database and create all required tables."""
    print("=" * 60)
    print("PHASE 0: Database Initialization")
    print("=" * 60)

    # Ensure data/ directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ── Table 1: daily_bars ──────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_bars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume INTEGER,
            UNIQUE(ticker, date)
        )
    """)

    # ── Table 2: strategy_signals ────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            sma_50 REAL,
            sma_200 REAL,
            signal INTEGER DEFAULT 0,
            UNIQUE(ticker, date)
        )
    """)

    # ── Table 3: paper_executions ────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_executions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            ticker TEXT NOT NULL,
            action TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            simulated_price REAL NOT NULL,
            strategy_id TEXT DEFAULT 'sma_crossover'
        )
    """)

    # ── Table 4: pullback_signals ────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pullback_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            close REAL,
            sma_200 REAL,
            rsi_3 REAL,
            adv_30 REAL,
            signal REAL DEFAULT 0.0,
            exit_signal TEXT DEFAULT NULL,
            UNIQUE(ticker, date)
        )
    """)

    conn.commit()
    conn.close()

    print(f"  ✓ Database initialized at: {DB_PATH}")
    print(f"  ✓ Tables created: daily_bars, strategy_signals, pullback_signals, paper_executions")
    print()


if __name__ == "__main__":
    init_db()
