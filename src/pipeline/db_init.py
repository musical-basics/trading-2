"""
db_init.py — Level 2 Database Initialization

Creates the SQLite database with all required tables:
  Level 1 (preserved):
    - daily_bars: Raw EOD market data
    - strategy_signals: SMA crossover signals
    - pullback_signals: RSI pullback signals
    - paper_executions: Execution ledger for paper trades
  Level 2 (new):
    - quarterly_fundamentals: Raw quarterly financial reports
    - cross_sectional_scores: Daily EV/Sales Z-scores & target weights
    - wfo_results: Walk-Forward Optimization backtester metrics

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

    # ── Level 1 Tables ───────────────────────────────────────────

    # Table 1: daily_bars (unchanged)
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

    # Table 2: strategy_signals (unchanged)
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

    # Table 3: paper_executions (unchanged)
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

    # Table 4: pullback_signals (unchanged)
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

    # ── Level 2 Tables ───────────────────────────────────────────

    # Table 5: quarterly_fundamentals (NEW)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS quarterly_fundamentals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            period_end_date DATE NOT NULL,
            filing_date DATE NOT NULL,
            revenue REAL,
            total_debt REAL,
            cash_and_equivalents REAL,
            shares_outstanding REAL,
            UNIQUE(ticker, period_end_date)
        )
    """)

    # Table 6: cross_sectional_scores (NEW)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cross_sectional_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            enterprise_value REAL,
            ev_to_sales REAL,
            ev_sales_zscore REAL,
            target_weight REAL DEFAULT 0.0,
            UNIQUE(ticker, date)
        )
    """)

    # Table 7: wfo_results (NEW)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wfo_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT NOT NULL,
            test_window_start DATE NOT NULL,
            test_window_end DATE NOT NULL,
            sharpe_ratio REAL,
            max_drawdown REAL,
            cagr REAL
        )
    """)

    conn.commit()
    conn.close()

    all_tables = [
        "daily_bars", "strategy_signals", "pullback_signals",
        "paper_executions", "quarterly_fundamentals",
        "cross_sectional_scores", "wfo_results",
    ]
    print(f"  ✓ Database initialized at: {DB_PATH}")
    print(f"  ✓ Tables created: {', '.join(all_tables)}")
    print()


if __name__ == "__main__":
    init_db()
