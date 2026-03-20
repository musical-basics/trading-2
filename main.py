"""
main.py — Level 2 Quant Sandbox: CLI Pipeline Runner

Orchestrates all phases sequentially:
  Phase 0: Database Initialization
  Phase 1a: EOD Price Ingestion (yfinance → SQLite)
  Phase 1b: Quarterly Fundamental Ingestion (yfinance → SQLite)
  Phase 2: Cross-Sectional Scoring (merge_asof + EV/Sales Z-scores)
  Phase 3: Walk-Forward Optimization Tournament
  Phase 4: Portfolio Rebalance → Execution Routing (Alpaca Paper / Dry-Run)

Run with: python main.py
"""

import sys
import os
from datetime import datetime

# Ensure project root is on the path so src imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.pipeline import (
    db_init,
    data_ingestion,
    fundamental_ingestion,
    cross_sectional_scoring,
    wfo_backtester,
    portfolio_rebalancer,
    execution,
)


def main():
    start_time = datetime.now()

    print()
    print("╔" + "═" * 58 + "╗")
    print("║  LEVEL 2 — QUANT SANDBOX PIPELINE                       ║")
    print("║  Started: " + start_time.strftime("%Y-%m-%d %H:%M:%S") + " " * 27 + "║")
    print("╚" + "═" * 58 + "╝")
    print()

    # Phase 0: Initialize Database
    db_init.init_db()

    # Phase 1a: Ingest EOD prices
    data_ingestion.ingest()

    # Phase 1b: Ingest quarterly fundamentals
    fundamental_ingestion.ingest_fundamentals()

    # Phase 2: Compute cross-sectional EV/Sales Z-scores
    cross_sectional_scoring.compute_cross_sectional_scores()

    # Phase 3: Walk-Forward Optimization tournament
    wfo_backtester.run_wfo_tournament()

    # Phase 4: Portfolio rebalance → execution
    orders = portfolio_rebalancer.rebalance_portfolio()
    execution.route_orders(orders)

    # Done
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    print()
    print("╔" + "═" * 58 + "╗")
    print("║  PIPELINE COMPLETE                                      ║")
    print("║  Finished: " + end_time.strftime("%Y-%m-%d %H:%M:%S") + " " * 26 + "║")
    print(f"║  Elapsed: {elapsed:.1f}s" + " " * (47 - len(f"{elapsed:.1f}s")) + "║")
    print("╚" + "═" * 58 + "╝")
    print()


if __name__ == "__main__":
    main()
