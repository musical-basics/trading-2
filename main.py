"""
main.py — Level 1 Walking Skeleton: CLI Pipeline Runner

Orchestrates all phases sequentially:
  Phase 0: Database Initialization
  Phase 1: Data Ingestion (yfinance → SQLite)
  Phase 2: Signal Generation (SMA Crossover + Pullback)
  Phase 3: Simulation & Risk Limits
  Phase 4: Execution Routing (Alpaca Paper / Dry-Run)

Run with: python main.py
"""

import sys
import os
from datetime import datetime

# Ensure project root is on the path so src imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.pipeline import db_init, data_ingestion, simulation, execution
from src.strategies import strategy, pullback_strategy


def main():
    start_time = datetime.now()

    print()
    print("╔" + "═" * 58 + "╗")
    print("║  LEVEL 1 — WALKING SKELETON PIPELINE                    ║")
    print("║  Started: " + start_time.strftime("%Y-%m-%d %H:%M:%S") + " " * 27 + "║")
    print("╚" + "═" * 58 + "╝")
    print()

    # Phase 0: Initialize Database
    db_init.init_db()

    # Phase 1: Ingest EOD data from yfinance
    data_ingestion.ingest()

    # Phase 2: Compute strategy signals
    strategy.compute_signals()
    pullback_strategy.compute_pullback_signals()

    # Phase 3: Simulate and filter through risk limits
    approved_orders = simulation.simulate_and_filter()

    # Phase 4: Route orders to Alpaca (or dry-run)
    execution.route_orders(approved_orders)

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
