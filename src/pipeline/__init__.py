"""
src/pipeline — Backward-compatible re-exports.

Modules have been reorganized into subfolders:
  core/           → db_init
  data_sources/   → data_ingestion, fundamental providers
  scoring/        → cross_sectional_scoring
  backtesting/    → wfo_backtester, wfo_multi, strategy_tournament
  execution/      → order_router, simulation, portfolio_rebalancer, portfolio_state

These re-exports ensure old imports (from src.pipeline import X) still work.
"""

from src.pipeline.core import db_init
from src.pipeline.data_sources import data_ingestion
from src.pipeline.data_sources.yfinance import fundamentals as fundamental_ingestion
from src.pipeline.scoring import cross_sectional_scoring
from src.pipeline.backtesting import wfo_backtester, wfo_multi, strategy_tournament
from src.pipeline.execution import (
    order_router as execution,
    simulation,
    portfolio_rebalancer,
    portfolio_state,
)
