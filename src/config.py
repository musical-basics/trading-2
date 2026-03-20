"""
src/config.py — Centralized Configuration

Single source of truth for paths and shared constants.
All modules import DB_PATH from here instead of computing it locally.
"""

import os
from dotenv import load_dotenv

# ── Project Root ─────────────────────────────────────────────
# config.py lives at <project>/src/config.py
# So project root is one directory up from this file
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Database Path ────────────────────────────────────────────
DB_PATH = os.path.join(PROJECT_ROOT, "data", "level1_trading.db")

# ── Environment Variables ────────────────────────────────────
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# ── Default Universe ─────────────────────────────────────────
DEFAULT_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
    "SPY", "QQQ", "GLD", "META", "NVDA",
]
