"""
src/config.py — Centralized Configuration (Level 2)

Single source of truth for paths, constants, and shared parameters.
All modules import from here instead of computing locally.
"""

import os
from dotenv import load_dotenv

# ── Project Root ─────────────────────────────────────────────
# config.py lives at <project>/src/config.py
# So project root is one directory up from this file
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Database Path ────────────────────────────────────────────
DB_PATH = os.path.join(PROJECT_ROOT, "data", "level2_trading.db")

# ── Environment Variables ────────────────────────────────────
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# ── Default Universe (Expanded S&P 50 Subset) ───────────────
DEFAULT_UNIVERSE = [
    # Technology
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO",
    "ORCL", "CRM", "AMD", "ADBE", "INTC", "CSCO", "QCOM",
    # Financials
    "JPM", "V", "MA", "BAC", "WFC", "GS", "MS",
    # Healthcare
    "UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO",
    # Consumer
    "WMT", "PG", "KO", "PEP", "COST", "MCD", "NKE", "HD",
    # Industrials & Energy
    "XOM", "CVX", "CAT", "BA", "UPS", "GE", "HON",
    # Communications & Utilities
    "DIS", "NFLX", "CMCSA", "T", "VZ",
    # ETFs (reference benchmarks)
    "SPY", "QQQ",
]

# ── Level 2 WFO / Friction Constants ────────────────────────
SLIPPAGE_BPS = 0.0005           # 5 basis points per trade
COMMISSION_PER_SHARE = 0.005    # $0.005 per share

# ── Portfolio Constraints ────────────────────────────────────
MAX_SINGLE_WEIGHT = 0.10        # No single stock > 10% of portfolio
CASH_BUFFER = 0.05              # Always keep 5% cash

# ── Signal Thresholds ───────────────────────────────────────
ZSCORE_BUY_THRESHOLD = -1.0     # Z-score < -1.0 → undervalued → BUY

# ── Fundamental Data Alignment ──────────────────────────────
FILING_DELAY_DAYS = 45          # Proxy SEC filing delay (period_end + 45d)

# ── Liquidity Gating ────────────────────────────────────────
ADV_LOOKBACK = 30               # 30-day Average Daily Volume lookback
ADV_MAX_PCT = 0.01              # Max trade size = 1% of 30-day ADV

# ── WFO Window Parameters ───────────────────────────────────
WFO_TRAIN_YEARS = 2             # Lookback training window
WFO_TEST_YEARS = 1              # Forward test window
WFO_STEP_YEARS = 1              # Roll step
WFO_ZSCORE_CANDIDATES = [-0.5, -0.75, -1.0, -1.25, -1.5]

