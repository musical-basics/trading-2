"""
shared.py — Shared helpers and sidebar configuration for all pages.
"""

import sys
import os
import sqlite3
import pandas as pd
import streamlit as st

# Ensure project root is on the path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.config import DB_PATH


def get_db_connection():
    return sqlite3.connect(DB_PATH)


def table_exists(table_name):
    try:
        conn = get_db_connection()
        count = pd.read_sql_query(
            f"SELECT COUNT(*) as cnt FROM {table_name}", conn
        )["cnt"][0]
        conn.close()
        return count > 0
    except Exception:
        return False


def get_table_count(table_name):
    try:
        conn = get_db_connection()
        count = pd.read_sql_query(
            f"SELECT COUNT(*) as cnt FROM {table_name}", conn
        )["cnt"][0]
        conn.close()
        return count
    except Exception:
        return 0


def render_sidebar():
    """Render the shared sidebar configuration. Returns config dict."""
    with st.sidebar:
        st.markdown("## ⚙️ Configuration")
        st.divider()

        api_key = os.getenv("ALPACA_API_KEY", "").strip()
        api_secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
        if api_key and api_secret:
            st.success("🟢 Alpaca API Keys Loaded")
        else:
            st.warning("🟡 Dry-Run Mode")

        st.divider()

        universe_input = st.text_area(
            "Ticker Universe",
            value="AAPL, MSFT, GOOGL, AMZN, TSLA, SPY, QQQ, GLD, META, NVDA",
            height=68,
        )
        universe_list = [t.strip().upper() for t in universe_input.split(",") if t.strip()]
        st.caption(f"{len(universe_list)} tickers")

        st.divider()

        st.markdown("**SMA Crossover**")
        fast_sma = st.number_input("Fast SMA", min_value=5, max_value=100, value=50, step=5)
        slow_sma = st.number_input("Slow SMA", min_value=50, max_value=500, value=200, step=10)

        st.markdown("**Pullback (RSI)**")
        rsi_period = st.number_input("RSI Period", min_value=2, max_value=14, value=3, step=1)
        rsi_entry = st.number_input("RSI Entry", min_value=5, max_value=40, value=20, step=5)
        rsi_exit = st.number_input("RSI Exit", min_value=50, max_value=90, value=70, step=5)

        st.divider()

        st.markdown("**Risk Limits**")
        capital_per_trade = st.number_input("Capital/Trade ($)", min_value=100, max_value=10000, value=1000, step=100)
        max_positions = st.number_input("Max Positions", min_value=1, max_value=20, value=5, step=1)

    return {
        "universe": universe_list,
        "fast_sma": int(fast_sma),
        "slow_sma": int(slow_sma),
        "rsi_period": int(rsi_period),
        "rsi_entry": int(rsi_entry),
        "rsi_exit": int(rsi_exit),
        "capital_per_trade": float(capital_per_trade),
        "max_positions": int(max_positions),
    }
