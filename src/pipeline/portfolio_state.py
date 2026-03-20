"""
portfolio_state.py — Level 2 Portfolio State Utility

Reconstructs the current portfolio from either:
  - Alpaca API (live mode): queries real account equity and positions
  - Paper executions (dry-run mode): reconstructs from paper_executions table

Provides a unified interface for the portfolio rebalancer.
"""

import sqlite3
import os
from src.config import DB_PATH


def get_portfolio_state():
    """
    Get current portfolio state (total equity and holdings).
    Auto-selects Alpaca API or paper-based reconstruction.

    Returns:
        total_equity (float): Total portfolio value
        holdings (dict): {ticker: {'shares': int, 'avg_price': float}}
    """
    api_key = os.getenv("ALPACA_API_KEY", "").strip()
    secret_key = os.getenv("ALPACA_SECRET_KEY", "").strip()

    if api_key and secret_key:
        return _get_portfolio_from_alpaca()
    else:
        return _get_portfolio_from_paper()


def _get_portfolio_from_alpaca():
    """Query Alpaca API for live portfolio state."""
    try:
        import alpaca_trade_api as tradeapi

        api_key = os.getenv("ALPACA_API_KEY", "").strip()
        secret_key = os.getenv("ALPACA_SECRET_KEY", "").strip()
        base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

        api = tradeapi.REST(api_key, secret_key, base_url, api_version="v2")
        account = api.get_account()
        total_equity = float(account.equity)

        positions = api.list_positions()
        holdings = {}
        for pos in positions:
            holdings[pos.symbol] = {
                "shares": int(pos.qty),
                "avg_price": float(pos.avg_entry_price),
            }

        return total_equity, holdings

    except Exception as e:
        print(f"  ⚠ Alpaca API failed: {e}. Falling back to paper state.")
        return _get_portfolio_from_paper()


def _get_portfolio_from_paper():
    """
    Reconstruct portfolio from paper_executions table.
    Uses a default starting equity of $100,000.
    """
    STARTING_EQUITY = 100_000.0

    conn = sqlite3.connect(DB_PATH)
    try:
        import pandas as pd

        executions = pd.read_sql_query("""
            SELECT ticker, action, quantity, simulated_price
            FROM paper_executions
            ORDER BY timestamp
        """, conn)

        holdings = {}
        cash_spent = 0.0

        if not executions.empty:
            for _, row in executions.iterrows():
                ticker = row["ticker"]
                qty = int(row["quantity"])
                price = float(row["simulated_price"])

                if ticker not in holdings:
                    holdings[ticker] = {"shares": 0, "avg_price": 0.0}

                if row["action"] == "BUY":
                    # Update average price
                    current = holdings[ticker]
                    total_cost = (current["shares"] * current["avg_price"]) + (qty * price)
                    total_shares = current["shares"] + qty
                    holdings[ticker]["shares"] = total_shares
                    holdings[ticker]["avg_price"] = total_cost / total_shares if total_shares > 0 else 0
                    cash_spent += qty * price

                elif row["action"] == "SELL":
                    holdings[ticker]["shares"] -= qty
                    cash_spent -= qty * price

            # Remove liquidated positions
            holdings = {t: h for t, h in holdings.items() if h["shares"] > 0}

        # Estimate current equity (starting - cash spent + current value of holdings)
        # For simplicity, use the most recent price from daily_bars
        holdings_value = 0.0
        for ticker, info in holdings.items():
            try:
                price_row = pd.read_sql_query(
                    "SELECT adj_close FROM daily_bars WHERE ticker = ? ORDER BY date DESC LIMIT 1",
                    conn, params=(ticker,)
                )
                if not price_row.empty:
                    holdings_value += info["shares"] * price_row["adj_close"].iloc[0]
                else:
                    holdings_value += info["shares"] * info["avg_price"]
            except Exception:
                holdings_value += info["shares"] * info["avg_price"]

        remaining_cash = STARTING_EQUITY - cash_spent
        total_equity = remaining_cash + holdings_value

        return total_equity, holdings

    finally:
        conn.close()


if __name__ == "__main__":
    equity, holdings = get_portfolio_state()
    print(f"Total equity: ${equity:,.2f}")
    print(f"Holdings: {holdings}")
