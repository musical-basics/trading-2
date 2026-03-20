"""
ls_zscore_strategy.py — Long/Short Monthly EV/Sales Z-Score Strategy

Strategy rules:
  1. At the start of each month, rank the universe by EV/Sales Z-score
  2. LONG the N lowest Z-score stocks (cheapest relative to peers)
  3. SHORT the N highest Z-score stocks (most expensive relative to peers)
  4. Hold for 1 month, then close all positions and re-rank
  5. Equal weight within each leg

This is a dollar-neutral long/short equity strategy — portfolio return
comes from the spread between cheap and expensive stocks, not market direction.
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import DB_PATH, SLIPPAGE_BPS


def simulate_ls_zscore(n_long=2, n_short=2, starting_capital=10000):
    """
    Simulate the long/short monthly rebalance strategy using
    cross_sectional_scores data.

    Args:
        n_long: Number of stocks to go long (lowest Z-scores)
        n_short: Number of stocks to go short (highest Z-scores)
        starting_capital: Initial portfolio value

    Returns:
        equity_df: DataFrame with [date, equity, long_tickers, short_tickers]
        trades_log: list of dicts describing each monthly rebalance
    """
    conn = sqlite3.connect(DB_PATH)

    # Load Z-scores + prices
    df = pd.read_sql_query("""
        SELECT cs.ticker, cs.date, cs.ev_sales_zscore,
               db.adj_close
        FROM cross_sectional_scores cs
        JOIN daily_bars db ON cs.ticker = db.ticker AND cs.date = db.date
        ORDER BY cs.date, cs.ticker
    """, conn, parse_dates=["date"])
    conn.close()

    if df.empty:
        return pd.DataFrame(), []

    # Add year-month for grouping
    df["year_month"] = df["date"].dt.to_period("M")

    # Get all unique months
    months = sorted(df["year_month"].unique())

    if len(months) < 2:
        return pd.DataFrame(), []

    # For each month, get the FIRST trading day's Z-scores to select positions
    # Then track daily returns through that month
    trades_log = []
    all_daily_returns = []

    for i, month in enumerate(months):
        month_data = df[df["year_month"] == month].copy()
        if month_data.empty:
            continue

        # Get first day of this month to rank and select
        first_day = month_data["date"].min()
        ranking_day = month_data[month_data["date"] == first_day]

        if len(ranking_day) < (n_long + n_short):
            continue  # Not enough tickers to fill both legs

        # Rank: lowest Z-score = LONG, highest Z-score = SHORT
        sorted_rank = ranking_day.sort_values("ev_sales_zscore")
        long_tickers = sorted_rank.head(n_long)["ticker"].tolist()
        short_tickers = sorted_rank.tail(n_short)["ticker"].tolist()

        # Get daily returns for all days in this month
        all_dates = sorted(month_data["date"].unique())

        for j, date in enumerate(all_dates):
            if j == 0:
                continue  # Skip first day (entry day, no return yet)

            prev_date = all_dates[j - 1]
            day_return = 0.0

            # Long leg: profit from price increases
            for ticker in long_tickers:
                curr = month_data[(month_data["date"] == date) & (month_data["ticker"] == ticker)]
                prev = month_data[(month_data["date"] == prev_date) & (month_data["ticker"] == ticker)]
                if not curr.empty and not prev.empty:
                    ret = (curr["adj_close"].iloc[0] / prev["adj_close"].iloc[0]) - 1
                    day_return += ret / n_long  # Equal weight

            # Short leg: profit from price decreases
            for ticker in short_tickers:
                curr = month_data[(month_data["date"] == date) & (month_data["ticker"] == ticker)]
                prev = month_data[(month_data["date"] == prev_date) & (month_data["ticker"] == ticker)]
                if not curr.empty and not prev.empty:
                    ret = (curr["adj_close"].iloc[0] / prev["adj_close"].iloc[0]) - 1
                    day_return -= ret / n_short  # Invert return for short

            # Deduct friction on rebalance day (first trading day of month)
            if j == 1:
                # Slippage for entire portfolio turnover (all positions change)
                day_return -= SLIPPAGE_BPS * 2  # Both legs rebalance

            all_daily_returns.append({
                "date": date,
                "daily_return": day_return,
                "long_tickers": ", ".join(long_tickers),
                "short_tickers": ", ".join(short_tickers),
            })

        trades_log.append({
            "month": str(month),
            "long": long_tickers,
            "short": short_tickers,
            "long_zscores": sorted_rank.head(n_long)["ev_sales_zscore"].tolist(),
            "short_zscores": sorted_rank.tail(n_short)["ev_sales_zscore"].tolist(),
        })

    if not all_daily_returns:
        return pd.DataFrame(), trades_log

    equity_df = pd.DataFrame(all_daily_returns)
    equity_df["equity"] = starting_capital * (1 + equity_df["daily_return"]).cumprod()

    return equity_df, trades_log


if __name__ == "__main__":
    eq, trades = simulate_ls_zscore()
    if not eq.empty:
        print(f"L/S Z-Score Strategy: {len(eq)} days")
        print(f"Final equity: ${eq['equity'].iloc[-1]:,.2f}")
        total_ret = eq['equity'].iloc[-1] / 10000 - 1
        print(f"Total return: {total_ret:+.2%}")
        print()
        for t in trades:
            print(f"  {t['month']}: LONG {t['long']} | SHORT {t['short']}")
    else:
        print("No data. Run Phase 2 first.")
