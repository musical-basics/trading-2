"""
strategy_tournament.py — Run all 5 strategies as portfolio-level backtests
and produce comparable metrics (Sharpe, MaxDD, CAGR, Total Return).

Strategies:
  0. Buy & Hold (equal-weight) — baseline benchmark
  1. EV/Sales Z-Score (long-only) — existing WFO approach
  2. L/S Z-Score — long cheapest, short most expensive, monthly rebalance
  3. SMA Crossover (equal-weight) — portfolio of all tickers, equal allocation
  4. Pullback RSI (equal-weight) — portfolio of all tickers, equal allocation
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import DB_PATH, SLIPPAGE_BPS


def _compute_metrics(equity_series, daily_returns):
    """Compute Sharpe, MaxDD, CAGR, Total Return from an equity curve."""
    trading_days = len(daily_returns)

    # Sharpe
    if daily_returns.std() > 0:
        sharpe = daily_returns.mean() / daily_returns.std() * np.sqrt(252)
    else:
        sharpe = 0.0

    # Max Drawdown
    running_max = equity_series.expanding().max()
    drawdown = 1 - equity_series / running_max
    max_dd = drawdown.max()

    # CAGR
    if trading_days > 0 and equity_series.iloc[0] > 0:
        total_return_factor = equity_series.iloc[-1] / equity_series.iloc[0]
        cagr = total_return_factor ** (252 / max(trading_days, 1)) - 1
    else:
        cagr = 0.0

    total_return = equity_series.iloc[-1] / equity_series.iloc[0] - 1

    return {
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "cagr": cagr,
        "total_return": total_return,
        "trading_days": trading_days,
    }


def run_buyhold_portfolio(conn, starting_capital=10000):
    """
    Buy & Hold (equal-weight): hold all tickers in the universe
    with 1/N allocation from day 1. No signals needed.
    """
    tickers = pd.read_sql_query(
        "SELECT DISTINCT ticker FROM daily_bars WHERE ticker != 'SPY' ORDER BY ticker", conn
    )["ticker"].tolist()

    if not tickers:
        return pd.DataFrame(), {}

    n = len(tickers)
    all_daily = []

    for ticker in tickers:
        df = pd.read_sql_query("""
            SELECT date, adj_close FROM daily_bars
            WHERE ticker = ? ORDER BY date
        """, conn, params=(ticker,), parse_dates=["date"])

        if df.empty:
            continue

        df["daily_return"] = df["adj_close"].pct_change()
        df["weighted_return"] = df["daily_return"] / n
        all_daily.append(df[["date", "weighted_return"]].copy())

    if not all_daily:
        return pd.DataFrame(), {}

    combined = pd.concat(all_daily)
    portfolio = combined.groupby("date")["weighted_return"].sum().reset_index()
    portfolio.columns = ["date", "daily_return"]
    portfolio = portfolio.sort_values("date").reset_index(drop=True)
    portfolio["equity"] = starting_capital * (1 + portfolio["daily_return"].fillna(0)).cumprod()

    metrics = _compute_metrics(portfolio["equity"], portfolio["daily_return"].fillna(0))
    return portfolio, metrics


def run_sma_portfolio(conn, starting_capital=10000):
    """
    SMA Crossover portfolio: equal-weight every ticker in the universe.
    For each ticker, if position=1 (after BUY signal), allocate 1/N of capital.
    Aggregate daily returns across all tickers.
    """
    tickers = pd.read_sql_query(
        "SELECT DISTINCT ticker FROM strategy_signals ORDER BY ticker", conn
    )["ticker"].tolist()

    if not tickers:
        return pd.DataFrame(), {}

    n = len(tickers)
    all_daily = []

    for ticker in tickers:
        df = pd.read_sql_query("""
            SELECT s.date, s.signal, b.adj_close
            FROM strategy_signals s
            JOIN daily_bars b ON s.ticker = b.ticker AND s.date = b.date
            WHERE s.ticker = ? ORDER BY s.date
        """, conn, params=(ticker,), parse_dates=["date"])

        if df.empty:
            continue

        df["daily_return"] = df["adj_close"].pct_change()

        # Track position
        pos = 0
        positions = []
        for sig in df["signal"]:
            if sig == 1:
                pos = 1
            elif sig == -1:
                pos = 0
            positions.append(pos)

        df["position"] = pd.Series(positions).shift(1).fillna(0).values
        df["weighted_return"] = df["daily_return"] * df["position"] / n
        all_daily.append(df[["date", "weighted_return"]].copy())

    if not all_daily:
        return pd.DataFrame(), {}

    combined = pd.concat(all_daily)
    portfolio = combined.groupby("date")["weighted_return"].sum().reset_index()
    portfolio.columns = ["date", "daily_return"]
    portfolio = portfolio.sort_values("date").reset_index(drop=True)
    portfolio["equity"] = starting_capital * (1 + portfolio["daily_return"]).cumprod()

    metrics = _compute_metrics(portfolio["equity"], portfolio["daily_return"])
    return portfolio, metrics


def run_pullback_portfolio(conn, starting_capital=10000):
    """
    Pullback RSI portfolio: equal-weight every ticker.
    For each ticker, simulate the pullback strategy and track position.
    """
    from src.strategies import pullback_strategy

    tickers = pd.read_sql_query(
        "SELECT DISTINCT ticker FROM pullback_signals ORDER BY ticker", conn
    )["ticker"].tolist()

    if not tickers:
        return pd.DataFrame(), {}

    n = len(tickers)
    all_daily = []

    for ticker in tickers:
        df_sim = pullback_strategy.simulate_pullback(ticker, conn)
        if df_sim.empty:
            continue

        df_sim["date"] = pd.to_datetime(df_sim["date"])
        df_sim["weighted_return"] = df_sim["strategy_return"].fillna(0) / n
        all_daily.append(df_sim[["date", "weighted_return"]].copy())

    if not all_daily:
        return pd.DataFrame(), {}

    combined = pd.concat(all_daily)
    portfolio = combined.groupby("date")["weighted_return"].sum().reset_index()
    portfolio.columns = ["date", "daily_return"]
    portfolio = portfolio.sort_values("date").reset_index(drop=True)
    portfolio["equity"] = starting_capital * (1 + portfolio["daily_return"]).cumprod()

    metrics = _compute_metrics(portfolio["equity"], portfolio["daily_return"])
    return portfolio, metrics


def run_ls_zscore(starting_capital=10000, n_long=2, n_short=2):
    """Wrapper around ls_zscore_strategy."""
    from src.strategies.ls_zscore_strategy import simulate_ls_zscore
    eq, trades = simulate_ls_zscore(n_long=n_long, n_short=n_short,
                                     starting_capital=starting_capital)
    if eq.empty:
        return pd.DataFrame(), {}

    eq["date"] = pd.to_datetime(eq["date"])
    metrics = _compute_metrics(eq["equity"], eq["daily_return"])
    return eq[["date", "daily_return", "equity"]], metrics


def run_ev_sales_longonly(conn, starting_capital=10000):
    """
    EV/Sales Z-Score long-only: buy stocks below Z < -1, equal weight,
    with max single weight cap.
    """
    df = pd.read_sql_query("""
        SELECT cs.ticker, cs.date, cs.ev_sales_zscore, cs.target_weight,
               db.adj_close
        FROM cross_sectional_scores cs
        JOIN daily_bars db ON cs.ticker = db.ticker AND cs.date = db.date
        ORDER BY cs.date, cs.ticker
    """, conn, parse_dates=["date"])

    if df.empty:
        return pd.DataFrame(), {}

    df = df.sort_values(["ticker", "date"])
    df["daily_return"] = df.groupby("ticker")["adj_close"].pct_change()

    # Use target_weight from cross_sectional_scoring
    df["weighted_return"] = df["target_weight"] * df["daily_return"]
    portfolio = df.groupby("date")["weighted_return"].sum().reset_index()
    portfolio.columns = ["date", "daily_return"]
    portfolio = portfolio.sort_values("date").reset_index(drop=True)
    portfolio["equity"] = starting_capital * (1 + portfolio["daily_return"]).cumprod()

    metrics = _compute_metrics(portfolio["equity"], portfolio["daily_return"])
    return portfolio, metrics


def run_tournament(n_long=2, n_short=2):
    """
    Run all 5 strategies and save results to wfo_results.
    Returns dict of {strategy_name: (equity_df, metrics)}.
    """
    conn = sqlite3.connect(DB_PATH)
    results = {}

    # Strategy 0: Buy & Hold (equal-weight baseline)
    try:
        eq, met = run_buyhold_portfolio(conn)
        if not eq.empty:
            results["Buy & Hold (EW)"] = (eq, met)
    except Exception as e:
        print(f"  ⚠ Buy & Hold failed: {e}")

    # Strategy 1: EV/Sales Z-Score (long-only)
    try:
        eq, met = run_ev_sales_longonly(conn)
        if not eq.empty:
            results["EV/Sales Long-Only"] = (eq, met)
    except Exception as e:
        print(f"  ⚠ EV/Sales Long-Only failed: {e}")

    # Strategy 2: L/S Z-Score
    try:
        eq, met = run_ls_zscore(n_long=n_long, n_short=n_short)
        if not eq.empty:
            results["L/S Z-Score"] = (eq, met)
    except Exception as e:
        print(f"  ⚠ L/S Z-Score failed: {e}")

    # Strategy 3: SMA Crossover (equal-weight portfolio)
    try:
        eq, met = run_sma_portfolio(conn)
        if not eq.empty:
            results["SMA Crossover (EW)"] = (eq, met)
    except Exception as e:
        print(f"  ⚠ SMA Crossover failed: {e}")

    # Strategy 4: Pullback RSI (equal-weight portfolio)
    try:
        eq, met = run_pullback_portfolio(conn)
        if not eq.empty:
            results["Pullback RSI (EW)"] = (eq, met)
    except Exception as e:
        print(f"  ⚠ Pullback RSI failed: {e}")

    # Save results to wfo_results table
    cursor = conn.cursor()
    for name, (eq_df, metrics) in results.items():
        strategy_id = name.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
        cursor.execute("DELETE FROM wfo_results WHERE strategy_id = ?", (strategy_id,))
        cursor.execute("""
            INSERT INTO wfo_results
            (strategy_id, test_window_start, test_window_end,
             sharpe_ratio, max_drawdown, cagr)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            strategy_id,
            eq_df["date"].min().strftime("%Y-%m-%d"),
            eq_df["date"].max().strftime("%Y-%m-%d"),
            metrics["sharpe"],
            metrics["max_drawdown"],
            metrics["cagr"],
        ))

    conn.commit()
    conn.close()

    return results


if __name__ == "__main__":
    results = run_tournament()
    print(f"\n{'Strategy':<25} {'Return':>10} {'Sharpe':>10} {'MaxDD':>10} {'CAGR':>10}")
    print("-" * 65)
    for name, (eq, met) in results.items():
        print(f"{name:<25} {met['total_return']:>+9.2%} {met['sharpe']:>10.2f} "
              f"{met['max_drawdown']:>9.2%} {met['cagr']:>9.2%}")
