"""
wfo_multi.py — True Walk-Forward Optimization for all 4 strategies.

For each strategy, sweeps tunable parameters on a training window,
then evaluates with the best parameters on the test window.
Rolling windows are stitched into one out-of-sample equity curve.

Tunable parameters per strategy:
  EV/Sales Long-Only : Z-score buy threshold
  L/S Z-Score        : n_long, n_short
  SMA Crossover      : fast_sma, slow_sma windows
  Pullback RSI       : rsi_period, rsi_entry threshold
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import DB_PATH, MAX_SINGLE_WEIGHT, CASH_BUFFER


def _compute_metrics(daily_returns):
    """Compute Sharpe, MaxDD, CAGR from daily returns."""
    if len(daily_returns) < 2 or daily_returns.std() == 0:
        return {"sharpe": 0.0, "max_drawdown": 0.0, "cagr": 0.0}

    sharpe = daily_returns.mean() / daily_returns.std() * np.sqrt(252)
    equity = (1 + daily_returns).cumprod()
    running_max = equity.expanding().max()
    max_dd = (1 - equity / running_max).max()
    cagr = equity.iloc[-1] ** (252 / len(daily_returns)) - 1

    return {"sharpe": sharpe, "max_drawdown": max_dd, "cagr": cagr}


def _get_date_windows(all_dates, train_frac=0.66):
    """
    Given limited data (~14mo), create a single train/test split.
    With more data, this would become a rolling loop.
    """
    n = len(all_dates)
    split = int(n * train_frac)
    if split < 20 or (n - split) < 10:
        return []

    windows = [{
        "train_start": all_dates[0],
        "train_end": all_dates[split - 1],
        "test_start": all_dates[split],
        "test_end": all_dates[-1],
    }]

    # If enough data, add a second overlapping window
    if n > 200:
        split2 = int(n * 0.5)
        windows.append({
            "train_start": all_dates[int(n * 0.16)],
            "train_end": all_dates[int(n * 0.66)],
            "test_start": all_dates[int(n * 0.66) + 1],
            "test_end": all_dates[-1],
        })

    return windows


# ═══════════════════════════════════════════════════════════════
# Strategy 1: EV/Sales Z-Score Long-Only
# Tunable: Z-score buy threshold
# ═══════════════════════════════════════════════════════════════
def _ev_sales_sharpe(data, threshold):
    """Quick Sharpe for a given Z-score threshold on training data."""
    d = data.copy()
    d["weight"] = 0.0
    buy = d["ev_sales_zscore"] < threshold
    if not buy.any():
        return -np.inf

    counts = d.loc[buy].groupby("date")["ticker"].transform("count")
    d.loc[buy, "weight"] = np.minimum(1.0 / counts.values, MAX_SINGLE_WEIGHT)
    d["wr"] = d["weight"] * d["daily_return"]
    pr = d.groupby("date")["wr"].sum()
    return pr.mean() / pr.std() * np.sqrt(252) if pr.std() > 0 else -np.inf


def _ev_sales_simulate(data, threshold, starting_eq=1.0):
    """Simulate EV/Sales with a given threshold, return equity series."""
    d = data.copy()
    d["weight"] = 0.0
    buy = d["ev_sales_zscore"] < threshold
    if buy.any():
        counts = d.loc[buy].groupby("date")["ticker"].transform("count")
        d.loc[buy, "weight"] = np.minimum(1.0 / counts.values, MAX_SINGLE_WEIGHT)

    d["wr"] = d["weight"] * d["daily_return"]
    port = d.groupby("date")["wr"].sum().reset_index()
    port.columns = ["date", "daily_return"]
    port = port.sort_values("date")
    port["equity"] = starting_eq * (1 + port["daily_return"]).cumprod()
    return port


def wfo_ev_sales(conn):
    """WFO for EV/Sales Z-Score strategy."""
    df = pd.read_sql_query("""
        SELECT cs.ticker, cs.date, cs.ev_sales_zscore, db.adj_close
        FROM cross_sectional_scores cs
        JOIN daily_bars db ON cs.ticker = db.ticker AND cs.date = db.date
        ORDER BY cs.date, cs.ticker
    """, conn, parse_dates=["date"])
    if df.empty:
        return None

    df = df.sort_values(["ticker", "date"])
    df["daily_return"] = df.groupby("ticker")["adj_close"].pct_change()

    all_dates = sorted(df["date"].unique())
    windows = _get_date_windows(all_dates)
    if not windows:
        return None

    thresholds = [-2.0, -1.5, -1.0, -0.5, 0.0, 0.5]
    oos_parts = []
    cum_eq = 1.0
    results = []

    for w in windows:
        train = df[(df["date"] >= w["train_start"]) & (df["date"] <= w["train_end"])]
        test = df[(df["date"] >= w["test_start"]) & (df["date"] <= w["test_end"])]

        best_t, best_s = thresholds[2], -np.inf
        for t in thresholds:
            s = _ev_sales_sharpe(train, t)
            if s > best_s:
                best_s, best_t = s, t

        oos_eq = _ev_sales_simulate(test, best_t, cum_eq)
        cum_eq = oos_eq["equity"].iloc[-1]
        oos_parts.append(oos_eq)
        results.append({
            "window": f"{w['test_start'].strftime('%Y-%m-%d')} → {w['test_end'].strftime('%Y-%m-%d')}",
            "best_param": f"threshold={best_t}",
            "train_sharpe": round(best_s, 3),
            **_compute_metrics(oos_eq["daily_return"]),
        })

    stitched = pd.concat(oos_parts, ignore_index=True)
    return {
        "name": "EV/Sales Long-Only",
        "stitched": stitched,
        "windows": results,
        "overall": _compute_metrics(stitched["daily_return"]),
    }


# ═══════════════════════════════════════════════════════════════
# Strategy 2: L/S Z-Score
# Tunable: n_long, n_short
# ═══════════════════════════════════════════════════════════════
def _ls_simulate_window(conn, start, end, n_long, n_short, starting_eq=1.0):
    """Simulate L/S Z-Score over a specific date range."""
    scores = pd.read_sql_query("""
        SELECT cs.ticker, cs.date, cs.ev_sales_zscore, db.adj_close
        FROM cross_sectional_scores cs
        JOIN daily_bars db ON cs.ticker = db.ticker AND cs.date = db.date
        WHERE cs.date >= ? AND cs.date <= ?
        ORDER BY cs.date, cs.ticker
    """, conn, params=(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
        parse_dates=["date"])

    if scores.empty:
        return pd.DataFrame(), -np.inf

    scores = scores.sort_values(["ticker", "date"])
    scores["daily_return"] = scores.groupby("ticker")["adj_close"].pct_change()

    # Monthly rebalance: assign weights at month boundaries
    scores["month"] = scores["date"].dt.to_period("M")
    months = sorted(scores["month"].unique())

    daily_returns = []
    for m in months:
        month_data = scores[scores["month"] == m].copy()
        first_day = month_data.groupby("ticker").first().reset_index()
        ranked = first_day.sort_values("ev_sales_zscore")

        longs = ranked.head(n_long)["ticker"].tolist()
        shorts = ranked.tail(n_short)["ticker"].tolist()

        for _, row in month_data.iterrows():
            w = 0.0
            if row["ticker"] in longs:
                w = 1.0 / n_long
            elif row["ticker"] in shorts:
                w = -1.0 / n_short
            daily_returns.append({"date": row["date"], "wr": w * row["daily_return"]})

    if not daily_returns:
        return pd.DataFrame(), -np.inf

    port = pd.DataFrame(daily_returns).groupby("date")["wr"].sum().reset_index()
    port.columns = ["date", "daily_return"]
    port = port.sort_values("date")
    port["equity"] = starting_eq * (1 + port["daily_return"]).cumprod()

    sharpe = port["daily_return"].mean() / port["daily_return"].std() * np.sqrt(252) if port["daily_return"].std() > 0 else -np.inf
    return port, sharpe


def wfo_ls_zscore(conn):
    """WFO for L/S Z-Score strategy."""
    dates_df = pd.read_sql_query(
        "SELECT DISTINCT date FROM cross_sectional_scores ORDER BY date", conn, parse_dates=["date"]
    )
    if dates_df.empty:
        return None

    all_dates = sorted(dates_df["date"].tolist())
    windows = _get_date_windows(all_dates)
    if not windows:
        return None

    candidates = [(1, 1), (1, 2), (2, 2), (2, 3), (3, 3), (1, 4), (2, 4)]
    oos_parts = []
    cum_eq = 1.0
    results = []

    for w in windows:
        best_params, best_s = (2, 2), -np.inf
        for nl, ns in candidates:
            _, s = _ls_simulate_window(conn, w["train_start"], w["train_end"], nl, ns)
            if s > best_s:
                best_s, best_params = s, (nl, ns)

        oos_eq, _ = _ls_simulate_window(conn, w["test_start"], w["test_end"],
                                         best_params[0], best_params[1], cum_eq)
        if oos_eq.empty:
            continue

        cum_eq = oos_eq["equity"].iloc[-1]
        oos_parts.append(oos_eq)
        results.append({
            "window": f"{w['test_start'].strftime('%Y-%m-%d')} → {w['test_end'].strftime('%Y-%m-%d')}",
            "best_param": f"long={best_params[0]}, short={best_params[1]}",
            "train_sharpe": round(best_s, 3),
            **_compute_metrics(oos_eq["daily_return"]),
        })

    if not oos_parts:
        return None

    stitched = pd.concat(oos_parts, ignore_index=True)
    return {
        "name": "L/S Z-Score",
        "stitched": stitched,
        "windows": results,
        "overall": _compute_metrics(stitched["daily_return"]),
    }


# ═══════════════════════════════════════════════════════════════
# Strategy 3: SMA Crossover (equal-weight portfolio)
# Tunable: fast_sma, slow_sma
# ═══════════════════════════════════════════════════════════════
def _sma_portfolio_simulate(conn, start, end, fast, slow, starting_eq=1.0):
    """Simulate SMA crossover portfolio for given date range and params."""
    bars = pd.read_sql_query("""
        SELECT ticker, date, adj_close FROM daily_bars
        WHERE date >= ? AND date <= ?
        ORDER BY ticker, date
    """, conn, params=(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
        parse_dates=["date"])

    if bars.empty:
        return pd.DataFrame(), -np.inf

    tickers = bars["ticker"].unique()
    n = len(tickers)
    all_daily = []

    for ticker in tickers:
        # Need extra lookback data for SMA calculation
        full = pd.read_sql_query("""
            SELECT date, adj_close FROM daily_bars
            WHERE ticker = ? AND date <= ? ORDER BY date
        """, conn, params=(ticker, end.strftime("%Y-%m-%d")), parse_dates=["date"])

        if len(full) < slow:
            continue

        full[f"sma_{fast}"] = full["adj_close"].rolling(fast).mean()
        full[f"sma_{slow}"] = full["adj_close"].rolling(slow).mean()
        full["daily_return"] = full["adj_close"].pct_change()

        # Generate signals
        full["signal"] = 0
        full.loc[full[f"sma_{fast}"] > full[f"sma_{slow}"], "signal"] = 1

        # Filter to test window
        test_mask = full["date"] >= start
        test = full[test_mask].copy()
        if test.empty:
            continue

        test["position"] = test["signal"].shift(1).fillna(0)
        test["wr"] = test["daily_return"] * test["position"] / n
        all_daily.append(test[["date", "wr"]])

    if not all_daily:
        return pd.DataFrame(), -np.inf

    combined = pd.concat(all_daily)
    port = combined.groupby("date")["wr"].sum().reset_index()
    port.columns = ["date", "daily_return"]
    port = port.sort_values("date")
    port["equity"] = starting_eq * (1 + port["daily_return"]).cumprod()

    sharpe = port["daily_return"].mean() / port["daily_return"].std() * np.sqrt(252) if port["daily_return"].std() > 0 else -np.inf
    return port, sharpe


def wfo_sma(conn):
    """WFO for SMA Crossover strategy."""
    dates_df = pd.read_sql_query(
        "SELECT DISTINCT date FROM daily_bars ORDER BY date", conn, parse_dates=["date"]
    )
    if dates_df.empty:
        return None

    all_dates = sorted(dates_df["date"].tolist())
    windows = _get_date_windows(all_dates)
    if not windows:
        return None

    candidates = [(10, 50), (20, 100), (30, 150), (50, 200), (20, 200)]
    oos_parts = []
    cum_eq = 1.0
    results = []

    for w in windows:
        best_params, best_s = (50, 200), -np.inf
        for fast, slow in candidates:
            _, s = _sma_portfolio_simulate(conn, w["train_start"], w["train_end"], fast, slow)
            if s > best_s:
                best_s, best_params = s, (fast, slow)

        oos_eq, _ = _sma_portfolio_simulate(conn, w["test_start"], w["test_end"],
                                             best_params[0], best_params[1], cum_eq)
        if oos_eq.empty:
            continue

        cum_eq = oos_eq["equity"].iloc[-1]
        oos_parts.append(oos_eq)
        results.append({
            "window": f"{w['test_start'].strftime('%Y-%m-%d')} → {w['test_end'].strftime('%Y-%m-%d')}",
            "best_param": f"fast={best_params[0]}, slow={best_params[1]}",
            "train_sharpe": round(best_s, 3),
            **_compute_metrics(oos_eq["daily_return"]),
        })

    if not oos_parts:
        return None

    stitched = pd.concat(oos_parts, ignore_index=True)
    return {
        "name": "SMA Crossover (EW)",
        "stitched": stitched,
        "windows": results,
        "overall": _compute_metrics(stitched["daily_return"]),
    }


# ═══════════════════════════════════════════════════════════════
# Strategy 4: Pullback RSI (equal-weight portfolio)
# Tunable: rsi_period, rsi_entry
# ═══════════════════════════════════════════════════════════════
def _rsi(series, period):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def _pullback_portfolio_simulate(conn, start, end, rsi_period, rsi_entry, starting_eq=1.0):
    """Simulate Pullback RSI portfolio for a specific window."""
    bars = pd.read_sql_query("""
        SELECT ticker, date, adj_close FROM daily_bars
        WHERE date <= ? ORDER BY ticker, date
    """, conn, params=(end.strftime("%Y-%m-%d"),), parse_dates=["date"])

    if bars.empty:
        return pd.DataFrame(), -np.inf

    tickers = bars["ticker"].unique()
    n = len(tickers)
    all_daily = []

    for ticker in tickers:
        full = bars[bars["ticker"] == ticker].copy().sort_values("date")
        if len(full) < 200:
            continue

        full["sma_200"] = full["adj_close"].rolling(200).mean()
        full["rsi"] = _rsi(full["adj_close"], rsi_period)
        full["daily_return"] = full["adj_close"].pct_change()

        # Pullback signals: above 200 SMA + RSI < entry
        full["in_position"] = 0
        pos = 0
        for idx in full.index:
            price = full.loc[idx, "adj_close"]
            sma = full.loc[idx, "sma_200"]
            r = full.loc[idx, "rsi"]
            if pd.isna(sma) or pd.isna(r):
                continue
            if pos == 0 and price > sma and r < rsi_entry:
                pos = 1
            elif pos == 1 and r > 70:  # Exit at RSI > 70
                pos = 0
            full.loc[idx, "in_position"] = pos

        test = full[full["date"] >= start].copy()
        if test.empty:
            continue

        test["position"] = test["in_position"].shift(1).fillna(0)
        test["wr"] = test["daily_return"] * test["position"] / n
        all_daily.append(test[["date", "wr"]])

    if not all_daily:
        return pd.DataFrame(), -np.inf

    combined = pd.concat(all_daily)
    port = combined.groupby("date")["wr"].sum().reset_index()
    port.columns = ["date", "daily_return"]
    port = port.sort_values("date")
    port["equity"] = starting_eq * (1 + port["daily_return"]).cumprod()

    sharpe = port["daily_return"].mean() / port["daily_return"].std() * np.sqrt(252) if port["daily_return"].std() > 0 else -np.inf
    return port, sharpe


def wfo_pullback(conn):
    """WFO for Pullback RSI strategy."""
    dates_df = pd.read_sql_query(
        "SELECT DISTINCT date FROM daily_bars ORDER BY date", conn, parse_dates=["date"]
    )
    if dates_df.empty:
        return None

    all_dates = sorted(dates_df["date"].tolist())
    windows = _get_date_windows(all_dates)
    if not windows:
        return None

    candidates = [(2, 10), (2, 20), (3, 15), (3, 20), (3, 30), (5, 25)]
    oos_parts = []
    cum_eq = 1.0
    results = []

    for w in windows:
        best_params, best_s = (3, 20), -np.inf
        for period, entry in candidates:
            _, s = _pullback_portfolio_simulate(conn, w["train_start"], w["train_end"], period, entry)
            if s > best_s:
                best_s, best_params = s, (period, entry)

        oos_eq, _ = _pullback_portfolio_simulate(conn, w["test_start"], w["test_end"],
                                                  best_params[0], best_params[1], cum_eq)
        if oos_eq.empty:
            continue

        cum_eq = oos_eq["equity"].iloc[-1]
        oos_parts.append(oos_eq)
        results.append({
            "window": f"{w['test_start'].strftime('%Y-%m-%d')} → {w['test_end'].strftime('%Y-%m-%d')}",
            "best_param": f"rsi_period={best_params[0]}, entry={best_params[1]}",
            "train_sharpe": round(best_s, 3),
            **_compute_metrics(oos_eq["daily_return"]),
        })

    if not oos_parts:
        return None

    stitched = pd.concat(oos_parts, ignore_index=True)
    return {
        "name": "Pullback RSI (EW)",
        "stitched": stitched,
        "windows": results,
        "overall": _compute_metrics(stitched["daily_return"]),
    }


# ═══════════════════════════════════════════════════════════════
# Run all WFO
# ═══════════════════════════════════════════════════════════════
def run_all_wfo():
    """Run WFO for all 4 strategies. Returns list of result dicts."""
    conn = sqlite3.connect(DB_PATH)
    results = []

    for fn in [wfo_ev_sales, wfo_ls_zscore, wfo_sma, wfo_pullback]:
        try:
            r = fn(conn)
            if r:
                results.append(r)
        except Exception as e:
            print(f"  ⚠ {fn.__name__} failed: {e}")

    conn.close()
    return results


if __name__ == "__main__":
    results = run_all_wfo()
    for r in results:
        print(f"\n{'='*50}")
        print(f"{r['name']}")
        print(f"  OOS Sharpe: {r['overall']['sharpe']:.3f}")
        print(f"  OOS MaxDD:  {r['overall']['max_drawdown']:.2%}")
        print(f"  OOS CAGR:   {r['overall']['cagr']:.2%}")
        for w in r["windows"]:
            print(f"    Window {w['window']}: param={w['best_param']}, "
                  f"train_sharpe={w['train_sharpe']}, oos_sharpe={w['sharpe']:.3f}")
