"""
portfolio_rebalancer.py — Level 2 Phase 4: Target Weight Portfolio Rebalancer

Translates target portfolio weights from cross_sectional_scores into
physical buy/sell orders by computing the delta between desired and
actual positions. Applies:
  - Concentration limits (max 10% per stock)
  - Cash buffer (5% minimum cash)
  - Liquidity gating (1% of 30-day ADV max order size)
"""

import sqlite3
import math
import pandas as pd
from datetime import datetime, timedelta
from src.config import (
    DB_PATH, MAX_SINGLE_WEIGHT, CASH_BUFFER, ADV_LOOKBACK, ADV_MAX_PCT
)
from src.pipeline.execution.portfolio_state import get_portfolio_state


def rebalance_portfolio():
    """
    Calculate rebalance orders by comparing target weights against
    current holdings. Returns a list of order dicts.

    Returns:
        list of {ticker, action, quantity, price, target_weight}
    """
    print("=" * 60)
    print("PHASE 4: Portfolio Rebalancer")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # ── Step 1: Load today's target weights ──────────────────
    print("  Loading target weights...", end=" ")
    targets = pd.read_sql_query("""
        SELECT ticker, target_weight
        FROM cross_sectional_scores
        WHERE date = (SELECT MAX(date) FROM cross_sectional_scores)
    """, conn)

    if targets.empty:
        print("⚠ No cross-sectional scores found. Run Phase 2 first.")
        conn.close()
        return []

    latest_date = pd.read_sql_query(
        "SELECT MAX(date) as d FROM cross_sectional_scores", conn
    )["d"].iloc[0]
    print(f"✓ {len(targets)} tickers (date: {latest_date})")

    # ── Step 2: Get current portfolio state ──────────────────
    print("  Getting portfolio state...", end=" ")
    total_equity, holdings = get_portfolio_state()
    print(f"✓ Equity=${total_equity:,.2f}, {len(holdings)} positions")

    # ── Step 3: Apply concentration limits ───────────────────
    print("  Applying concentration limits...", end=" ")
    targets["target_weight"] = targets["target_weight"].clip(upper=MAX_SINGLE_WEIGHT)

    # Enforce cash buffer
    total_weight = targets["target_weight"].sum()
    max_total = 1.0 - CASH_BUFFER
    if total_weight > max_total:
        scale = max_total / total_weight
        targets["target_weight"] *= scale
        print(f"scaled by {scale:.3f} ", end="")
    print(f"✓ Total weight: {targets['target_weight'].sum():.3f}")

    # ── Step 4: Get current prices ───────────────────────────
    prices = pd.read_sql_query("""
        SELECT ticker, adj_close as price
        FROM daily_bars
        WHERE date = (SELECT MAX(date) FROM daily_bars)
    """, conn)
    price_map = dict(zip(prices["ticker"], prices["price"]))

    # ── Step 5: Calculate the diff ───────────────────────────
    print("  Calculating order deltas...", end=" ")
    orders = []

    # Process target weight tickers (BUY or adjust)
    for _, row in targets.iterrows():
        ticker = row["ticker"]
        target_weight = row["target_weight"]
        price = price_map.get(ticker)

        if price is None or price <= 0:
            continue

        target_shares = math.floor((total_equity * target_weight) / price)
        current_shares = holdings.get(ticker, {}).get("shares", 0)
        delta = target_shares - current_shares

        if delta == 0:
            continue

        action = "BUY" if delta > 0 else "SELL"
        quantity = abs(delta)

        orders.append({
            "ticker": ticker,
            "action": action,
            "quantity": quantity,
            "price": price,
            "target_weight": target_weight,
        })

    # Liquidate positions not in targets
    target_tickers = set(targets["ticker"].tolist())
    for ticker, info in holdings.items():
        if ticker not in target_tickers and info["shares"] > 0:
            price = price_map.get(ticker, info.get("avg_price", 0))
            orders.append({
                "ticker": ticker,
                "action": "SELL",
                "quantity": info["shares"],
                "price": price,
                "target_weight": 0.0,
            })

    print(f"✓ {len(orders)} orders")

    # ── Step 6: Apply ADV liquidity gating ───────────────────
    if orders:
        print("  Applying ADV liquidity gating...", end=" ")
        gated = 0
        cutoff_date = (datetime.now() - timedelta(days=ADV_LOOKBACK + 10)).strftime("%Y-%m-%d")

        for order in orders:
            adv_result = pd.read_sql_query("""
                SELECT AVG(volume) as adv
                FROM (
                    SELECT volume FROM daily_bars
                    WHERE ticker = ? AND date >= ?
                    ORDER BY date DESC
                    LIMIT ?
                )
            """, conn, params=(order["ticker"], cutoff_date, ADV_LOOKBACK))

            if not adv_result.empty and adv_result["adv"].iloc[0] is not None:
                adv = adv_result["adv"].iloc[0]
                max_trade = math.floor(adv * ADV_MAX_PCT)

                if order["quantity"] > max_trade and max_trade > 0:
                    original = order["quantity"]
                    order["quantity"] = max_trade
                    gated += 1
                    print(f"\n    ⚠ {order['ticker']}: {original} → {max_trade} (1% ADV)", end="")

        print(f"  ✓ {gated} orders gated")

    # ── Sort: SELLs first ────────────────────────────────────
    orders.sort(key=lambda x: (0 if x["action"] == "SELL" else 1, x["ticker"]))

    conn.close()

    # ── Print summary ────────────────────────────────────────
    if orders:
        print()
        print(f"  {'ACTION':<6} {'TICKER':<8} {'QTY':>6} {'PRICE':>10} {'WEIGHT':>8}")
        print(f"  {'─' * 6} {'─' * 8} {'─' * 6} {'─' * 10} {'─' * 8}")
        for o in orders:
            print(f"  {o['action']:<6} {o['ticker']:<8} {o['quantity']:>6} "
                  f"${o['price']:>9.2f} {o['target_weight']:>7.1%}")
    else:
        print("  ✓ Portfolio already at target. No orders needed.")

    print()
    print(f"  ✓ {len(orders)} orders ready for execution")
    print()

    return orders


if __name__ == "__main__":
    orders = rebalance_portfolio()
