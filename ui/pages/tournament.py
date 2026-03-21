"""Strategy Tournament — Compare all 5 strategies over identical trading days."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.config import SLIPPAGE_BPS, COMMISSION_PER_SHARE, MAX_SINGLE_WEIGHT, CASH_BUFFER
from src.pipeline import strategy_tournament

cfg = render_sidebar()

st.markdown("# 🏆 Strategy Tournament")
st.caption("Compare all strategies over the exact same trading days — $10,000 starting capital")
st.divider()

has_data = table_exists("daily_bars")
has_xs = table_exists("cross_sectional_scores")
has_sma = table_exists("strategy_signals")
has_pb = table_exists("pullback_signals")

if not has_data:
    st.info("ℹ️ No data yet. Run the pipeline first.")
else:
    # ── Get actual trading days from the database ────────────
    conn = get_db_connection()
    all_trading_dates = pd.read_sql_query(
        "SELECT DISTINCT date FROM daily_bars WHERE ticker != 'SPY' ORDER BY date",
        conn, parse_dates=["date"]
    )["date"].tolist()
    conn.close()

    total_available = len(all_trading_dates)

    # ── Settings ────────────────────────────────────────────
    row1_c1, row1_c2 = st.columns(2)
    with row1_c1:
        period_label = st.selectbox(
            "📅 Trading Period (measurement window)",
            ["63 Days (~3 months)", "126 Days (~6 months)", "252 Days (~1 year)", f"All ({total_available} days)"],
            index=2,
            key="t_period",
        )
    with row1_c2:
        lookback_label = st.selectbox(
            "🔍 Lookback Period (indicator warm-up)",
            ["200 Days (SMA-200)", "400 Days", "600 Days"],
            index=0,
            key="t_lookback",
            help="Extra data before the trading period for indicator warm-up (SMA, RSI, Z-scores, etc.)",
        )

    row2_c1, row2_c2 = st.columns(2)
    with row2_c1:
        n_long = st.number_input("L/S: Long", min_value=1, max_value=10, value=2, step=1, key="t_n_long")
    with row2_c2:
        n_short = st.number_input("L/S: Short", min_value=1, max_value=10, value=2, step=1, key="t_n_short")

    # Parse settings
    period_map = {"63": 63, "126": 126, "252": 252, "All": None}
    selected_days = None
    for key in period_map:
        if period_label.startswith(key):
            selected_days = period_map[key]
            break

    lookback_days = int(lookback_label.split(" ")[0])

    # Determine the exact trading dates for the TRADING window
    if selected_days and selected_days <= total_available:
        eval_dates = set(all_trading_dates[-selected_days:])
        eval_start = all_trading_dates[-selected_days]
        eval_end = all_trading_dates[-1]
        target_days = selected_days
    else:
        eval_dates = set(all_trading_dates)
        eval_start = all_trading_dates[0]
        eval_end = all_trading_dates[-1]
        target_days = total_available

    # Show data requirements
    total_needed = target_days + lookback_days
    data_sufficient = total_available >= total_needed
    if data_sufficient:
        st.caption(f"✅ Data requirement: **{target_days}** trading + **{lookback_days}** lookback = "
                  f"**{total_needed}** days needed — you have **{total_available}** available")
    else:
        st.warning(f"⚠️ Data requirement: **{target_days}** trading + **{lookback_days}** lookback = "
                  f"**{total_needed}** days needed — but you only have **{total_available}**. "
                  f"Some strategies may not have fully warmed-up indicators.")

    if st.button("🏆 Run Tournament", type="primary", use_container_width=True):
        with st.spinner("Running all strategies..."):
            results = strategy_tournament.run_tournament(n_long=int(n_long), n_short=int(n_short))
        st.session_state["tournament_results"] = results
        st.rerun()

    # ── Load results ─────────────────────────────────────────
    results = st.session_state.get("tournament_results")

    if results is None:
        conn = get_db_connection()
        strats = {}
        try:
            eq, met = strategy_tournament.run_buyhold_portfolio(conn)
            if not eq.empty: strats["Buy & Hold (EW)"] = (eq, met)
        except Exception: pass
        if has_xs:
            try:
                eq, met = strategy_tournament.run_ev_sales_longonly(conn)
                if not eq.empty: strats["EV/Sales Long-Only"] = (eq, met)
            except Exception: pass
            try:
                eq, met = strategy_tournament.run_ls_zscore(n_long=int(n_long), n_short=int(n_short))
                if not eq.empty: strats["L/S Z-Score"] = (eq, met)
            except Exception: pass
        if has_sma:
            try:
                eq, met = strategy_tournament.run_sma_portfolio(conn)
                if not eq.empty: strats["SMA Crossover (EW)"] = (eq, met)
            except Exception: pass
        if has_pb:
            try:
                eq, met = strategy_tournament.run_pullback_portfolio(conn)
                if not eq.empty: strats["Pullback RSI (EW)"] = (eq, met)
            except Exception: pass
        conn.close()
        if strats:
            results = strats

    if results:
        # ── Trim all strategies to the exact same trading dates ─
        trimmed_results = {}
        for name, (eq_df, _) in results.items():
            eq = eq_df.copy()
            eq["date"] = pd.to_datetime(eq["date"])

            # Filter to only the target trading dates
            eq_window = eq[eq["date"].isin(eval_dates)].copy()

            if eq_window.empty or len(eq_window) < 2:
                continue

            # Rebase equity to $10,000 from the window start
            eq_window = eq_window.sort_values("date").reset_index(drop=True)
            eq_window["equity"] = 10000 * (1 + eq_window["daily_return"].fillna(0)).cumprod()

            actual_days = len(eq_window)
            has_full_data = actual_days >= target_days * 0.95  # within 5% of target

            # Recompute metrics over this window
            dr = eq_window["daily_return"].fillna(0)
            sharpe = dr.mean() / dr.std() * np.sqrt(252) if dr.std() > 0 else 0
            running_max = eq_window["equity"].expanding().max()
            max_dd = (1 - eq_window["equity"] / running_max).max()
            total_ret = eq_window["equity"].iloc[-1] / 10000 - 1
            cagr = (1 + total_ret) ** (252 / max(actual_days, 1)) - 1

            trimmed_results[name] = {
                "eq": eq_window,
                "total_return": total_ret,
                "sharpe": sharpe,
                "max_drawdown": max_dd,
                "cagr": cagr,
                "days": actual_days,
                "target_days": target_days,
                "full_data": has_full_data,
            }

        # ── Metrics Table ────────────────────────────────────
        st.markdown("### 📊 Strategy Comparison")
        st.caption(f"All strategies evaluated over **{target_days} trading days**: "
                  f"{eval_start.strftime('%Y-%m-%d')} → {eval_end.strftime('%Y-%m-%d')}")

        rows = []
        for name, m in trimmed_results.items():
            flag = "" if m["full_data"] else " 🚩"
            rows.append({
                "Strategy": name + flag,
                "Total Return": f"{m['total_return']:+.2%}",
                "Sharpe": f"{m['sharpe']:.2f}",
                "Max Drawdown": f"{m['max_drawdown']:.2%}",
                "CAGR": f"{m['cagr']:.2%}",
                "Trading Days": f"{m['days']}/{target_days}",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        # Flag explanation
        flagged = [n for n, m in trimmed_results.items() if not m["full_data"]]
        if flagged:
            st.warning(f"🚩 **Incomplete data:** {', '.join(flagged)} — "
                      f"missing trading days within the {target_days}-day window. "
                      f"Their equity curves start later than other strategies.")

        # ── Winner Cards ─────────────────────────────────────
        best_return = max(trimmed_results, key=lambda x: trimmed_results[x]["total_return"])
        best_sharpe = max(trimmed_results, key=lambda x: trimmed_results[x]["sharpe"])
        lowest_dd = min(trimmed_results, key=lambda x: trimmed_results[x]["max_drawdown"])

        c1, c2, c3 = st.columns(3)
        c1.metric("🥇 Best Return", best_return,
                  delta=f"{trimmed_results[best_return]['total_return']:+.2%}")
        c2.metric("🥇 Best Sharpe", best_sharpe,
                  delta=f"{trimmed_results[best_sharpe]['sharpe']:.2f}")
        c3.metric("🥇 Lowest MaxDD", lowest_dd,
                  delta=f"{trimmed_results[lowest_dd]['max_drawdown']:.2%}")

        # ── Equity Curves ────────────────────────────────────
        st.divider()
        st.markdown("### 📈 Equity Curves ($10,000)")

        colors = {
            "Buy & Hold (EW)": "#9E9E9E",
            "EV/Sales Long-Only": "#2196F3",
            "L/S Z-Score": "#E040FB",
            "SMA Crossover (EW)": "#FF9800",
            "Pullback RSI (EW)": "#00BCD4",
        }

        fig = go.Figure()

        # SPY benchmark (trimmed to same trading days)
        conn = get_db_connection()
        spy = pd.read_sql_query("""
            SELECT date, adj_close FROM daily_bars
            WHERE ticker = 'SPY' ORDER BY date
        """, conn, parse_dates=["date"])
        conn.close()

        if not spy.empty:
            spy = spy[spy["date"].isin(eval_dates)]
            spy["daily_return"] = spy["adj_close"].pct_change()
            spy["equity"] = 10000 * (1 + spy["daily_return"].fillna(0)).cumprod()
            fig.add_trace(go.Scatter(
                x=spy["date"], y=spy["equity"],
                name="SPY (Benchmark)",
                line=dict(color="gray", width=2, dash="dash"),
            ))

        for name, m in trimmed_results.items():
            eq = m["eq"]
            line_style = dict(color=colors.get(name, "#FFFFFF"), width=2.5)
            if not m["full_data"]:
                line_style["dash"] = "dot"
            fig.add_trace(go.Scatter(
                x=eq["date"], y=eq["equity"],
                name=name + (" 🚩" if not m["full_data"] else ""),
                line=line_style,
            ))

        fig.update_layout(
            template="plotly_dark", height=550,
            yaxis_title="Portfolio Value ($)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Backtest Parameters ──────────────────────────────
        st.divider()
        st.markdown("### ⚙️ Backtest Parameters")
        f1, f2, f3, f4 = st.columns(4)
        f1.metric("Slippage", f"{SLIPPAGE_BPS*10000:.1f} bps")
        f2.metric("Commission", f"${COMMISSION_PER_SHARE}/share")
        f3.metric("Max Weight", f"{MAX_SINGLE_WEIGHT:.0%}")
        f4.metric("Cash Buffer", f"{CASH_BUFFER:.0%}")

    else:
        st.info("ℹ️ Click **Run Tournament** to compare all strategies.")
