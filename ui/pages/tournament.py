"""WFO Tournament — Strategy comparison across all 5 strategies."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.config import SLIPPAGE_BPS, COMMISSION_PER_SHARE, MAX_SINGLE_WEIGHT, CASH_BUFFER
from src.pipeline import strategy_tournament

cfg = render_sidebar()

st.markdown("# 🏆 Strategy Tournament")
st.caption("Compare all strategies on equal footing — portfolio-level backtests with $10,000 starting capital")
st.divider()

has_data = table_exists("daily_bars")
has_xs = table_exists("cross_sectional_scores")
has_sma = table_exists("strategy_signals")
has_pb = table_exists("pullback_signals")

if not has_data:
    st.info("ℹ️ No data yet. Run the pipeline first.")
else:
    # Strategy config
    col1, col2 = st.columns(2)
    with col1:
        n_long = st.number_input("L/S: Stocks to LONG", min_value=1, max_value=10, value=2, step=1, key="t_n_long")
    with col2:
        n_short = st.number_input("L/S: Stocks to SHORT", min_value=1, max_value=10, value=2, step=1, key="t_n_short")

    if st.button("🏆 Run Tournament", type="primary", use_container_width=True):
        with st.spinner("Running all strategies..."):
            results = strategy_tournament.run_tournament(n_long=int(n_long), n_short=int(n_short))
        st.success(f"✅ Tournament complete — {len(results)} strategies evaluated!")
        st.session_state["tournament_results"] = results
        st.rerun()

    # Check for cached results or load from DB
    results = st.session_state.get("tournament_results")

    if results is None:
        # Try to run on-the-fly if data exists
        conn = get_db_connection()
        any_results = False

        strats = {}
        # Buy & Hold baseline
        try:
            eq, met = strategy_tournament.run_buyhold_portfolio(conn)
            if not eq.empty:
                strats["Buy & Hold (EW)"] = (eq, met)
        except Exception:
            pass
        if has_xs:
            try:
                eq, met = strategy_tournament.run_ev_sales_longonly(conn)
                if not eq.empty:
                    strats["EV/Sales Long-Only"] = (eq, met)
            except Exception:
                pass
            try:
                eq, met = strategy_tournament.run_ls_zscore(n_long=int(n_long), n_short=int(n_short))
                if not eq.empty:
                    strats["L/S Z-Score"] = (eq, met)
            except Exception:
                pass
        if has_sma:
            try:
                eq, met = strategy_tournament.run_sma_portfolio(conn)
                if not eq.empty:
                    strats["SMA Crossover (EW)"] = (eq, met)
            except Exception:
                pass
        if has_pb:
            try:
                eq, met = strategy_tournament.run_pullback_portfolio(conn)
                if not eq.empty:
                    strats["Pullback RSI (EW)"] = (eq, met)
            except Exception:
                pass

        conn.close()
        if strats:
            results = strats

    if results:
        # ── Metrics Table ────────────────────────────────────
        st.markdown("### 📊 Strategy Comparison")
        rows = []
        for name, (eq_df, metrics) in results.items():
            rows.append({
                "Strategy": name,
                "Total Return": f"{metrics['total_return']:+.2%}",
                "Sharpe": f"{metrics['sharpe']:.2f}",
                "Max Drawdown": f"{metrics['max_drawdown']:.2%}",
                "CAGR": f"{metrics['cagr']:.2%}",
                "Days": metrics["trading_days"],
            })

        df_metrics = pd.DataFrame(rows)
        st.dataframe(df_metrics, use_container_width=True, hide_index=True)

        # ── Metric Cards ─────────────────────────────────────
        # Find the best for each metric
        all_metrics = {name: met for name, (_, met) in results.items()}
        best_return = max(all_metrics, key=lambda x: all_metrics[x]["total_return"])
        best_sharpe = max(all_metrics, key=lambda x: all_metrics[x]["sharpe"])
        lowest_dd = min(all_metrics, key=lambda x: all_metrics[x]["max_drawdown"])

        c1, c2, c3 = st.columns(3)
        c1.metric("🥇 Best Return", best_return,
                  delta=f"{all_metrics[best_return]['total_return']:+.2%}")
        c2.metric("🥇 Best Sharpe", best_sharpe,
                  delta=f"{all_metrics[best_sharpe]['sharpe']:.2f}")
        c3.metric("🥇 Lowest MaxDD", lowest_dd,
                  delta=f"{all_metrics[lowest_dd]['max_drawdown']:.2%}")

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

        # Add SPY benchmark
        conn = get_db_connection()
        spy = pd.read_sql_query("""
            SELECT date, adj_close FROM daily_bars
            WHERE ticker = 'SPY' ORDER BY date
        """, conn, parse_dates=["date"])
        conn.close()

        if not spy.empty:
            # Align SPY to the earliest strategy start
            earliest = min(eq["date"].min() for eq, _ in results.values())
            spy = spy[spy["date"] >= earliest]
            spy["daily_return"] = spy["adj_close"].pct_change()
            spy["equity"] = 10000 * (1 + spy["daily_return"].fillna(0)).cumprod()
            fig.add_trace(go.Scatter(
                x=spy["date"], y=spy["equity"],
                name="SPY (Benchmark)",
                line=dict(color="gray", width=2, dash="dash"),
            ))

        for name, (eq_df, _) in results.items():
            eq_df = eq_df.copy()
            eq_df["date"] = pd.to_datetime(eq_df["date"])
            fig.add_trace(go.Scatter(
                x=eq_df["date"], y=eq_df["equity"],
                name=name,
                line=dict(color=colors.get(name, "#FFFFFF"), width=2.5),
            ))

        fig.update_layout(
            template="plotly_dark", height=550,
            yaxis_title="Portfolio Value ($)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Friction Parameters ──────────────────────────────
        st.divider()
        st.markdown("### ⚙️ Backtest Parameters")
        f1, f2, f3, f4 = st.columns(4)
        f1.metric("Slippage", f"{SLIPPAGE_BPS*10000:.1f} bps")
        f2.metric("Commission", f"${COMMISSION_PER_SHARE}/share")
        f3.metric("Max Weight", f"{MAX_SINGLE_WEIGHT:.0%}")
        f4.metric("Cash Buffer", f"{CASH_BUFFER:.0%}")

    else:
        st.info("ℹ️ Click **Run Tournament** to compare all strategies.")
