"""
ui/app.py — Level 2 Quant Sandbox: Streamlit Control Center

Interactive dashboard to control and visualize the trading pipeline.
Run with: streamlit run ui/app.py  (from project root)

Tabs:
  1. Data & Signals   — Ingest data, compute signals, preview tables
  2. Charts & Simulation — Interactive Plotly charts with SMA overlays, PnL
  3. Strategy Comparison — Compare SMA Crossover vs Pullback strategy
  4. Execution Desk   — Route paper trades, view execution ledger
  5. Cross-Sectional Scores — EV/Sales Z-score rankings (Level 2)
  6. WFO Tournament   — Walk-Forward Optimization results (Level 2)
  7. Portfolio Rebalancer — Target weights vs current positions (Level 2)
"""

import sys
import os

# Ensure project root is on the path so src imports work
# ui/app.py lives at <project>/ui/app.py, so project root is one level up
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import math
from datetime import datetime, timedelta

from src.config import (
    DB_PATH, ZSCORE_BUY_THRESHOLD, SLIPPAGE_BPS, COMMISSION_PER_SHARE,
    MAX_SINGLE_WEIGHT, CASH_BUFFER
)
from src.pipeline import (
    db_init, data_ingestion, fundamental_ingestion,
    cross_sectional_scoring, wfo_backtester,
    portfolio_rebalancer, simulation, execution,
)
from src.strategies import strategy, pullback_strategy


# ═════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Level 2 — Quant Sandbox",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ═════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════
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


# ═════════════════════════════════════════════════════════════
# SIDEBAR — Configuration
# ═════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.divider()

    api_key = os.getenv("ALPACA_API_KEY", "").strip()
    api_secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
    if api_key and api_secret:
        st.success("🟢 Alpaca API Keys Loaded")
    else:
        st.warning("🟡 Alpaca Keys Not Set (Dry-Run Mode)")

    st.divider()

    st.markdown("### 🌐 Ticker Universe")
    universe_input = st.text_area(
        "Tickers (comma-separated)",
        value="AAPL, MSFT, GOOGL, AMZN, TSLA, SPY, QQQ, GLD, META, NVDA",
        height=80,
    )
    universe_list = [t.strip().upper() for t in universe_input.split(",") if t.strip()]
    st.caption(f"{len(universe_list)} tickers configured")

    st.divider()

    st.markdown("### 📊 Strategy Parameters")
    st.markdown("**SMA Crossover**")
    fast_sma = st.number_input("Fast SMA Window", min_value=5, max_value=100, value=50, step=5)
    slow_sma = st.number_input("Slow SMA Window", min_value=50, max_value=500, value=200, step=10)

    st.markdown("**Pullback Strategy**")
    rsi_period = st.number_input("RSI Period", min_value=2, max_value=14, value=3, step=1)
    rsi_entry = st.number_input("RSI Entry (Oversold)", min_value=5, max_value=40, value=20, step=5)
    rsi_exit = st.number_input("RSI Exit (Overbought)", min_value=50, max_value=90, value=70, step=5)

    st.divider()

    st.markdown("### 🛡️ Risk Limits")
    capital_per_trade = st.number_input("Capital Per Trade ($)", min_value=100, max_value=10000, value=1000, step=100)
    max_positions = st.number_input("Max Open Positions", min_value=1, max_value=20, value=5, step=1)

    st.divider()

    st.markdown("### 💾 Database Status")
    db_exists = os.path.exists(DB_PATH)
    if db_exists:
        st.success("🟢 DB exists")
        col1, col2 = st.columns(2)
        col1.metric("Daily Bars", get_table_count("daily_bars"))
        col2.metric("SMA Signals", get_table_count("strategy_signals"))
        col3, col4 = st.columns(2)
        col3.metric("Fundamentals", get_table_count("quarterly_fundamentals"))
        col4.metric("XS Scores", get_table_count("cross_sectional_scores"))
        col5, col6 = st.columns(2)
        col5.metric("WFO Results", get_table_count("wfo_results"))
        col6.metric("Executions", get_table_count("paper_executions"))
    else:
        st.info("🔵 DB not initialized yet")


# ═════════════════════════════════════════════════════════════
# MAIN AREA
# ═════════════════════════════════════════════════════════════
st.markdown("# 🧪 Level 2: Quant Sandbox")
st.markdown("*Cross-Sectional Fundamental Ranking — Mission Control*")


tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📥 Data & Signals",
    "📊 Charts & Simulation",
    "⚔️ Strategy Comparison",
    "🚀 Execution Desk",
    "🧬 Cross-Sectional Scores",
    "🏆 WFO Tournament",
    "⚖️ Portfolio Rebalancer",
])


# ─────────────────────────────────────────────────────────────
# TAB 1: Data & Signals
# ─────────────────────────────────────────────────────────────
with tab1:
    st.markdown("## Phase 1 & 2: Data Ingestion & Signal Generation")
    st.divider()

    col_btn1, col_btn2 = st.columns(2)

    with col_btn1:
        if st.button("🗃️ Initialize Database", use_container_width=True):
            with st.spinner("Creating database tables..."):
                db_init.init_db()
            st.success("✅ Database initialized!")
            st.rerun()

    with col_btn2:
        if st.button("📥 Run Phase 1a: Ingest Prices", use_container_width=True):
            with st.spinner("Fetching EOD data from yfinance..."):
                data_ingestion.UNIVERSE = universe_list
                data_ingestion.ingest()
            st.success(f"✅ Prices ingested for {len(universe_list)} tickers!")
            st.rerun()

    col_btn3, col_btn4 = st.columns(2)

    with col_btn3:
        if st.button("📥 Run Phase 1b: Ingest Fundamentals", use_container_width=True):
            if not table_exists("daily_bars"):
                st.error("❌ No data in daily_bars. Run Phase 1a first!")
            else:
                with st.spinner("Fetching quarterly financials..."):
                    fundamental_ingestion.ingest_fundamentals(tickers=universe_list)
                st.success("✅ Quarterly fundamentals ingested!")
                st.rerun()

    with col_btn4:
        if st.button("🧮 Run Phase 2: Cross-Sectional Scores", use_container_width=True):
            if not table_exists("quarterly_fundamentals"):
                st.error("❌ No fundamentals. Run Phase 1b first!")
            else:
                with st.spinner("Computing EV/Sales Z-scores..."):
                    cross_sectional_scoring.compute_cross_sectional_scores()
                st.success("✅ Cross-sectional scores computed!")
                st.rerun()

    col_btn5, col_btn6 = st.columns(2)

    with col_btn5:
        if st.button("🧮 SMA Crossover Signals", use_container_width=True):
            if not table_exists("daily_bars"):
                st.error("❌ No data in daily_bars. Run Phase 1a first!")
            else:
                with st.spinner("Computing SMA crossover signals..."):
                    strategy.compute_signals()
                st.success("✅ SMA signals computed!")
                st.rerun()

    with col_btn6:
        if st.button("🎯 Pullback Strategy Signals", use_container_width=True):
            if not table_exists("daily_bars"):
                st.error("❌ No data in daily_bars. Run Phase 1a first!")
            else:
                with st.spinner("Computing pullback strategy signals..."):
                    pullback_strategy.RSI_PERIOD = int(rsi_period)
                    pullback_strategy.RSI_ENTRY_THRESHOLD = int(rsi_entry)
                    pullback_strategy.RSI_EXIT_THRESHOLD = int(rsi_exit)
                    pullback_strategy.compute_pullback_signals()
                st.success("✅ Pullback signals computed!")
                st.rerun()

    st.divider()

    # Data Previews
    if table_exists("daily_bars"):
        with st.expander("📋 Preview: daily_bars", expanded=False):
            conn = get_db_connection()
            st.dataframe(pd.read_sql_query(
                "SELECT * FROM daily_bars ORDER BY date DESC LIMIT 100", conn
            ), use_container_width=True, height=400)
            conn.close()

    if table_exists("quarterly_fundamentals"):
        with st.expander("📋 Preview: quarterly_fundamentals", expanded=False):
            conn = get_db_connection()
            st.dataframe(pd.read_sql_query(
                "SELECT * FROM quarterly_fundamentals ORDER BY filing_date DESC LIMIT 100", conn
            ), use_container_width=True, height=400)
            conn.close()

    if table_exists("cross_sectional_scores"):
        with st.expander("📋 Preview: cross_sectional_scores", expanded=False):
            conn = get_db_connection()
            st.dataframe(pd.read_sql_query(
                "SELECT * FROM cross_sectional_scores ORDER BY date DESC, ev_sales_zscore ASC LIMIT 100", conn
            ), use_container_width=True, height=400)
            conn.close()

    if table_exists("strategy_signals"):
        with st.expander("📋 Preview: strategy_signals", expanded=False):
            conn = get_db_connection()
            st.dataframe(pd.read_sql_query(
                "SELECT * FROM strategy_signals ORDER BY date DESC LIMIT 100", conn
            ), use_container_width=True, height=400)
            conn.close()

    if not table_exists("daily_bars"):
        st.info("ℹ️ Click 'Initialize Database' then 'Ingest Prices' to get started.")


# ─────────────────────────────────────────────────────────────
# TAB 2: Charts & Simulation
# ─────────────────────────────────────────────────────────────
with tab2:
    st.markdown("## Charts & Simulation")
    st.divider()

    if not table_exists("daily_bars"):
        st.info("ℹ️ No data yet. Run Phase 1 first.")
    else:
        conn = get_db_connection()

        available_tickers = pd.read_sql_query(
            "SELECT DISTINCT ticker FROM daily_bars ORDER BY ticker", conn
        )["ticker"].tolist()

        col_sel, col_strat = st.columns([1, 1])
        with col_sel:
            selected_ticker = st.selectbox("Select Ticker", available_tickers, index=0)
        with col_strat:
            strategy_view = st.selectbox("Strategy Overlay", ["SMA Crossover", "Pullback (RSI)", "Both"])

        if selected_ticker:
            df_chart = pd.read_sql_query("""
                SELECT date, open, high, low, close, adj_close, volume
                FROM daily_bars WHERE ticker = ? ORDER BY date
            """, conn, params=(selected_ticker,))
            df_chart["date"] = pd.to_datetime(df_chart["date"])

            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True,
                vertical_spacing=0.03, row_heights=[0.75, 0.25],
                subplot_titles=[f"{selected_ticker} — Price", "RSI (3-day)"]
            )

            fig.add_trace(go.Candlestick(
                x=df_chart["date"], open=df_chart["open"], high=df_chart["high"],
                low=df_chart["low"], close=df_chart["close"], name="OHLC",
                increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
            ), row=1, col=1)

            # SMA overlay
            if strategy_view in ["SMA Crossover", "Both"] and table_exists("strategy_signals"):
                df_sma = pd.read_sql_query(
                    "SELECT date, sma_50, sma_200, signal FROM strategy_signals WHERE ticker = ? ORDER BY date",
                    conn, params=(selected_ticker,))
                df_sma["date"] = pd.to_datetime(df_sma["date"])

                fig.add_trace(go.Scatter(x=df_sma["date"], y=df_sma["sma_50"],
                    name=f"SMA {int(fast_sma)}", line=dict(color="#2196F3", width=2)), row=1, col=1)
                fig.add_trace(go.Scatter(x=df_sma["date"], y=df_sma["sma_200"],
                    name=f"SMA {int(slow_sma)}", line=dict(color="#FF9800", width=2)), row=1, col=1)

                buys = df_sma[df_sma["signal"] == 1].merge(df_chart[["date", "close"]], on="date")
                if not buys.empty:
                    fig.add_trace(go.Scatter(x=buys["date"], y=buys["close"] * 0.97,
                        mode="markers", name="SMA BUY",
                        marker=dict(symbol="triangle-up", size=14, color="#00E676",
                                    line=dict(width=1, color="#004D40"))), row=1, col=1)

                sells = df_sma[df_sma["signal"] == -1].merge(df_chart[["date", "close"]], on="date")
                if not sells.empty:
                    fig.add_trace(go.Scatter(x=sells["date"], y=sells["close"] * 1.03,
                        mode="markers", name="SMA SELL",
                        marker=dict(symbol="triangle-down", size=14, color="#FF1744",
                                    line=dict(width=1, color="#B71C1C"))), row=1, col=1)

            # Pullback overlay
            if strategy_view in ["Pullback (RSI)", "Both"] and table_exists("pullback_signals"):
                df_pb = pd.read_sql_query(
                    "SELECT date, sma_200, rsi_3, signal, exit_signal FROM pullback_signals WHERE ticker = ? ORDER BY date",
                    conn, params=(selected_ticker,))
                df_pb["date"] = pd.to_datetime(df_pb["date"])

                if strategy_view == "Pullback (RSI)":
                    fig.add_trace(go.Scatter(x=df_pb["date"], y=df_pb["sma_200"],
                        name="SMA 200 (Trend)", line=dict(color="#FF9800", width=2, dash="dash")), row=1, col=1)

                pb_entries = df_pb[df_pb["signal"] == 1.0].merge(df_chart[["date", "close"]], on="date")
                if not pb_entries.empty:
                    fig.add_trace(go.Scatter(x=pb_entries["date"], y=pb_entries["close"] * 0.96,
                        mode="markers", name="PB ENTRY",
                        marker=dict(symbol="diamond", size=12, color="#00BCD4",
                                    line=dict(width=1, color="#006064"))), row=1, col=1)

                pb_tp = df_pb[df_pb["exit_signal"] == "TAKE_PROFIT"].merge(df_chart[["date", "close"]], on="date")
                if not pb_tp.empty:
                    fig.add_trace(go.Scatter(x=pb_tp["date"], y=pb_tp["close"] * 1.04,
                        mode="markers", name="PB TAKE PROFIT",
                        marker=dict(symbol="star", size=12, color="#76FF03",
                                    line=dict(width=1, color="#33691E"))), row=1, col=1)

                pb_sl = df_pb[df_pb["exit_signal"] == "STOP_LOSS"].merge(df_chart[["date", "close"]], on="date")
                if not pb_sl.empty:
                    fig.add_trace(go.Scatter(x=pb_sl["date"], y=pb_sl["close"] * 1.04,
                        mode="markers", name="PB STOP LOSS",
                        marker=dict(symbol="x", size=12, color="#FF5252",
                                    line=dict(width=1, color="#B71C1C"))), row=1, col=1)

                fig.add_trace(go.Scatter(x=df_pb["date"], y=df_pb["rsi_3"],
                    name="RSI(3)", line=dict(color="#AB47BC", width=1.5)), row=2, col=1)
                fig.add_hline(y=rsi_entry, line_dash="dash", line_color="green",
                              annotation_text=f"Oversold ({rsi_entry})", row=2, col=1)
                fig.add_hline(y=rsi_exit, line_dash="dash", line_color="red",
                              annotation_text=f"Overbought ({rsi_exit})", row=2, col=1)

            fig.update_layout(template="plotly_dark", height=700,
                xaxis_rangeslider_visible=False,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(t=40))
            fig.update_yaxes(title_text="Price ($)", row=1, col=1)
            fig.update_yaxes(title_text="RSI", row=2, col=1)

            st.plotly_chart(fig, use_container_width=True)

            # ── Simulation Metrics ───────────────────────────
            st.divider()

            if strategy_view in ["SMA Crossover", "Both"] and table_exists("strategy_signals"):
                st.markdown("### 📈 SMA Crossover Simulation")
                df_sim = pd.read_sql_query("""
                    SELECT s.date, b.adj_close, s.signal
                    FROM strategy_signals s
                    JOIN daily_bars b ON s.ticker = b.ticker AND s.date = b.date
                    WHERE s.ticker = ? ORDER BY s.date
                """, conn, params=(selected_ticker,))

                if not df_sim.empty:
                    df_sim["daily_return"] = df_sim["adj_close"].pct_change()
                    pos = 0; positions = []
                    for sig in df_sim["signal"]:
                        if sig == 1: pos = 1
                        elif sig == -1: pos = 0
                        positions.append(pos)
                    df_sim["strategy_return"] = df_sim["daily_return"] * pd.Series(positions).shift(1).values
                    strat_cum = (1 + df_sim["strategy_return"].fillna(0)).cumprod().iloc[-1] - 1
                    bh_cum = (1 + df_sim["daily_return"].fillna(0)).cumprod().iloc[-1] - 1
                    c1, c2, c3 = st.columns(3)
                    c1.metric("SMA Strategy", f"{strat_cum:+.1%}", delta=f"{(strat_cum-bh_cum):+.1%} vs B&H")
                    c2.metric("Buy & Hold", f"{bh_cum:+.1%}")
                    c3.metric("Data Points", f"{len(df_sim):,}")

            if strategy_view in ["Pullback (RSI)", "Both"] and table_exists("pullback_signals"):
                st.markdown("### 🎯 Pullback Strategy Simulation")
                df_pb_sim = pullback_strategy.simulate_pullback(selected_ticker, conn)
                if not df_pb_sim.empty:
                    df_pb_sim["daily_return"] = df_pb_sim["adj_close"].pct_change()
                    pb_cum = (1 + df_pb_sim["strategy_return"].fillna(0)).cumprod().iloc[-1] - 1
                    pb_bh = (1 + df_pb_sim["daily_return"].fillna(0)).cumprod().iloc[-1] - 1
                    entries = (df_pb_sim["signal"] == 1.0).sum()
                    exits = df_pb_sim["exit_signal"].notna().sum()
                    p1, p2, p3, p4 = st.columns(4)
                    p1.metric("Pullback", f"{pb_cum:+.1%}", delta=f"{(pb_cum-pb_bh):+.1%} vs B&H")
                    p2.metric("Buy & Hold", f"{pb_bh:+.1%}")
                    p3.metric("Entries", int(entries))
                    p4.metric("Exits", int(exits))
                else:
                    st.info("ℹ️ No pullback data for this ticker.")

        conn.close()


# ─────────────────────────────────────────────────────────────
# TAB 3: Strategy Comparison (Level 1 + Level 2)
# ─────────────────────────────────────────────────────────────
with tab3:
    st.markdown("## ⚔️ Strategy Comparison")
    st.divider()

    has_sma = table_exists("strategy_signals")
    has_pb = table_exists("pullback_signals")
    has_xs = table_exists("cross_sectional_scores")

    if not has_sma and not has_pb and not has_xs:
        st.info("ℹ️ Compute strategies first (Tab 1) to compare them.")
    else:
        conn = get_db_connection()
        available_tickers = pd.read_sql_query(
            "SELECT DISTINCT ticker FROM daily_bars ORDER BY ticker", conn
        )["ticker"].tolist()

        # ── L/S Z-Score Strategy (portfolio-level) ───────────
        if has_xs:
            st.markdown("### 🧬 L/S Monthly Z-Score Strategy")
            st.caption("Long the cheapest, short the most expensive — rebalance monthly")

            ls_col1, ls_col2 = st.columns(2)
            with ls_col1:
                n_long = st.number_input("Stocks to LONG", min_value=1, max_value=10, value=2, step=1, key="ls_n_long")
            with ls_col2:
                n_short = st.number_input("Stocks to SHORT", min_value=1, max_value=10, value=2, step=1, key="ls_n_short")

            from src.strategies.ls_zscore_strategy import simulate_ls_zscore
            ls_eq, ls_trades = simulate_ls_zscore(n_long=int(n_long), n_short=int(n_short))

            if not ls_eq.empty:
                ls_eq["date"] = pd.to_datetime(ls_eq["date"])
                ls_total = ls_eq["equity"].iloc[-1] / 10000 - 1

                # Build SPY Buy & Hold benchmark over the same period
                spy_bench = pd.read_sql_query("""
                    SELECT date, adj_close FROM daily_bars
                    WHERE ticker = 'SPY' ORDER BY date
                """, conn, parse_dates=["date"])

                if not spy_bench.empty:
                    spy_bench = spy_bench[spy_bench["date"] >= ls_eq["date"].iloc[0]]
                    spy_bench["daily_return"] = spy_bench["adj_close"].pct_change()
                    spy_bench["equity"] = 10000 * (1 + spy_bench["daily_return"].fillna(0)).cumprod()
                    spy_total = spy_bench["equity"].iloc[-1] / 10000 - 1
                else:
                    spy_total = None

                # Compute Sharpe and MaxDD for L/S
                ls_returns = ls_eq["daily_return"]
                ls_sharpe = ls_returns.mean() / ls_returns.std() * np.sqrt(252) if ls_returns.std() > 0 else 0
                ls_running_max = ls_eq["equity"].expanding().max()
                ls_max_dd = (1 - ls_eq["equity"] / ls_running_max).max()

                # Metrics row
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("L/S Z-Score", f"{ls_total:+.1%}",
                          delta=f"{(ls_total - (spy_total or 0)):+.1%} vs SPY" if spy_total is not None else None)
                m2.metric("SPY B&H", f"{spy_total:+.1%}" if spy_total is not None else "N/A")
                m3.metric("Sharpe", f"{ls_sharpe:.2f}")
                m4.metric("Max Drawdown", f"{ls_max_dd:.2%}")

                # Equity curve: L/S vs SPY
                fig_ls = go.Figure()
                fig_ls.add_trace(go.Scatter(
                    x=ls_eq["date"], y=ls_eq["equity"],
                    name="L/S Z-Score", line=dict(color="#E040FB", width=2.5),
                ))
                if not spy_bench.empty:
                    fig_ls.add_trace(go.Scatter(
                        x=spy_bench["date"], y=spy_bench["equity"],
                        name="SPY Buy & Hold", line=dict(color="#FF9800", width=2, dash="dash"),
                    ))
                fig_ls.update_layout(template="plotly_dark", height=450,
                    yaxis_title="Portfolio Value ($)",
                    title="L/S Monthly Rebalance vs SPY ($10,000)",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                st.plotly_chart(fig_ls, use_container_width=True)

                # Monthly trade log
                if ls_trades:
                    with st.expander("📒 Monthly Trade Log", expanded=False):
                        trade_rows = []
                        for t in ls_trades:
                            trade_rows.append({
                                "Month": t["month"],
                                "LONG": ", ".join(t["long"]),
                                "Long Z-Scores": ", ".join(f"{z:.2f}" for z in t["long_zscores"]),
                                "SHORT": ", ".join(t["short"]),
                                "Short Z-Scores": ", ".join(f"{z:.2f}" for z in t["short_zscores"]),
                            })
                        st.dataframe(pd.DataFrame(trade_rows), use_container_width=True, height=400)
            else:
                st.info("ℹ️ Not enough cross-sectional data. Run Phase 2 first.")

        # ── Per-Ticker Comparison (Level 1 strategies) ───────
        st.divider()
        st.markdown("### 📊 Per-Ticker: SMA Crossover vs Pullback")

        compare_ticker = st.selectbox("Select Ticker", available_tickers, index=0, key="compare_ticker")

        if compare_ticker:
            df_base = pd.read_sql_query(
                "SELECT date, adj_close FROM daily_bars WHERE ticker = ? ORDER BY date",
                conn, params=(compare_ticker,))
            df_base["date"] = pd.to_datetime(df_base["date"])
            df_base["daily_return"] = df_base["adj_close"].pct_change()
            df_base["buyhold_equity"] = (1 + df_base["daily_return"].fillna(0)).cumprod() * 10000

            sma_cum = None
            df_sma_eq = pd.DataFrame()
            if has_sma:
                df_s = pd.read_sql_query("""
                    SELECT s.date, s.signal, b.adj_close FROM strategy_signals s
                    JOIN daily_bars b ON s.ticker = b.ticker AND s.date = b.date
                    WHERE s.ticker = ? ORDER BY s.date
                """, conn, params=(compare_ticker,))
                if not df_s.empty:
                    df_s["date"] = pd.to_datetime(df_s["date"])
                    df_s["daily_return"] = df_s["adj_close"].pct_change()
                    pos = 0; pp = []
                    for sig in df_s["signal"]:
                        if sig == 1: pos = 1
                        elif sig == -1: pos = 0
                        pp.append(pos)
                    df_s["strategy_return"] = df_s["daily_return"] * pd.Series(pp).shift(1).values
                    df_s["equity"] = (1 + df_s["strategy_return"].fillna(0)).cumprod() * 10000
                    sma_cum = df_s["equity"].iloc[-1] / 10000 - 1
                    df_sma_eq = df_s

            pb_cum = None
            df_pb_eq = pd.DataFrame()
            if has_pb:
                df_p = pullback_strategy.simulate_pullback(compare_ticker, conn)
                if not df_p.empty:
                    df_p["date"] = pd.to_datetime(df_p["date"])
                    df_p["equity"] = (1 + df_p["strategy_return"].fillna(0)).cumprod() * 10000
                    pb_cum = df_p["equity"].iloc[-1] / 10000 - 1
                    df_pb_eq = df_p

            bh_cum = df_base["buyhold_equity"].iloc[-1] / 10000 - 1

            c1, c2, c3 = st.columns(3)
            c1.metric("Buy & Hold", f"{bh_cum:+.1%}")
            c2.metric("SMA Crossover", f"{sma_cum:+.1%}" if sma_cum is not None else "N/A",
                       delta=f"{(sma_cum-bh_cum):+.1%} vs B&H" if sma_cum is not None else None)
            c3.metric("Pullback (RSI)", f"{pb_cum:+.1%}" if pb_cum is not None else "N/A",
                       delta=f"{(pb_cum-bh_cum):+.1%} vs B&H" if pb_cum is not None else None)

            st.markdown("### 📈 Equity Curves ($10,000)")
            fig_comp = go.Figure()
            fig_comp.add_trace(go.Scatter(x=df_base["date"], y=df_base["buyhold_equity"],
                name="Buy & Hold", line=dict(color="#FF9800", width=2, dash="dash")))
            if not df_sma_eq.empty:
                fig_comp.add_trace(go.Scatter(x=df_sma_eq["date"], y=df_sma_eq["equity"],
                    name="SMA Crossover", line=dict(color="#2196F3", width=2)))
            if not df_pb_eq.empty:
                fig_comp.add_trace(go.Scatter(x=df_pb_eq["date"], y=df_pb_eq["equity"],
                    name="Pullback (RSI)", line=dict(color="#00BCD4", width=2)))
            fig_comp.update_layout(template="plotly_dark", height=500,
                yaxis_title="Portfolio Value ($)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_comp, use_container_width=True)

        conn.close()


# ─────────────────────────────────────────────────────────────
# TAB 4: Execution Desk
# ─────────────────────────────────────────────────────────────
with tab4:
    st.markdown("## Phase 4: Execution Desk")
    st.divider()

    today_str = datetime.now().strftime("%Y-%m-%d")

    st.markdown("### 📋 Today's Signals")
    if table_exists("strategy_signals"):
        conn = get_db_connection()
        today_signals = pd.read_sql_query("""
            SELECT s.ticker, 'SMA' as strategy, b.adj_close as price,
                   CASE WHEN s.signal=1 THEN 'BUY' WHEN s.signal=-1 THEN 'SELL' END as action
            FROM strategy_signals s JOIN daily_bars b ON s.ticker=b.ticker AND s.date=b.date
            WHERE s.signal != 0 AND s.date = ?
        """, conn, params=(today_str,))

        if table_exists("pullback_signals"):
            pb_today = pd.read_sql_query("""
                SELECT p.ticker, 'Pullback' as strategy, p.close as price,
                       CASE WHEN p.signal=1.0 THEN 'BUY' WHEN p.exit_signal IS NOT NULL THEN 'SELL' END as action
                FROM pullback_signals p
                WHERE (p.signal=1.0 OR p.exit_signal IS NOT NULL) AND p.date=?
            """, conn, params=(today_str,))
            today_signals = pd.concat([today_signals, pb_today], ignore_index=True)

        if today_signals.empty:
            st.info(f"No signals for today ({today_str}).")
        else:
            st.dataframe(today_signals, use_container_width=True)
        conn.close()
    else:
        st.info("ℹ️ No signals computed yet.")

    st.divider()
    st.markdown("### 🛡️ Risk Check")

    conn = get_db_connection()
    try:
        positions_df = pd.read_sql_query("""
            SELECT ticker,
                   SUM(CASE WHEN action='BUY' THEN quantity ELSE -quantity END) as net_shares
            FROM paper_executions GROUP BY ticker HAVING net_shares > 0
        """, conn)
    except Exception:
        positions_df = pd.DataFrame()

    current_open = len(positions_df) if not positions_df.empty else 0
    c1, c2, c3 = st.columns(3)
    c1.metric("Open Positions", f"{current_open} / {int(max_positions)}")
    c2.metric("Capital Per Trade", f"${int(capital_per_trade):,}")
    slots = int(max_positions) - current_open
    c3.metric("Available Slots", slots if slots > 0 else "0 ⛔")
    if not positions_df.empty:
        with st.expander("📊 Current Positions", expanded=True):
            st.dataframe(positions_df, use_container_width=True)
    conn.close()

    st.divider()
    st.markdown("### 🚀 Route Paper Trades")

    col_e1, col_e2 = st.columns([2, 1])
    with col_e1:
        if st.button("🚀 ROUTE PAPER TRADES", type="primary", use_container_width=True):
            with st.spinner("Running simulation filter and routing..."):
                simulation.MAX_OPEN_POSITIONS = int(max_positions)
                simulation.CAPITAL_PER_TRADE = float(capital_per_trade)
                approved = simulation.simulate_and_filter()
                if approved:
                    execution.route_orders(approved)
                    st.success(f"✅ {len(approved)} orders routed!")
                    st.rerun()
                else:
                    st.info("ℹ️ No orders to route.")

    with col_e2:
        if st.button("🔄 Run Full L2 Pipeline", use_container_width=True):
            with st.spinner("Running complete Level 2 pipeline..."):
                db_init.init_db()
                data_ingestion.UNIVERSE = universe_list
                data_ingestion.ingest()
                fundamental_ingestion.ingest_fundamentals(tickers=universe_list)
                cross_sectional_scoring.compute_cross_sectional_scores()
                wfo_backtester.run_wfo_tournament()
                orders = portfolio_rebalancer.rebalance_portfolio()
                execution.route_orders(orders)
            st.success("✅ Full Level 2 pipeline executed!")
            st.rerun()

    st.divider()
    st.markdown("### 📒 Execution Ledger")
    if table_exists("paper_executions"):
        conn = get_db_connection()
        df_exec = pd.read_sql_query("SELECT * FROM paper_executions ORDER BY timestamp DESC", conn)
        conn.close()
        if not df_exec.empty:
            st.dataframe(df_exec, use_container_width=True, height=400)
        else:
            st.info("ℹ️ No trades executed yet.")
    else:
        st.info("ℹ️ Initialize the database first.")


# ─────────────────────────────────────────────────────────────
# TAB 5: Cross-Sectional Scores (Level 2 NEW)
# ─────────────────────────────────────────────────────────────
with tab5:
    st.markdown("## 🧬 Cross-Sectional EV/Sales Z-Score Rankings")
    st.divider()

    if not table_exists("cross_sectional_scores"):
        st.info("ℹ️ No cross-sectional scores yet. Run Phase 2 from Tab 1.")
    else:
        conn = get_db_connection()

        # Get available dates
        dates = pd.read_sql_query(
            "SELECT DISTINCT date FROM cross_sectional_scores ORDER BY date DESC", conn
        )["date"].tolist()

        selected_date = st.selectbox("Select Date", dates, index=0, key="xs_date")

        if selected_date:
            # Load scores for selected date
            day_scores = pd.read_sql_query("""
                SELECT ticker, enterprise_value, ev_to_sales, ev_sales_zscore, target_weight
                FROM cross_sectional_scores
                WHERE date = ?
                ORDER BY ev_sales_zscore ASC
            """, conn, params=(selected_date,))

            # Z-Score Bar Chart
            st.markdown(f"### Z-Score Rankings — {selected_date}")
            fig_zs = go.Figure()

            colors = ["#00E676" if z < ZSCORE_BUY_THRESHOLD else
                      "#FF9800" if z < 0 else
                      "#FF1744" for z in day_scores["ev_sales_zscore"]]

            fig_zs.add_trace(go.Bar(
                x=day_scores["ticker"],
                y=day_scores["ev_sales_zscore"],
                marker_color=colors,
                text=[f"{z:.2f}" for z in day_scores["ev_sales_zscore"]],
                textposition="outside",
            ))

            fig_zs.add_hline(y=ZSCORE_BUY_THRESHOLD, line_dash="dash",
                             line_color="#00E676", annotation_text=f"BUY Threshold ({ZSCORE_BUY_THRESHOLD})")

            fig_zs.update_layout(
                template="plotly_dark", height=500,
                yaxis_title="EV/Sales Z-Score",
                xaxis_title="Ticker",
            )
            st.plotly_chart(fig_zs, use_container_width=True)

            # Summary metrics
            buy_count = (day_scores["ev_sales_zscore"] < ZSCORE_BUY_THRESHOLD).sum()
            total_weight = day_scores["target_weight"].sum()
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Tickers Scored", len(day_scores))
            c2.metric("BUY Candidates", buy_count)
            c3.metric("Total Weight", f"{total_weight:.1%}")
            c4.metric("Mean Z-Score", f"{day_scores['ev_sales_zscore'].mean():.3f}")

            # Data table
            st.markdown("### 📋 Full Score Table")
            display_df = day_scores.copy()
            display_df["enterprise_value"] = display_df["enterprise_value"].apply(lambda x: f"${x/1e9:.1f}B")
            display_df["ev_to_sales"] = display_df["ev_to_sales"].apply(lambda x: f"{x:.2f}")
            display_df["ev_sales_zscore"] = display_df["ev_sales_zscore"].apply(lambda x: f"{x:.3f}")
            display_df["target_weight"] = display_df["target_weight"].apply(lambda x: f"{x:.1%}")
            st.dataframe(display_df, use_container_width=True, height=400)

            # Time-series Z-score for a selected ticker
            st.divider()
            st.markdown("### 📈 Z-Score Over Time")
            ticker_for_ts = st.selectbox("Select Ticker", day_scores["ticker"].tolist(), key="zs_ts_ticker")
            if ticker_for_ts:
                ts_data = pd.read_sql_query("""
                    SELECT date, ev_sales_zscore FROM cross_sectional_scores
                    WHERE ticker = ? ORDER BY date
                """, conn, params=(ticker_for_ts,))
                ts_data["date"] = pd.to_datetime(ts_data["date"])

                fig_ts = go.Figure()
                fig_ts.add_trace(go.Scatter(
                    x=ts_data["date"], y=ts_data["ev_sales_zscore"],
                    mode="lines", name=ticker_for_ts,
                    line=dict(color="#2196F3", width=2),
                ))
                fig_ts.add_hline(y=ZSCORE_BUY_THRESHOLD, line_dash="dash",
                                 line_color="#00E676", annotation_text="BUY Threshold")
                fig_ts.add_hline(y=0, line_dash="dot", line_color="gray")
                fig_ts.update_layout(template="plotly_dark", height=400,
                    yaxis_title="EV/Sales Z-Score")
                st.plotly_chart(fig_ts, use_container_width=True)

        conn.close()


# ─────────────────────────────────────────────────────────────
# TAB 6: WFO Tournament (Level 2 NEW)
# ─────────────────────────────────────────────────────────────
with tab6:
    st.markdown("## 🏆 Walk-Forward Optimization Tournament")
    st.divider()

    if not table_exists("wfo_results"):
        st.info("ℹ️ No WFO results yet. Run Phase 3 first.")
    else:
        conn = get_db_connection()
        wfo_df = pd.read_sql_query(
            "SELECT * FROM wfo_results ORDER BY test_window_start", conn
        )

        if wfo_df.empty:
            st.info("ℹ️ No WFO results yet. Click 'Run WFO Tournament' below or run Phase 3.")

            if st.button("🏆 Run WFO Tournament", use_container_width=True):
                with st.spinner("Running Walk-Forward Optimization..."):
                    wfo_backtester.run_wfo_tournament()
                st.success("✅ WFO tournament complete!")
                st.rerun()
        else:
            # WFO Metrics Table
            st.markdown("### 📊 Test Window Metrics")
            display_wfo = wfo_df[["strategy_id", "test_window_start", "test_window_end",
                                   "sharpe_ratio", "max_drawdown", "cagr"]].copy()
            display_wfo["sharpe_ratio"] = display_wfo["sharpe_ratio"].apply(lambda x: f"{x:.3f}")
            display_wfo["max_drawdown"] = display_wfo["max_drawdown"].apply(lambda x: f"{x:.2%}")
            display_wfo["cagr"] = display_wfo["cagr"].apply(lambda x: f"{x:.2%}")
            st.dataframe(display_wfo, use_container_width=True)

            # Overall OOS summary
            avg_sharpe = wfo_df["sharpe_ratio"].mean()
            avg_dd = wfo_df["max_drawdown"].mean()
            avg_cagr = wfo_df["cagr"].mean()

            st.markdown("### 📈 Aggregate OOS Performance")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Avg Sharpe", f"{avg_sharpe:.3f}")
            c2.metric("Avg Max DD", f"{avg_dd:.2%}")
            c3.metric("Avg CAGR", f"{avg_cagr:.2%}")
            c4.metric("Windows", len(wfo_df))

            # Friction parameters
            st.divider()
            st.markdown("### ⚙️ Friction Parameters")
            f1, f2, f3, f4 = st.columns(4)
            f1.metric("Slippage", f"{SLIPPAGE_BPS*10000:.1f} bps")
            f2.metric("Commission", f"${COMMISSION_PER_SHARE}/share")
            f3.metric("Max Weight", f"{MAX_SINGLE_WEIGHT:.0%}")
            f4.metric("Cash Buffer", f"{CASH_BUFFER:.0%}")

        conn.close()


# ─────────────────────────────────────────────────────────────
# TAB 7: Portfolio Rebalancer (Level 2 NEW)
# ─────────────────────────────────────────────────────────────
with tab7:
    st.markdown("## ⚖️ Portfolio Rebalancer")
    st.divider()

    if not table_exists("cross_sectional_scores"):
        st.info("ℹ️ No cross-sectional scores yet. Run Phase 2 from Tab 1.")
    else:
        conn = get_db_connection()

        # Load latest target weights
        targets = pd.read_sql_query("""
            SELECT ticker, target_weight, ev_sales_zscore
            FROM cross_sectional_scores
            WHERE date = (SELECT MAX(date) FROM cross_sectional_scores)
            ORDER BY target_weight DESC, ev_sales_zscore ASC
        """, conn)

        latest_date = pd.read_sql_query(
            "SELECT MAX(date) as d FROM cross_sectional_scores", conn
        )["d"].iloc[0]

        # Current portfolio state
        from src.pipeline.portfolio_state import get_portfolio_state
        total_equity, holdings = get_portfolio_state()

        st.markdown(f"### Portfolio State (as of {latest_date})")

        # Summary metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Equity", f"${total_equity:,.0f}")
        c2.metric("Positions Held", len(holdings))
        active_targets = targets[targets["target_weight"] > 0]
        c3.metric("Target Positions", len(active_targets))
        c4.metric("Target Total Weight", f"{active_targets['target_weight'].sum():.1%}")

        # Target weights chart
        if not active_targets.empty:
            st.markdown("### 🎯 Target Weights")
            fig_tw = go.Figure()
            fig_tw.add_trace(go.Bar(
                x=active_targets["ticker"],
                y=active_targets["target_weight"],
                marker_color="#2196F3",
                text=[f"{w:.1%}" for w in active_targets["target_weight"]],
                textposition="outside",
            ))
            fig_tw.add_hline(y=MAX_SINGLE_WEIGHT, line_dash="dash",
                             line_color="#FF9800", annotation_text=f"Max Weight ({MAX_SINGLE_WEIGHT:.0%})")
            fig_tw.update_layout(template="plotly_dark", height=400,
                yaxis_title="Target Weight", yaxis_tickformat=".0%")
            st.plotly_chart(fig_tw, use_container_width=True)

        # Rebalance button
        st.divider()
        st.markdown("### 🚀 Execute Rebalance")

        if st.button("⚖️ CALCULATE & EXECUTE REBALANCE", type="primary", use_container_width=True):
            with st.spinner("Calculating rebalance orders..."):
                orders = portfolio_rebalancer.rebalance_portfolio()
                if orders:
                    st.markdown("#### Order Preview")
                    orders_df = pd.DataFrame(orders)
                    st.dataframe(orders_df, use_container_width=True)

                    if st.button("✅ Confirm & Route Orders"):
                        execution.route_orders(orders)
                        st.success(f"✅ {len(orders)} rebalance orders routed!")
                        st.rerun()
                else:
                    st.success("✅ Portfolio already at target. No rebalance needed.")

        conn.close()
