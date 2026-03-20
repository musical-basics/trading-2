"""WFO Tournament — Walk-Forward Optimization results."""

import streamlit as st
import pandas as pd
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.config import SLIPPAGE_BPS, COMMISSION_PER_SHARE, MAX_SINGLE_WEIGHT, CASH_BUFFER
from src.pipeline import wfo_backtester

cfg = render_sidebar()

st.markdown("# 🏆 Walk-Forward Optimization Tournament")
st.divider()

if not table_exists("wfo_results"):
    st.info("ℹ️ No WFO results yet. Run the pipeline or click below.")
    if st.button("🏆 Run WFO Tournament", use_container_width=True):
        with st.spinner("Running Walk-Forward Optimization..."):
            wfo_backtester.run_wfo_tournament()
        st.success("✅ WFO tournament complete!")
        st.rerun()
else:
    conn = get_db_connection()
    wfo_df = pd.read_sql_query(
        "SELECT * FROM wfo_results ORDER BY test_window_start", conn
    )

    if wfo_df.empty:
        st.info("ℹ️ No WFO results yet.")
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
