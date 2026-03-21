"""Data Pipeline — Run full pipeline or individual steps."""

import streamlit as st
import pandas as pd
import os
from ui.shared import get_db_connection, table_exists, get_table_count, render_sidebar, DB_PATH
from src.pipeline import (
    db_init, data_ingestion, fundamental_ingestion,
    fundamental_ingestion_fmp, cross_sectional_scoring,
)
from src.strategies import strategy, pullback_strategy

cfg = render_sidebar()

st.markdown("# ⚙️ Data Pipeline")
st.divider()

# ── Primary Action ───────────────────────────────────────────
if st.button("🚀 Run Full Pipeline", type="primary", use_container_width=True):
    progress = st.progress(0, text="Initializing database...")
    db_init.init_db()

    progress.progress(15, text="Ingesting EOD prices...")
    data_ingestion.UNIVERSE = cfg["universe"]
    data_ingestion.ingest()

    progress.progress(35, text="Ingesting quarterly fundamentals (FMP if available)...")
    fundamental_ingestion_fmp.ingest_fundamentals_fmp(tickers=cfg["universe"])

    progress.progress(55, text="Computing cross-sectional EV/Sales Z-scores...")
    cross_sectional_scoring.compute_cross_sectional_scores()

    progress.progress(70, text="Computing SMA crossover signals...")
    strategy.compute_signals()

    progress.progress(85, text="Computing pullback strategy signals...")
    pullback_strategy.RSI_PERIOD = cfg["rsi_period"]
    pullback_strategy.RSI_ENTRY_THRESHOLD = cfg["rsi_entry"]
    pullback_strategy.RSI_EXIT_THRESHOLD = cfg["rsi_exit"]
    pullback_strategy.compute_pullback_signals()

    progress.progress(100, text="✅ Pipeline complete!")
    st.success(f"✅ Full pipeline executed for {len(cfg['universe'])} tickers!")
    st.rerun()

# ── Pipeline Status ──────────────────────────────────────────
if os.path.exists(DB_PATH):
    st.markdown("### Pipeline Status")
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Daily Bars", f"{get_table_count('daily_bars'):,}")
    s2.metric("Fundamentals", f"{get_table_count('quarterly_fundamentals'):,}")
    s3.metric("XS Scores", f"{get_table_count('cross_sectional_scores'):,}")
    s4.metric("SMA Signals", f"{get_table_count('strategy_signals'):,}")
    s5.metric("Pullback Sig", f"{get_table_count('pullback_signals'):,}")
else:
    st.info("ℹ️ Click **Run Full Pipeline** to get started.")

# ── Individual Steps ─────────────────────────────────────────
with st.expander("⚙️ Run Individual Steps", expanded=False):
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("🗃️ Init DB Only", use_container_width=True):
            db_init.init_db()
            st.success("✅ Database initialized!")
            st.rerun()
        if st.button("📥 Ingest Fundamentals (FMP)", use_container_width=True):
            fundamental_ingestion_fmp.ingest_fundamentals_fmp(tickers=cfg["universe"])
            st.success("✅ Fundamentals ingested!")
            st.rerun()
        if st.button("🧮 SMA Signals Only", use_container_width=True):
            strategy.compute_signals()
            st.success("✅ SMA signals computed!")
            st.rerun()
    with col_b:
        if st.button("📥 Ingest Prices Only", use_container_width=True):
            data_ingestion.UNIVERSE = cfg["universe"]
            data_ingestion.ingest()
            st.success("✅ Prices ingested!")
            st.rerun()
        if st.button("🧮 XS Scores Only", use_container_width=True):
            cross_sectional_scoring.compute_cross_sectional_scores()
            st.success("✅ Cross-sectional scores computed!")
            st.rerun()
        if st.button("🎯 Pullback Signals Only", use_container_width=True):
            pullback_strategy.RSI_PERIOD = cfg["rsi_period"]
            pullback_strategy.RSI_ENTRY_THRESHOLD = cfg["rsi_entry"]
            pullback_strategy.RSI_EXIT_THRESHOLD = cfg["rsi_exit"]
            pullback_strategy.compute_pullback_signals()
            st.success("✅ Pullback signals computed!")
            st.rerun()

# ── Data Previews ────────────────────────────────────────────
st.divider()
preview_tables = {
    "daily_bars": "SELECT * FROM daily_bars ORDER BY date DESC LIMIT 100",
    "quarterly_fundamentals": "SELECT * FROM quarterly_fundamentals ORDER BY filing_date DESC LIMIT 100",
    "cross_sectional_scores": "SELECT * FROM cross_sectional_scores ORDER BY date DESC, ev_sales_zscore ASC LIMIT 100",
    "strategy_signals": "SELECT * FROM strategy_signals ORDER BY date DESC LIMIT 100",
}
for table_name, query in preview_tables.items():
    if table_exists(table_name):
        with st.expander(f"📋 Preview: {table_name}", expanded=False):
            conn = get_db_connection()
            st.dataframe(pd.read_sql_query(query, conn), use_container_width=True, height=400)
            conn.close()
