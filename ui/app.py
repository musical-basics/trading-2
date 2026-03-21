"""
ui/app.py — Level 2 Quant Sandbox: Multi-Page Navigation Entry Point

Run with: streamlit run ui/app.py  (from project root)
"""

import sys
import os

# Ensure project root is on the path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import streamlit as st

# ═══════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Level 2 — Quant Sandbox",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ═══════════════════════════════════════════════════════════════
# MULTI-PAGE NAVIGATION
# ═══════════════════════════════════════════════════════════════
pages = {
    "📊 Dashboard": [
        st.Page("pages/dashboard.py", title="Dashboard", icon="🏠", default=True),
    ],
    "🔬 Research": [
        st.Page("pages/charts.py", title="Charts & Simulation", icon="📊"),
        st.Page("pages/strategy_comparison.py", title="Strategy Comparison", icon="⚔️"),
        st.Page("pages/xs_scores.py", title="Cross-Sectional Scores", icon="🧬"),
        st.Page("pages/tournament.py", title="Strategy Tournament", icon="🏆"),
        st.Page("pages/wfo.py", title="WFO Backtester", icon="🔬"),
    ],
    "⚙️ Operations": [
        st.Page("pages/pipeline.py", title="Data Pipeline", icon="📥"),
        st.Page("pages/rebalancer.py", title="Portfolio Rebalancer", icon="⚖️"),
    ],
    "🚀 Trading": [
        st.Page("pages/execution.py", title="Execution Desk", icon="🚀"),
    ],
}

nav = st.navigation(pages)
nav.run()
