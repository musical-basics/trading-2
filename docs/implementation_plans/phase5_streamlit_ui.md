# Phase 5: Streamlit UI Upgrade — Implementation Plan

## Goal
Upgrade the Level 1 Streamlit dashboard to display Level 2 data: cross-sectional scores, WFO tearsheets, equity curves, and the portfolio rebalancer. The UI transitions from individual-ticker technical views to universe-wide quantitative analysis.

## Architecture References
- [stack.md](file:///Users/test2/Documents/trading-2/docs/stack.md) — UI spec (WFO tearsheets, equity curves, cross-sectional tables)
- [prd.md](file:///Users/test2/Documents/trading-2/docs/prd.md) — Feature scope
- Current UI: [app.py](file:///Users/test2/Documents/trading-2/ui/app.py) — 648 lines, 4 tabs

## Proposed Changes

### UI Restructure

#### [MODIFY] [app.py](file:///Users/test2/Documents/trading-2/ui/app.py)

The current app has 4 tabs. We will **add 3 new Level 2 tabs** while keeping the existing Level 1 tabs accessible for backward compatibility.

**New tab structure (7 tabs):**

| Tab | Name | Level | Status |
|-----|------|-------|--------|
| 1 | 📥 Data & Signals | L1 | Keep as-is |
| 2 | 📊 Charts & Simulation | L1 | Keep as-is |
| 3 | ⚔️ Strategy Comparison | L1 | Keep as-is |
| 4 | 🚀 Execution Desk | L1→L2 | Upgrade |
| 5 | 🧬 Cross-Sectional Scores | L2 | **NEW** |
| 6 | 🏆 WFO Tournament | L2 | **NEW** |
| 7 | ⚖️ Portfolio Rebalancer | L2 | **NEW** |

---

### Tab 5: Cross-Sectional Scores (NEW)

**Purpose:** Visualize the daily EV/Sales Z-score rankings across the full universe.

**Components:**
1. **Date selector** — Pick any date with score data
2. **Z-score heatmap / bar chart:**
   - X-axis: tickers (sorted by Z-score)
   - Y-axis: `ev_sales_zscore`
   - Color: gradient (green for undervalued / Z < -1, red for overvalued / Z > 1)
   - Horizontal line at `ZSCORE_BUY_THRESHOLD = -1.0`
3. **Data table:** Full `cross_sectional_scores` for the selected date (sortable)
   - Columns: ticker, enterprise_value, ev_to_sales, ev_sales_zscore, target_weight
4. **Time-series view:** Select a ticker → plot its Z-score over time
5. **Universe coverage:** Show how many tickers have fundamental data on each date

**Data source:** `SELECT * FROM cross_sectional_scores WHERE date = ?`

---

### Tab 6: WFO Tournament (NEW)

**Purpose:** Display WFO backtest results — the "proof" that the strategy works out-of-sample.

**Components:**
1. **WFO metrics table:**
   - Rows = test windows
   - Columns: `test_window_start`, `test_window_end`, `sharpe_ratio`, `max_drawdown`, `cagr`
   - Color-coded (green/red) based on performance
2. **Stitched OOS equity curve:**
   - Plot the combined out-of-sample equity curve (from `wfo_backtester`)
   - Overlay a buy-and-hold SPY benchmark
   - Show drawdown subplot below the equity curve
3. **Per-window breakdown:**
   - Expandable sections for each train/test window
   - Show which Z-score threshold was selected during training
   - Show the test-window equity curve individually
4. **Friction impact visualization:**
   - Two curves: with friction vs. without friction
   - Highlight the cost of slippage and commissions
5. **Summary metrics panel:**
   - Overall OOS Sharpe, Max Drawdown, CAGR
   - Total friction cost ($)
   - Number of trades

**Data source:** `SELECT * FROM wfo_results ORDER BY test_window_start`

---

### Tab 7: Portfolio Rebalancer (NEW)

**Purpose:** Show the current portfolio state, target weights, and pending rebalance orders.

**Components:**
1. **Current portfolio snapshot:**
   - Table: ticker, current_shares, current_value, current_weight
   - Pie chart of current allocations
2. **Target weights panel:**
   - Table: ticker, target_weight, target_shares, delta_shares, action
   - Bar chart: current weight vs target weight side-by-side
3. **Rebalance button:**
   - "⚖️ Execute Rebalance" → calls `portfolio_rebalancer.rebalance_portfolio()` then `execution.route_orders()`
   - Shows order preview before execution
4. **Constraints display:**
   - Max single weight: 10%
   - Cash buffer: 5%
   - ADV gating status per ticker
5. **Execution history:**
   - Recent rebalance trades from `paper_executions` where `strategy_id = 'ev_sales_v2'`

---

### Tab 1 Update: Data & Signals

#### [MODIFY] Tab 1 in [app.py](file:///Users/test2/Documents/trading-2/ui/app.py)

Add buttons for Level 2 pipeline steps:
- "📥 Ingest Fundamentals" → calls `fundamental_ingestion.ingest_fundamentals()`
- "🧮 Compute Cross-Sectional Scores" → calls `cross_sectional_scoring.compute_cross_sectional_scores()`
- "🏆 Run WFO Tournament" → calls `wfo_backtester.run_wfo_tournament()`
- Add database status metrics for new tables: `quarterly_fundamentals`, `cross_sectional_scores`, `wfo_results`

---

### Sidebar Update

#### [MODIFY] Sidebar in [app.py](file:///Users/test2/Documents/trading-2/ui/app.py)

- Add Level 2 database status metrics (row counts for new tables)
- Add Level 2 parameter controls:
  - Z-score threshold slider
  - WFO window size inputs
  - Friction parameters display (read-only, from config)
- Update page title: "Level 2 — Quant Sandbox"

---

### Header Update

- Page title: `Level 1 — Walking Skeleton` → `Level 2 — Quant Sandbox`
- Update `page_icon` to `🧪`

## Verification Plan

### Manual Verification
1. **Tab rendering:** All 7 tabs load without errors
2. **Cross-sectional chart:** Z-score bar chart renders with correct color gradient
3. **WFO equity curve:** Stitched OOS curve displays correctly with SPY benchmark
4. **Rebalancer:** Order preview shows correct deltas
5. **Responsiveness:** All new tabs work on wide and narrow layouts
6. **Empty state:** Tabs gracefully handle missing data (show "Run Phase X first" messages)
