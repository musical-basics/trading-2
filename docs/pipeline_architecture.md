# Level 2 Data Flow: The Master Enrichment Pipeline

## 1. Step-by-Step Execution Flow

### PHASE 1: Data Ingestion (`ingest_prices()` & `ingest_fundamentals()`)
* **Action:** Fetch EOD prices AND Quarterly Fundamentals.
* **Database:** Upsert into `daily_bars` and `quarterly_fundamentals`. Ensure `filing_date` is properly calculated to mock the SEC filing delay.

### PHASE 2: The Math Desk (`compute_cross_sectional_scores()`)
* **CRITICAL ENGINEERING STEP (Data Alignment):** You MUST use `pandas.merge_asof(direction='backward')` to join fundamentals onto daily bars. You join on `daily_bars.date` and `fundamentals.filing_date`. This guarantees that on any given `date`, the system only "knows" the fundamental data from the most recently published filing, completely eliminating Look-Ahead Bias.
* **The Math:** Calculate EV, EV/Sales, and group by `date` to calculate the daily `ev_sales_zscore` for the entire universe. 
* **The Signal:** Generate a `target_weight` (e.g., 0.10) for tickers where the Z-score < -1.0 (undervalued), and 0.0 for the rest.

### PHASE 3: The WFO Backtester (`run_wfo_tournament()`)
* **Action:** Instead of a simple start-to-finish paper PnL, run the historical data through rolling Train/Test windows. Apply the **Friction System** (deduct slippage). Save metrics to `wfo_results`.

### PHASE 4: Execution Reconciliation (`route_orders()`)
* **Action:** Compare the `target_weight` against current broker holdings, calculate the delta in shares, apply liquidity limits, and route the necessary orders to Alpaca.
