# Phase 3: Walk-Forward Optimization (WFO) Backtester — Implementation Plan

## Goal
Build a rolling Walk-Forward Optimization engine that evaluates the cross-sectional strategy on out-of-sample data with mandatory transaction cost friction. This replaces the simple start-to-finish backtest from Level 1.

## Architecture References
- [strat-flow.md](file:///Users/test2/Documents/trading-2/docs/strat-flow.md) — WFO loop spec + friction system
- [pipeline_architecture.md](file:///Users/test2/Documents/trading-2/docs/pipeline_architecture.md) — Phase 3 spec
- [datastructure.md](file:///Users/test2/Documents/trading-2/docs/datastructure.md) — `wfo_results` schema

## Proposed Changes

### WFO Engine

#### [NEW] `src/pipeline/wfo_backtester.py`

**Main function:** `run_wfo_tournament(strategy_id='ev_sales_zscore')`

**Step-by-step logic:**

1. **Load `cross_sectional_scores` into a DataFrame:**
   - Columns needed: `ticker, date, ev_sales_zscore, target_weight`
   - Also load `daily_bars` for price data (`adj_close`)
   - Merge scores with prices on `(ticker, date)`

2. **Define WFO windows (configurable via `config.py`):**
   ```python
   TRAIN_YEARS = 2          # Lookback window
   TEST_YEARS = 1           # Forward test window
   STEP_YEARS = 1           # Roll step
   ```
   - Add these constants to `config.py`

3. **The WFO loop:**
   ```
   For each window:
     a. TRAIN BLOCK: date range [window_start, window_start + 2 years)
        - Find optimal Z-score threshold that maximizes Sharpe in-sample
        - Test thresholds: [-0.5, -0.75, -1.0, -1.25, -1.5]
     b. TEST BLOCK: date range [train_end, train_end + 1 year)
        - Apply the best threshold from training to generate target_weights
        - Simulate daily portfolio returns with friction deductions
     c. Collect test-block equity curve and metrics
     d. Roll forward by STEP_YEARS
   ```

4. **Portfolio return simulation (per test block):**
   ```python
   # For each day in the test window:
   #   portfolio_return = sum(target_weight_i * daily_return_i) for all tickers
   #
   # On days where target_weight changes (trade required):
   #   friction = slippage + commissions
   #   Subtract friction from that day's portfolio return
   ```

5. **The Friction System (mandatory):**
   - **Detect trade days:** Where `target_weight` differs from previous day's `target_weight` for any ticker
   - **Slippage:** `abs(weight_delta) * portfolio_value * SLIPPAGE_BPS` (5 bps)
   - **Commissions:** `abs(share_delta) * COMMISSION_PER_SHARE` ($0.005/share)
   - Implementation: subtract friction cost as a proportion of portfolio value from that day's `daily_return`

6. **Stitch OOS equity curves:**
   - Concatenate all test-block equity curves into one continuous series
   - This produces the "real" out-of-sample performance, free of in-sample curve-fitting

7. **Calculate metrics per test window:**
   - **Sharpe Ratio:** `mean(daily_return) / std(daily_return) * sqrt(252)`
   - **Max Drawdown:** `max(1 - equity / running_max_equity)`
   - **CAGR:** `(final_equity / initial_equity) ^ (252 / trading_days) - 1`

8. **Save to `wfo_results` table:**
   - One row per test window: `strategy_id, test_window_start, test_window_end, sharpe_ratio, max_drawdown, cagr`

---

### Configuration Updates

#### [MODIFY] [config.py](file:///Users/test2/Documents/trading-2/src/config.py)
Add WFO window constants:
```python
WFO_TRAIN_YEARS = 2
WFO_TEST_YEARS = 1
WFO_STEP_YEARS = 1
WFO_ZSCORE_CANDIDATES = [-0.5, -0.75, -1.0, -1.25, -1.5]
```

---

### Pipeline Integration

#### [MODIFY] [main.py](file:///Users/test2/Documents/trading-2/main.py)
- Add import: `from src.pipeline import wfo_backtester`
- Add Phase 3 call: `wfo_backtester.run_wfo_tournament()`
- Place after `cross_sectional_scoring.compute_cross_sectional_scores()`

---

### Replace Level 1 Simulation

#### [MODIFY] [simulation.py](file:///Users/test2/Documents/trading-2/src/pipeline/simulation.py)
- The existing `simulate_and_filter()` uses Level 1 logic (fixed $1000 sizing, SMA signals)
- Keep it for backward compatibility but add a new function `simulate_and_filter_v2()` that:
  - Reads from `cross_sectional_scores` instead of `strategy_signals`
  - Uses target weights instead of fixed position sizing
  - Checks that the strategy has passed the WFO tournament (has entries in `wfo_results`)
  - Returns a list of `{ticker, target_weight, price}` dicts instead of `{ticker, action, quantity, price}`

## Verification Plan

### Automated Tests
1. **Window count:** With ~2 years of data, expect 1 train/test window. With 5+ years, expect multiple
2. **No overlap:** Test windows must not overlap with each other
3. **Friction impact:** Stitched OOS equity must be strictly less than a frictionless version
4. **Metric sanity:** Sharpe should be between -3 and +3; Drawdown between 0 and 1; CAGR reasonable
5. **DB persistence:** `wfo_results` table should have one row per test window
6. **Train/test separation:** No data from the test window should leak into the training optimization
