# Phase 2: The Math Desk — Implementation Plan

## Goal
Build the cross-sectional scoring engine that fuses daily prices with quarterly fundamentals using `pd.merge_asof` (to prevent look-ahead bias), computes Enterprise Value and EV/Sales, ranks the universe via daily Z-scores, and generates target portfolio weights.

> [!IMPORTANT]
> This is the most critical engineering phase. The `merge_asof(direction='backward')` join is the foundation that prevents look-ahead bias across the entire system.

## Architecture References
- [pipeline_architecture.md](file:///Users/test2/Documents/trading-2/docs/pipeline_architecture.md) — Phase 2 spec
- [datastructure.md](file:///Users/test2/Documents/trading-2/docs/datastructure.md) — `cross_sectional_scores` schema + math formulas
- [model_architecture.md](file:///Users/test2/Documents/trading-2/docs/model_architecture.md) — Ranking philosophy
- [stack.md](file:///Users/test2/Documents/trading-2/docs/stack.md) — Vectorization guardrails

## Proposed Changes

### Scoring Engine

#### [NEW] `src/pipeline/cross_sectional_scoring.py`

**Main function:** `compute_cross_sectional_scores()`

**Step-by-step logic:**

1. **Load data from SQLite:**
   - `daily_bars` → DataFrame with columns: `ticker, date, adj_close, volume`
   - `quarterly_fundamentals` → DataFrame with columns: `ticker, filing_date, revenue, total_debt, cash_and_equivalents, shares_outstanding`

2. **Bias-free data alignment — `pd.merge_asof`:**
   ```python
   # Sort both DataFrames by their join keys
   prices_df = prices_df.sort_values(['ticker', 'date'])
   fundamentals_df = fundamentals_df.sort_values(['ticker', 'filing_date'])

   # merge_asof: for each (ticker, date), find the most recent filing_date <= date
   merged = pd.merge_asof(
       prices_df,
       fundamentals_df,
       left_on='date',
       right_on='filing_date',
       by='ticker',
       direction='backward'
   )
   ```
   This guarantees we only use fundamental data that was publicly available at the time.

3. **Compute Enterprise Value (vectorized):**
   ```
   EV = (adj_close * shares_outstanding) + total_debt - cash_and_equivalents
   ```

4. **Compute EV/Sales (vectorized):**
   ```
   EV_Sales = EV / (revenue * 4)    # Quarterly revenue annualized
   ```

5. **Cross-sectional Z-score (vectorized `groupby`):**
   ```python
   merged['ev_sales_zscore'] = merged.groupby('date')['ev_to_sales'].transform(
       lambda x: (x - x.mean()) / x.std()
   )
   ```
   - On any given day, this ranks all 51 tickers relative to each other
   - Automatically adjusts for macro regime shifts

6. **Generate target weights:**
   ```python
   merged['target_weight'] = 0.0
   buy_mask = merged['ev_sales_zscore'] < ZSCORE_BUY_THRESHOLD  # < -1.0

   # Count how many BUY candidates per day
   buy_counts = merged[buy_mask].groupby('date')['ticker'].transform('count')

   # Equal weight among BUY candidates, capped at MAX_SINGLE_WEIGHT
   merged.loc[buy_mask, 'target_weight'] = np.minimum(
       1.0 / buy_counts,
       MAX_SINGLE_WEIGHT
   )
   ```
   - Respects `MAX_SINGLE_WEIGHT = 0.10` cap
   - Sum of weights is implicitly ≤ 1.0

7. **Drop rows without fundamental data** (pre-first-filing dates will have NaN)

8. **Upsert to `cross_sectional_scores` table:**
   - Columns: `ticker, date, enterprise_value, ev_to_sales, ev_sales_zscore, target_weight`
   - Use `INSERT OR REPLACE` with `UNIQUE(ticker, date)` constraint

> [!WARNING]
> **NO `for` loops for math.** All EV, EV/Sales, Z-score, and weight calculations MUST use vectorized pandas/numpy operations per the stack.md guardrails. The only `for` loop allowed is for the DB upsert (or use `df.to_sql`).

---

### Pipeline Integration

#### [MODIFY] [main.py](file:///Users/test2/Documents/trading-2/main.py)
- Add import: `from src.pipeline import cross_sectional_scoring`
- Add Phase 2 call: `cross_sectional_scoring.compute_cross_sectional_scores()`
- Place after `data_ingestion.ingest()` and `fundamental_ingestion.ingest_fundamentals()`

## Verification Plan

### Automated Tests
1. **Bias check:** For every row in `cross_sectional_scores`, verify that the corresponding `filing_date` from `quarterly_fundamentals` is ≤ the row's `date`
2. **Z-score sanity:** On any given date, confirm the mean of `ev_sales_zscore` ≈ 0.0 and std ≈ 1.0
3. **Weight constraints:** Confirm no `target_weight > 0.10` and sum of weights per day ≤ 0.95
4. **Row count:** Expect ~(trading_days × tickers_with_fundamentals) rows
5. **NaN check:** Confirm no NaN values in EV, EV/Sales, or Z-score columns for rows that have corresponding fundamental data
