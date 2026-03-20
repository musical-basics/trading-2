# Level 2 Database Schema & Math Logic

## 1. Raw Data Tables
### `daily_bars` (Unchanged from Level 1)
* `ticker`, `date`, `open`, `high`, `low`, `close`, `adj_close`, `volume`

### `quarterly_fundamentals` (NEW)
Stores the quarterly financial reports.
* `id` (INTEGER, PK)
* `ticker` (TEXT)
* `period_end_date` (DATE) - *The end of the fiscal quarter.*
* `filing_date` (DATE) - *CRITICAL: This is the date the report was made public. If unavailable via the API, strictly approximate as `period_end_date + 45 days`.*
* `revenue` (REAL)
* `total_debt` (REAL)
* `cash_and_equivalents` (REAL)
* `shares_outstanding` (REAL)

## 2. Computed State Tables
### `cross_sectional_scores` (Replaces strategy_signals)
Stores the daily relative ranking of every stock.
* `id` (INTEGER, PK)
* `ticker` (TEXT)
* `date` (DATE)
* `enterprise_value` (REAL)
* `ev_to_sales` (REAL)
* `ev_sales_zscore` (REAL) - *The cross-sectional ranking.*
* `target_weight` (REAL) - *Desired portfolio allocation (e.g., 0.10 for 10%).*

## 3. WFO Tables
### `wfo_results` (NEW)
Stores the metrics of the Walk-Forward Backtester.
* `id` (INTEGER, PK)
* `strategy_id` (TEXT)
* `test_window_start` (DATE)
* `test_window_end` (DATE)
* `sharpe_ratio` (REAL)
* `max_drawdown` (REAL)
* `cagr` (REAL)

## 4. Mathematical Formulas
* **Enterprise Value (EV):** `(adj_close * shares_outstanding) + total_debt - cash_and_equivalents`
* **EV/Sales:** `Enterprise Value / (revenue * 4)` *(Assuming revenue is quarterly, annualized)*
* **Z-Score:** $Z = \frac{X - \mu}{\sigma}$ (Calculated cross-sectionally per day across the active universe).
