# Phase 4: Execution — Portfolio Rebalancer — Implementation Plan

## Goal
Replace the Level 1 fixed-share execution system with a target-weight portfolio rebalancer. Phase 4 translates target weights from `cross_sectional_scores` into physical buy/sell orders by computing the delta between desired and actual positions, then applies concentration limits and liquidity gating before routing to Alpaca.

## Architecture References
- [tactical_execution.md](file:///Users/test2/Documents/trading-2/docs/tactical_execution.md) — Full rebalancer spec
- [pipeline_architecture.md](file:///Users/test2/Documents/trading-2/docs/pipeline_architecture.md) — Phase 4 spec

## Proposed Changes

### Portfolio Rebalancer

#### [NEW] `src/pipeline/portfolio_rebalancer.py`

**Main function:** `rebalance_portfolio()`

**Step-by-step logic:**

1. **Load today's target weights from `cross_sectional_scores`:**
   ```python
   SELECT ticker, target_weight
   FROM cross_sectional_scores
   WHERE date = (SELECT MAX(date) FROM cross_sectional_scores)
   ```

2. **Get current portfolio state:**
   - **Live mode:** Query Alpaca API for account equity and current positions
   - **Dry-run mode:** Query `paper_executions` table to reconstruct local portfolio state
   - Returns: `total_equity` (float) and `current_holdings` (dict: `{ticker: shares_held}`)

3. **Apply concentration limits:**
   - Cap each `target_weight` at `MAX_SINGLE_WEIGHT` (0.10)
   - Ensure `sum(all_target_weights) <= 1.0 - CASH_BUFFER` (0.95)
   - If sum exceeds 0.95, proportionally scale down all weights

4. **Calculate the diff (desired vs actual):**
   ```python
   for each ticker with target_weight > 0:
       target_shares = floor((total_equity * target_weight) / current_price)
       current_shares = current_holdings.get(ticker, 0)
       delta = target_shares - current_shares

       if delta > 0:  action = "BUY",  quantity = delta
       if delta < 0:  action = "SELL", quantity = abs(delta)
       if delta == 0: skip
   ```

   For tickers currently held but `target_weight == 0`:
   ```python
   action = "SELL", quantity = current_shares  # Full liquidation
   ```

5. **Liquidity gating (ADV filter):**
   ```python
   # Calculate 30-day Average Daily Volume from daily_bars
   adv_30 = SELECT AVG(volume) FROM daily_bars
             WHERE ticker = ? AND date >= (today - 30 days)

   max_trade_size = floor(adv_30 * ADV_MAX_PCT)  # 1% of ADV

   if quantity > max_trade_size:
       quantity = max_trade_size  # Truncate
       log warning: "Order truncated for {ticker}: {original} → {max_trade_size}"
   ```

6. **Generate order list:**
   - Returns list of `{ticker, action, quantity, price, target_weight}` dicts
   - Sorted: SELLs first (free up capital), then BUYs

---

### Execution Router Update

#### [MODIFY] [execution.py](file:///Users/test2/Documents/trading-2/src/pipeline/execution.py)

**Updates to `route_orders()`:**
- Accept new order format that includes `target_weight`
- Update `paper_executions` table to include `strategy_id = 'ev_sales_v2'`
- Keep backward compatibility with Level 1 order format (check for `target_weight` key)

**New `paper_executions` schema consideration:**
- Add `target_weight` column to `paper_executions` for audit trail
- Or: create a separate `portfolio_snapshots` table (optional, can defer)

---

### Helper: Portfolio State

#### [NEW] `src/pipeline/portfolio_state.py`

Utility module to reconstruct current portfolio from different sources:
- `get_portfolio_from_alpaca()` → queries live API
- `get_portfolio_from_paper()` → reconstructs from `paper_executions` table
- `get_portfolio_state()` → auto-selects based on whether Alpaca keys are configured

---

### Pipeline Integration

#### [MODIFY] [main.py](file:///Users/test2/Documents/trading-2/main.py)
- Add imports: `from src.pipeline import portfolio_rebalancer`
- Replace the old Phase 3+4 calls with:
  ```python
  # Phase 4: Portfolio rebalance → Alpaca
  orders = portfolio_rebalancer.rebalance_portfolio()
  execution.route_orders(orders)
  ```

## Verification Plan

### Automated Tests
1. **Weight cap:** No order's implied weight exceeds 10%
2. **Cash buffer:** Sum of all target weights ≤ 0.95
3. **ADV gating:** No order quantity exceeds 1% of 30-day ADV
4. **Delta correctness:** If currently holding 10 shares and target is 15, order should be BUY 5
5. **Liquidation:** If target_weight = 0 and currently holding shares → full SELL
6. **Dry-run mode:** Works correctly without Alpaca keys
7. **Idempotency:** Running twice produces zero orders (already at target)
