# Level 2 Tactical Execution: The Portfolio Rebalancer

## 1. Transitioning from "Orders" to "Target Weights"
In Level 1, the strategy spit out "Buy 5 shares." In Level 2, Phase 3 spits out `target_weight = 0.10` (I want Apple to be 10% of my total portfolio value). Phase 4 is responsible for translating weights into physical orders.

## 2. The Reconciliation System (The Diff)
Phase 4 must calculate the delta between intent and reality.
1. Query Alpaca API (or local mock state) for total account equity (e.g., $100,000).
2. Query Alpaca API for current shares held (e.g., AAPL: 10 shares @ $150 = $1,500 = 1.5% weight).
3. Query `cross_sectional_scores` for today's target weight (e.g., AAPL target = 0.10).
4. **The Math:** `(Total_Equity * Target_Weight) / Current_Price = Target_Shares`.
5. **The Delta:** `Target_Shares - Current_Shares = Action_Intent` (+ Buy, - Sell).

## 3. Capacity & Concentration Limits
* **Maximum Weight:** No single stock can exceed a 10% portfolio weight.
* **Cash Buffer:** Ensure the sum of all target weights never exceeds 0.95 (leave 5% in cash to prevent margin calls).

## 4. Liquidity Gating (The ADV Filter)
* **The 1% Rule:** Calculate the 30-day Average Daily Volume (ADV) in shares for the ticker from the `daily_bars` table.
* The `quantity` of the proposed trade CANNOT exceed `1%` of the 30-day ADV. If it does, the execution module MUST truncate the order size down to the 1% limit to avoid market impact.
