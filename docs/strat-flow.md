# Level 2 Strategy Sandbox: Walk-Forward Optimization (WFO)

## 1. The Purpose of the Arena
Before any strategy generates live paper trades in Phase 4, it must be evaluated by the Phase 3 WFO System. This proves the logic works Out-of-Sample (OOS). A backtest run over 5 years at once is severely curve-fitted.

## 2. The WFO Loop
The AI Agent must build a backtesting loop that operates on rolling windows over the `cross_sectional_scores` DataFrame:
1. **Window Size:** e.g., 2 Years Train, 1 Year Test.
2. **Step:** Train on the lookback window (e.g., to find the optimal Z-score threshold). Test on the 1-year forward window. 
3. **Roll:** Shift the window forward by 1 year. Repeat until the end of the dataset.
4. **Stitch:** Combine all Out-of-Sample (Test) blocks into a single valid equity curve.

## 3. The Friction System (Mandatory)
A strategy's paper returns are a hallucination without transaction costs. During the WFO simulation, every time a target weight changes (requiring a trade), the system MUST deduct:
* **Slippage:** `0.0005` (5 basis points) of the transaction value.
* **Commissions:** `$0.005` per share (simulated).
*(Implementation: Subtract the friction penalty directly from the `daily_return` on the exact index where a trade occurs).*
