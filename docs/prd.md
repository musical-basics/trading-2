# Product Requirements Document (PRD): Level 2 - The Quant Sandbox

## 1. System Overview
This is Level 2 of our 5-Level quantitative trading system. The objective is to upgrade the pipeline from basic technical analysis to institutional fundamental modeling. The system will ingest mixed-frequency data (daily prices + quarterly financials), rank the universe cross-sectionally using statistical Z-Scores, and run strategies through a rigorous Walk-Forward Optimization (WFO) backtester that includes simulated friction (slippage and commissions).

## 2. Core Philosophy: "Institutional Rigor & The Arena"
* **Cross-Sectional Over Time-Series:** Strategies no longer look at assets in isolation. We target the top 10% statistically cheapest stocks relative to their peers on any given day.
* **Eradicate Look-Ahead Bias:** Earnings reports are released weeks after a quarter ends. The system MUST strictly use `pandas.merge_asof` to align fundamental data only on the dates it was actually available to the public.
* **The WFO Arena:** A strategy's paper return is a hallucination without transaction costs. Every strategy must pass through the WFO Tournament with strict slippage deductions before it is allowed to route paper trades.

## 3. Scope of Level 2 Features
* **Expanded Universe:** Increase the universe to 20-50 liquid tickers (e.g., S&P 50 subset) to allow for meaningful cross-sectional Z-score rankings.
* **Mixed-Frequency Data:** Ingesting Quarterly Income Statements and Balance Sheets alongside Daily Bars.
* **The Strategy:** A Cross-Sectional Fundamental Reversion strategy (buying lowest EV/Sales Z-scores).
* **The Backtester:** A rolling Walk-Forward Optimization (WFO) engine.
* **Execution:** Executing a target portfolio weight allocation rather than fixed-share quantities.

## 4. Out of Scope for Level 2
* Machine Learning (XGBoost), Return/Risk Arbitrage Pricing Theory (APT) models (Reserved for Level 3).
* Entity-Component-System (ECS), Polars, DuckDB (Reserved for Levels 4 & 5).
