# Level 2 Tech Stack & Infrastructure

## 1. System Architecture
The system remains a sequential pipeline running locally, controlled via Streamlit. However, the data manipulation layer is upgraded to handle institutional-grade matrix operations.

## 2. Infrastructure & Storage
* **Database:** Local `SQLite3` (`data/level2_trading.db`). We will migrate our data to a new v2 database.
* **UI Control Center:** `Streamlit` (Upgraded to show WFO tearsheets, equity curves, and cross-sectional data tables).

## 3. Languages & Core Libraries
* **Language:** Python 3.11+
* **Data Manipulation:** `pandas`, `numpy`.
* **Stats & Math:** `scipy.stats` (Specifically `zscore` for fast cross-sectional ranking).
* **Financial Data:** `yfinance` (Using `.quarterly_financials` and `.quarterly_balance_sheet` to pull fundamental data).
* **Brokerage API:** `alpaca-trade-api`.

## 4. AI Agent Guardrails
* **Pandas Mastery:** The AI MUST utilize `pd.merge_asof` for joining price to fundamentals. Standard `pd.merge` is strictly forbidden for time-series alignment.
* **No `for` loops for math:** You MUST use vectorized Pandas operations. To rank 50 stocks on a specific day, use `df.groupby('date')['metric'].transform(lambda x: (x - x.mean()) / x.std())`.
