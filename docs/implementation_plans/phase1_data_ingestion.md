# Phase 1: Data Ingestion — Implementation Plan

> **Status:** ✅ Complete. This document records the work already done for reference.

## Goal
Upgrade the data pipeline from daily-only to mixed-frequency data (daily bars + quarterly fundamentals) for the expanded 51-ticker universe.

## Changes Made

---

### Configuration

#### [MODIFY] [config.py](file:///Users/test2/Documents/trading-2/src/config.py)
- `DB_PATH` → `data/level2_trading.db`
- `DEFAULT_UNIVERSE` → 51 tickers (S&P 50 subset + SPY/QQQ)
- Added constants: `SLIPPAGE_BPS`, `COMMISSION_PER_SHARE`, `MAX_SINGLE_WEIGHT`, `CASH_BUFFER`, `ZSCORE_BUY_THRESHOLD`, `FILING_DELAY_DAYS`, `ADV_LOOKBACK`, `ADV_MAX_PCT`

---

### Database

#### [MODIFY] [db_init.py](file:///Users/test2/Documents/trading-2/src/pipeline/db_init.py)
- Added 3 new tables: `quarterly_fundamentals`, `cross_sectional_scores`, `wfo_results`
- Level 1 tables preserved for backward compatibility

---

### Fundamental Ingestion

#### [NEW] [fundamental_ingestion.py](file:///Users/test2/Documents/trading-2/src/pipeline/fundamental_ingestion.py)
- Fetches `.quarterly_financials` + `.quarterly_balance_sheet` from yfinance
- Extracts: revenue, total_debt, cash_and_equivalents, shares_outstanding
- Calculates `filing_date = period_end_date + 45 days` (SEC filing delay proxy)
- `_safe_get()` handles variable yfinance field names across tickers
- Idempotent upsert via `INSERT OR REPLACE`

---

### Dependencies

#### [MODIFY] [requirements.txt](file:///Users/test2/Documents/trading-2/requirements.txt)
- Added `scipy`

## Verification
- ✅ Config: 51 tickers, all constants correct
- ✅ DB: All 7 tables created in `level2_trading.db`
- ✅ Ingestion: AAPL + MSFT → 5 quarters each, filing_date = period_end + 45d confirmed
