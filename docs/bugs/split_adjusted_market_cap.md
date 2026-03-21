# Split-Adjusted Market Cap Bug

**Date**: 2026-03-20  
**Severity**: Critical — data corruption  
**File**: `src/pipeline/scoring/cross_sectional_scoring.py`

## Bug

Enterprise Value was calculated using `adj_close * shares_outstanding`. However:
- `adj_close` is split-adjusted backward by yfinance (e.g., AAPL 2018 price divided by 4)
- `shares_outstanding` from SEC 10-Q filings is the raw historical count

This produced wildly wrong market caps for any stock that has split, corrupting all EV/Sales Z-scores.

## Fix

Changed to `close * shares_outstanding` for market cap.  
`adj_close` is still used for daily return calculations in backtests.

## Affected Tickers (examples)

| Ticker | Split | Date | Pre-fix Error |
|--------|-------|------|---------------|
| AAPL | 4:1 | Aug 2020 | 75% too small |
| TSLA | 5:1 | Aug 2020 | 80% too small |
| GOOGL | 20:1 | Jul 2022 | 95% too small |
| AMZN | 20:1 | Jun 2022 | 95% too small |
