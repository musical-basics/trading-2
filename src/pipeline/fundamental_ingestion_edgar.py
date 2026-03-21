"""
fundamental_ingestion_edgar.py — Quarterly Fundamental Ingestion via SEC EDGAR.

Uses the SEC EDGAR XBRL companyfacts API — truly free, no API key needed.
Provides 10-20+ years of quarterly fundamentals (actual SEC filings).

Same output schema as fundamental_ingestion.py — writes to quarterly_fundamentals table.
"""

import sqlite3
import time
import requests
import pandas as pd
from datetime import timedelta
from src.config import DB_PATH, DEFAULT_UNIVERSE, FILING_DELAY_DAYS

EDGAR_BASE = "https://data.sec.gov/api/xbrl/companyfacts"
EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
HEADERS = {"User-Agent": "TradingResearch research@example.com"}

# XBRL tags vary across companies — try multiple fallbacks
REVENUE_TAGS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
]
DEBT_TAGS = ["LongTermDebt", "LongTermDebtNoncurrent", "DebtCurrent"]
CASH_TAGS = ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsAndShortTermInvestments"]
SHARES_TAGS = ["CommonStockSharesOutstanding", "WeightedAverageNumberOfShareOutstandingBasicAndDiluted"]


def _get_cik_map():
    """Download ticker → CIK mapping from SEC."""
    resp = requests.get(EDGAR_TICKERS_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    cik_map = {}
    for _, entry in data.items():
        cik_map[entry["ticker"]] = str(entry["cik_str"]).zfill(10)
    return cik_map


def _extract_quarterly(facts, tags, unit_key="USD"):
    """Extract quarterly 10-Q values from XBRL facts, trying multiple tags."""
    gaap = facts.get("facts", {}).get("us-gaap", {})
    for tag in tags:
        concept = gaap.get(tag, {})
        units = concept.get("units", {})
        entries = units.get(unit_key, [])
        # Filter to 10-Q filings only, deduplicate by end date
        quarterly = {}
        for e in entries:
            if e.get("form") == "10-Q" and e.get("end"):
                end_date = e["end"]
                # Keep the most recent filing for each period end
                if end_date not in quarterly or e.get("filed", "") > quarterly[end_date].get("filed", ""):
                    quarterly[end_date] = e
        if quarterly:
            return quarterly
    return {}


def ingest_fundamentals_edgar(tickers=None):
    """
    Fetch quarterly fundamentals from SEC EDGAR and upsert into quarterly_fundamentals.
    No API key needed — truly free.
    """
    if tickers is None:
        tickers = DEFAULT_UNIVERSE

    print("=" * 60)
    print("PHASE 1b: Quarterly Fundamental Ingestion (SEC EDGAR)")
    print("=" * 60)

    # Step 1: Get CIK mapping
    print("  Loading SEC ticker → CIK mapping...", end=" ")
    try:
        cik_map = _get_cik_map()
        print(f"✓ {len(cik_map)} tickers mapped.")
    except Exception as e:
        print(f"FAILED: {e}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_inserted = 0
    failed_tickers = []

    for i, ticker in enumerate(tickers):
        # SEC rate limit: max 10 requests/sec
        if i > 0 and i % 8 == 0:
            time.sleep(1.2)

        cik = cik_map.get(ticker)
        if not cik:
            print(f"  {ticker}: CIK not found. Skipping.")
            failed_tickers.append(ticker)
            continue

        try:
            print(f"  Fetching {ticker} (CIK {cik})...", end=" ")

            resp = requests.get(f"{EDGAR_BASE}/CIK{cik}.json", headers=HEADERS, timeout=15)
            resp.raise_for_status()
            facts = resp.json()

            # Extract quarterly data for each field
            revenue_q = _extract_quarterly(facts, REVENUE_TAGS, "USD")
            debt_q = _extract_quarterly(facts, DEBT_TAGS, "USD")
            cash_q = _extract_quarterly(facts, CASH_TAGS, "USD")
            shares_q = _extract_quarterly(facts, SHARES_TAGS, "shares")

            # Merge all dates
            all_dates = set(revenue_q.keys()) | set(debt_q.keys()) | set(cash_q.keys()) | set(shares_q.keys())
            # Only keep dates that have at least revenue
            if revenue_q:
                all_dates = set(revenue_q.keys())
            elif cash_q:
                all_dates = set(cash_q.keys())

            ticker_rows = 0
            for end_date in sorted(all_dates):
                try:
                    period_end_date = pd.Timestamp(end_date)
                    filing_date = period_end_date + timedelta(days=FILING_DELAY_DAYS)

                    revenue = revenue_q.get(end_date, {}).get("val")
                    debt = debt_q.get(end_date, {}).get("val")
                    cash = cash_q.get(end_date, {}).get("val")
                    shares = shares_q.get(end_date, {}).get("val")

                    cursor.execute("""
                        INSERT OR REPLACE INTO quarterly_fundamentals
                        (ticker, period_end_date, filing_date, revenue,
                         total_debt, cash_and_equivalents, shares_outstanding)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker,
                        period_end_date.strftime("%Y-%m-%d"),
                        filing_date.strftime("%Y-%m-%d"),
                        revenue, debt, cash, shares,
                    ))
                    ticker_rows += 1
                except Exception:
                    continue

            conn.commit()
            total_inserted += ticker_rows
            print(f"✓ {ticker_rows} quarters stored.")

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print("RATE LIMITED — waiting 5s...")
                time.sleep(5)
            else:
                print(f"FAILED: {e}")
            failed_tickers.append(ticker)
        except Exception as e:
            print(f"FAILED: {e}")
            failed_tickers.append(ticker)

    conn.commit()
    conn.close()

    print()
    print(f"  ✓ Total quarterly records upserted: {total_inserted}")
    if failed_tickers:
        print(f"  ⚠ Failed tickers: {', '.join(failed_tickers)}")
    else:
        print(f"  ✓ All {len(tickers)} tickers fetched successfully.")
    print()


if __name__ == "__main__":
    ingest_fundamentals_edgar(tickers=["AAPL", "MSFT"])
