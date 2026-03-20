# Level 2 Model Architecture: Cross-Sectional Ranking

*Note: The Arbitrage Pricing Theory (APT) factor models are reserved for Level 3. Level 2 focuses on establishing the cross-sectional fundamental ranking paradigm.*

## 1. Core Philosophy: Relative Mispricing
Instead of betting on the absolute direction of a single stock, Level 2 models identify extreme relative mispricings within a cohort. 

## 2. The Fundamental Ranking Model
We evaluate the business via the Enterprise Multiple (EV/Sales).
* **Why EV/Sales?** It is capital-structure neutral (accounts for debt and cash) and works for growth companies with negative earnings.
* **The Heuristic:** A low EV/Sales ratio relative to the cohort implies the market is deeply discounting the company's revenue streams. If the Z-score drops below -1.0 (1 standard deviation cheaper than the mean), it is a statistical outlier (undervalued) and becomes a `BUY` candidate.

## 3. Handling Regime Shifts
Because the Z-score is calculated *cross-sectionally* every single day, the model automatically adjusts to macro regime shifts. If the entire market crashes and all valuations drop, the system won't blindly buy everything; it will only target the assets that crashed *disproportionately* harder than the daily cohort mean.
