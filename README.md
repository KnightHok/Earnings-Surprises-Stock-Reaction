# Earnings Surprises & Stock Reactions
## 1. Project Overview
This project analyzes how compaanies' quarterly earnings surpires (EPS actual vs. analyst consunsus) relate to short term stock returns. It implements a repeatable, auditable pipeline that mirrows real buy side and sell side research workflows:
* Ingest data from Yahoo Finance (yfinance) and Nasdaq API
* Clean, deduplicate, and classify events as BMO (before market open) or AMC (after market close)
* Align events with stock price data to compute abnormal returns (returns vs. SPY benchmark)
* Visualize results in Power BI dashboards

## 2. Business Question
* Do stocks with positive EPS surpireses outperform those with negative surprieses in the days after the earnings?
* Does timing of release (AMC vs. BMO) change the market reaction?
* Are reactions consistent across sectors?

## 3. Data Sources
* Yahoo Finance (yfinance): EPS actual, EPS consunsus, event timestamps
* Nasdaq API: supplemental EPS/revenue data for events missing in Yahoo
* Yahoo Finance Prices: daily close data for all tickers + SPY (benchmark)