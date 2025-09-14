import os, pandas as pd, yfinance as yf
from .config import TICKERS, YF_LIMIT, DATA_DIR
from util import to_utc_naive, infer_amc_bmo_from_ts, ensure_float

def main():
    records = []
    for t in TICKERS:
        try:
            df = yf.Ticker(t).get_earnings_dates(limit=YF_LIMIT)
            if df is None or df.empty:
                continue
            df = df.reset_index()
            df["ticker"] = t

            # Drop rows that are not Earnings
            if "Event Type" in df.columns:
                df = df[df["Event Type"].str.contains("Earnings", case=False, na=False)]

            df.rename(columns={
                "Earnings Date":"report_ts",
                "Reported EPS":"eps_actual",
                "EPS Estimate":"eps_consensus"
            }, inplace=True)
            # Normalize report_ts to UTC naive
            df["report_ts"] = pd.to_datetime(df["report_ts"], utc=True).dt.tz_convert("UTC").dt.tz_localize(None)

            # Apply amc_bmo classification
            df["amc_bmo"] = df["report_ts"].apply(infer_amc_bmo_from_ts)
            
            records.append(df[["ticker","report_ts", "amc_bmo", "eps_actual","eps_consensus"]])
        except Exception as e:
            print(f"Failed {t}: {e}")
    out = pd.concat(records, ignore_index=True if records else pd.DataFrame())
    out = ensure_float(out, ["eps_actual","eps_consensus"]).dropna(subset=["report_ts","ticker"])
    os.makedirs(DATA_DIR, exist_ok=True)

    out.to_csv(f"{DATA_DIR}/yf_events.csv", index=False)
    print(f"wrote {DATA_DIR}/yf_events.csv  rows={len(out)}")

if __name__ == "__main__":
    main()