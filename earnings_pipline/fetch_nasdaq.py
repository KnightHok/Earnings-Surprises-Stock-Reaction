import os, requests, pandas as pd
from .config import TICKERS, DATA_DIR
from .util import to_utc_naive, ensure_float, et_calendar_date

def main():
    records = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for t in TICKERS:
        try:
            url = f"https://api.nasdaq.com/api/company/{t}/earnings-surprise"
            r = requests.get(url, headers=headers, timeout=10)
            data = r.json()
            if not data or "data" not in data:
                continue
            table = data["data"].get("earningsSurpriseTable", {}).get("rows", [])
            for record in table:
                records.append({
                    "ticker": t,
                    "report_ts": record.get("dateReported"),  # usually "YYYY-MM-DD"
                    "eps_actual": record.get("eps"),
                    "eps_consensus": record.get("consensusForecast"),
                    "amc_bmo": "UNKNOWN"
                })
        except Exception as e:
            print(f"Failed {t}: {e}")

    out = pd.DataFrame(records)
    if not out.empty:
        out = ensure_float(out, ["eps_actual","eps_consensus"]).dropna(subset=["report_ts","ticker"])
    os.makedirs(DATA_DIR, exist_ok=True)
    out.to_csv(f"{DATA_DIR}/nasdaq_events.csv", index=False)
    print(f"wrote {DATA_DIR}/nasdaq_events.csv  rows={len(out)}")

if __name__ == "__main__":
    main()