import os, pandas as pd
from .config import DATA_DIR
from .util import eps_surprise_pct, ET, et_calendar_date, to_utc_naive, load_csv, ensure_float

def merge_events(yf_df, nz_df):
    common_cols = ["ticker","report_ts","amc_bmo","eps_actual","eps_consensus"]

    # normalize columns and add source
    yf = yf_df[common_cols].copy()
    yf["source"] = "yfinance"
    nz = nz_df[common_cols].copy()
    nz["source"] = "nasdaq"

    yf = ensure_float(yf, ["eps_actual","eps_consensus"])
    nz = ensure_float(nz, ["eps_actual","eps_consensus"])

    # ensure datetime & et_date available
    for df in (yf, nz):
        df["report_ts"] = pd.to_datetime(df["report_ts"], utc=True, errors="coerce").dt.tz_convert("UTC").dt.tz_localize(None)
        df["et_date"] = df["report_ts"].apply(et_calendar_date)# Add ET calendar date

    # concat with yfinance first; stable sort so yfinance "wins"
    merged = pd.concat([yf, nz], ignore_index=True, sort=False)
    merged["source"] = pd.Categorical(merged["source"], ["yfinance","nasdaq"], ordered=True)
    merged = merged.sort_values(["ticker","et_date","source","report_ts"], kind="mergesort")
    merged = merged.drop_duplicates(subset=["ticker","et_date"], keep="first")

    # types & cleaning
    merged = merged.astype({
        "ticker": "string",
        "report_ts": "string",
        "amc_bmo": "category",
        "eps_actual": float,
        "eps_consensus": float
    })

    # drop events missing either EPS field
    merged = merged.dropna(subset=["eps_actual","eps_consensus"]).reset_index(drop=True)

    # compute eps_surprise_pct
    # merged["eps_surprise_pct"] = merged["eps_surprise_pct"].apply(eps_surprise_pct)
    merged["eps_surprise_pct"] = merged.apply(lambda row: eps_surprise_pct(row["eps_actual"], row["eps_consensus"]), axis=1)

    # final tidy + save
    merged = merged.sort_values(["report_ts", "ticker"], ascending=[False, True]).reset_index(drop=True)

    out = merged[["ticker","report_ts","amc_bmo","eps_actual","eps_consensus","source","et_date","eps_surprise_pct"]]

    os.makedirs(DATA_DIR, exist_ok=True)
    out.to_csv(os.path.join(DATA_DIR, "events.csv"), index=False)
    print(f"wrote {DATA_DIR}/events.csv  rows={len(out)} (latest on top)")


if __name__ == "__main__":
    yf = load_csv(os.path.join(DATA_DIR, "yf_events.csv"))
    nz = load_csv(os.path.join(DATA_DIR, "nasdaq_events.csv"))
    merge_events(yf, nz)