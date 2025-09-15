import os
import glob
import pandas as pd

from .config import DATA_DIR, RAW_PRICES_DIR

def load_and_clean(path):
    """Load one Nasdaq price CSV and return cleaned DataFrame"""

    df = pd.read_csv(path)

    # Normalize columns
    cols = {c: c.strip() for c in df.columns}
    df = df.rename(columns=cols)

    # Must have these columns
    required_cols = {"Date", "Close/Last"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"{path} missing required columns {required_cols}")

    # get Ticker from file name
    ticker = os.path.splitext(os.path.basename(path))[0].upper()

    # Keep only needed columns
    df = df[["Date","Close/Last"]].copy()

    # Parse date
    df["dt"] = pd.to_datetime(df["Date"], errors="coerce")

    # Clean close price string like "$123.45" or "1,234.56"
    close = (
        df["Close/Last"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df["close"] = pd.to_numeric(close, errors="coerce")

    df["ticker"] = ticker

    return df[["ticker","dt","close"]].dropna()

def main():
    files = glob.glob(os.path.join(RAW_PRICES_DIR, "*.csv"))
    if not files:
        raise SystemExit(f"No CSV files found in {RAW_PRICES_DIR}")
    
    all_dfs = []
    for f in files:
        try:
            sub = load_and_clean(f)
            all_dfs.append(sub)
        except Exception as e:
            print(f"Skipped {f}: {e}")
    if not all_dfs:
        raise SystemExit("No valid files loaded")

    prices = pd.concat(all_dfs, ignore_index=True)
    prices = prices.sort_values(["ticker", "dt"]).reset_index(drop=True)

    # compute daily returns
    prices["ret"] = prices.groupby("ticker")["close"].pct_change()

    os.makedirs(DATA_DIR, exist_ok=True)
    out_path = os.path.join(DATA_DIR, "prices.csv")
    prices.to_csv(out_path, index=False)
    print(f"Wrote {out_path}  rows={len(prices)} tickers={prices['ticker'].nunique()}")

if __name__ == "__main__":
    main()

