# earnings_pipeline/util.py
import pandas as pd
import numpy as np
from datetime import timezone
import pytz
import os
import glob

ET = pytz.timezone("America/New_York")

def load_csv(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    # try to parse report_ts if exists
    if "report_ts" in df.columns:
        df["report_ts"] = df["report_ts"].apply(to_utc_naive)
    return df

def to_utc_naive(ts):
    """accepts pandas/py datetime (tz-aware or not) and returns UTC naive"""
    if ts is None or pd.isna(ts):
        return None
    ts = pd.to_datetime(ts, utc=True)
    return ts.tz_convert("UTC").tz_localize(None)

def infer_amc_bmo_from_ts(utc_ts):
    """Classy if BMO/AMC from a UTC timestamp by converting to ET with DST."""
    if utc_ts is None or pd.isna(utc_ts):
        return "UNKNOWN"
    # Convert to ET
    et_ts = utc_ts.replace(tzinfo=timezone.utc).astimezone(ET)
    hhmm = et_ts.hour * 60 + et_ts.minute

    if hhmm < 570:
        return "BMO"
    elif hhmm >= 960:
        return "AMC"
    else:
        return "INTRADAY"
    
def eps_surprise_pct(actual, est):
    if pd.isna(actual) or pd.isna(est) or est == 0:
        return np.nan
    return (actual - est) / abs(est)

def add_eps_surprise(df):
    df["eps_surprise_pct"] = np.where(
        (df["eps_consensus"].notna()) & (df["eps_consensus"] != 0),
        (df["eps_actual"] - df["eps_consensus"]) / df["eps_consensus"].abs(),
        np.nan
    )
    return df

def et_calendar_date(utc_ts):
    """Date the market would consider 'that earnings day in ET'"""
    if utc_ts is None or pd.isna(utc_ts):
        return pd.NaT
    et_ts = utc_ts.replace(tzinfo=timezone.utc).astimezone(ET)
    return pd.Timestamp(et_ts.date())

def get_all_tickers(raw_prices_dir="raw_prices"):    
    folder_path = raw_prices_dir

    if not os.path.exists(raw_prices_dir):
        print(f"Warning: {raw_prices_dir} directory not found")
        return []

    # get all tickers
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))

    tickers = []

    for file in all_files:
        ticker = os.path.splitext(os.path.basename(file))[0]
        tickers.append(ticker)

    return tickers

def ensure_float(df, cols):
    df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")
    return df
    
