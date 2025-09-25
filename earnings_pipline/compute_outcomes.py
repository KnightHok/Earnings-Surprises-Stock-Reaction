import os
import psycopg2
import pandas as pd
from datetime import timedelta

DB_URL = os.getenv("DB_URL", "postgres://root:password@localhost:1515/eqr?sslmode=disable")

def get_events(conn):
    """Fetch all earnings events that don't yet have outcomes."""
    sql = """
        SELECT e.event_id, e.ticker, e.report_ts_utc, e.amc_bmo, e.eps_surprise_pct, e.et_date
        FROM eqr.earnings_events e
        LEFT JOIN eqr.event_outcomes o ON e.event_id = o.event_id
        WHERE o.event_id IS NULL; -- only unfinished ones
    """

    return pd.read_sql(sql, conn, parse_dates=["report_ts_utc", "et_date"])

def get_prices(conn, tickers):
    """Fetch prices for tickers + SPY, with daily returns already in schema."""
    sql = """
        SELECT ticker, dt, ret
        FROM eqr.prices
        WHERE ticker = ANY(%s)
        ORDER BY ticker, dt;
    """
    return pd.read_sql(sql, conn, params=(tickers,), parse_dates=["dt"])

def compute_outcomes(events, prices):
    """Compute AR_1D, AR_3D, AR_1W for each event."""
    out = []
    tickers = events["ticker"].unique()
    prices = prices.pivot(index="dt", columns="ticker", values="ret")
    if "SPY" not in prices.columns:
        raise ValueError("SPY must be present in prices for abnormal returns")
    
    for ev in events.itertuples(index=False):
        ticker, ts, amc_bmo, s, et_date, event_id = (
            ev.ticker, ev.report_ts_utc, ev.amc_bmo, ev.eps_surprise_pct, ev.et_date, ev.event_id
        )
        # Decide day0
        day0 = et_date
        if amc_bmo == "AMC": # next trading day
            next_dates = prices.index[prices.index > pd.Timestamp(et_date)]
            if len(next_dates) == 0:
                continue
            day0 = next_dates.min()
        if day0 not in prices.index:
            continue # skip if no price that day

        # window
        window = prices.loc[day0: day0 + timedelta(days=7)] # gives a week horizon
        if ticker not in window.columns:
            continue

        # abnormal returns with bounds checking
        if len(window) < 1:
            continue # skip if no trading days
        ar_1d = window[ticker].iloc[0] - window["SPY"].iloc[0]
        ar_3d = None
        if len(window) >= 3:
            ar_3d = window[ticker].iloc[:3].sum() - window["SPY"].iloc[:3].sum()
        
        ar_1w = None
        if len(window) >= 5:
            ar_1w = window[ticker].iloc[:5].sum() - window["SPY"].iloc[:5].sum()

        out.append({
            "event_id": event_id,
            "ticker": ticker,
            "report_ts_utc": ts,
            "amc_bmo": amc_bmo,
            "eps_surprise_pct": s,
            "ar_1d": ar_1d,
            "ar_3d": ar_3d,
            "ar_1w": ar_1w
        })
        
    return pd.DataFrame(out)

def upsert_outcomes(conn, df):
    """Upsert into eqr.event_outcomes."""
    if df.empty:
        print("No outcomes to insert")
        return
    
    with conn.cursor() as cur:
        for row in df.itertuples(index=False):
            cur.execute("""
                INSERT INTO eqr.event_outcomes
                        (event_id,ticker,report_ts_utc,amc_bmo,eps_surprise_pct,ar_1d,ar_3d,ar_1w,computed_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (event_id) DO UPDATE SET
                            ticker = EXCLUDED.ticker,
                            report_ts_utc = EXCLUDED.report_ts_utc,
                            amc_bmo = EXCLUDED.amc_bmo,
                            eps_surprise_pct = EXCLUDED.eps_surprise_pct,
                            ar_1d = EXCLUDED.ar_1d,
                            ar_3d = EXCLUDED.ar_3d,
                            ar_1w = EXCLUDED.ar_1w,
                            computed_at = NOW();
                        """,
                        (
                            row.event_id, row.ticker, row.report_ts_utc,
                            row.amc_bmo, row.eps_surprise_pct,
                            row.ar_1d, row.ar_3d, row.ar_1w
                        )
            )
    conn.commit()
    print(f"Upserted {len(df)} rows into eqr.event_outcomes")

def main():
    with psycopg2.connect(DB_URL) as conn:
        events = get_events(conn)
        if events.empty:
            print("No events to compute")
            return
        tickers = list(events["ticker"].unique()) + ["SPY"]
        prices = get_prices(conn, tickers)
        outcomes = compute_outcomes(events, prices)
        upsert_outcomes(conn, outcomes)

if __name__ == "__main__":
    main()