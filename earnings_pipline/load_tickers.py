import os
import sys
import time
import yfinance as yf
import psycopg2
from itertools import islice
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values, RealDictCursor

# CONFIG
DB_DSN = os.getenv("DB_DSN", "postgresql://root:password@localhost:1515/eqr")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 200))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 8))
BATCH_PAUSE = float(os.getenv("BATCH_PAUSE", 0.1))

# SMALL UTILS

def chunked(iterable, size):
    """Yield lists of up to 'size' items from any iterable (lazy, memory safe).
    Uses itertools.islice to 'take' up to `size` items at a time."""
    if size < 1:
        raise ValueError("size must be >= 1")
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def get_conn():
    """Create a new DB connection. Caller can use `with get_conn() as conn`."""
    return psycopg2.connect(DB_DSN)

# DATABASE OPERATIONS

def insert_missing_symbols(conn, symbols):
    """
    Insert symbols (uppercase) into eqr.tickers, skipping duplicates.
    Uses INSERT ... ON CONFLICT DO NOTHING (atomic + race-safe).
    """
    rows = [(s.upper(),) for s in symbols]
    sql = """
    INSERT INTO eqr.tickers (ticker)
    VALUES %s
    ON CONFLICT (ticker) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

def symbols_needing_enrichment(conn, limit=100000):
    """
    Return tickers where any of name/sector/industry is NULL.
    These are the only rows we'll fetch from yfinance.
    """
    q = """
    SELECT ticker
    FROM eqr.tickers
    WHERE name IS NULL OR sector IS NULL OR industry IS NULL
    ORDER BY ticker
    LIMIT %s"""

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, (limit,))
        return [r["ticker"] for r in cur.fetchall()]
    
def upsert_metadata(conn, items):
    """
    Upsert rows with (ticker, name, sector, industry).
    COALESCE keeps existing non-NULL values when the new value is NULL.
    """
    if not items:
        return
    sql = """
    INSERT INTO eqr.tickers AS t (ticker, name, sector, industry)
    VALUES %s
    ON CONFLICT (ticker) DO UPDATE
        SET name = COALESCE(EXCLUDED.name, t.name),
            sector = COALESCE(EXCLUDED.sector, t.sector),
            industry = COALESCE(EXCLUDED.industry, t.industry)
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, items)
    conn.commit()

# YFINANCE OPERATIONS

def fetch_meta_yfinance(symbols, max_workers=8, pause=0.0):
    # symbols = get_all_tickers()
    out = {}

    def one(sym):
        try:
            info = yf.Ticker(sym).get_info()
        except Exception:
            info = {}

        return sym, {
            "name": info.get("longName") or info.get("shortName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
        }
    
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(one, s) for s in symbols]
        for fut in as_completed(futs):
            sym, rec = fut.result()
            out[sym] = rec
            if pause:
                time.sleep(pause)
    return out

# END TO END PIPELINE

def load_and_enrich(symbols):
    """
    1) Insert any missing tickers.
    2) Select only rows that still need enrichment.
    3) Fetch metadata in batches from yfinance.
    4) Upsert with COALESCE to avoid clobbering existing values.
    """
    symbols = [s.upper() for s in symbols]
    with get_conn() as conn:
        # 1) Add tickers that aren't present yet
        insert_missing_symbols(conn, symbols)

        # 2) Work only on rows that still need data
        to_fill = symbols_needing_enrichment(conn, limit=100000)
        if not to_fill:
            print("Nothing to enrich; all rows are complete.")
            return
        
        total = len(to_fill)
        print(f"{total} ticker(s) need enrichment.")

        #3) Batch fetch + upsert
        done = 0
        for batch in chunked(to_fill, BATCH_SIZE):
            meta = fetch_meta_yfinance(batch, max_workers=MAX_WORKERS, pause=BATCH_PAUSE)
            rows = [
                (
                    sym,
                    (meta.get(sym) or {}).get("name"),
                    (meta.get(sym) or {}).get("sector"),
                    (meta.get(sym) or {}).get("industry"),
                )
                for sym in batch
            ]
            
            upsert_metadata(conn, rows)
            done += len(batch)
            print(f"Enriched {done}/{total}")
            time.sleep(BATCH_PAUSE)


if __name__ == "__main__":
    # Symbols can be passed as CLI args; if none given, use a small demo set
    syms = sys.argv[1:] or ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN"]
    load_and_enrich(syms)