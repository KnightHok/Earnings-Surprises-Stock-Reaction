-- =====================================================================
-- Earnings Surprises & Stock Reactions – Database Schema (PostgreSQL)
-- =====================================================================

-- =====================================================================
-- 1) Reference: Tickers
-- =====================================================================

CREATE TABLE IF NOT EXISTS eqr.tickers (
    ticker TEXT PRIMARY KEY,
    name TEXT,
    sector TEXT,
    industry TEXT
);

COMMENT ON TABLE eqr.tickers IS 'Reference table for tickers (name/sector/industry).';

-- =====================================================================
-- 2) Prices: one row per ticker-day
-- =====================================================================

CREATE TABLE IF NOT EXISTS eqr.prices (
    ticker TEXT NOT NULL,
    dt DATE NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    ret DOUBLE PRECISION,
    CONSTRAINT pk_prices PRIMARY KEY (ticker, dt),
    CONSTRAINT fk_prices_ticker FOREIGN KEY (ticker) REFERENCES eqr.tickers(ticker)
);

-- Helpful for time-window scans across many tickers
CREATE INDEX IF NOT EXISTS prices_dt_idx ON eqr.prices (dt);

COMMENT ON TABLE eqr.prices IS 'Daily adjusted close and return per ticker.';
COMMENT ON COLUMN eqr.prices.ret IS 'Close-to-close pct change; NULL for first day per ticker.';

-- =====================================================================
-- 3) Earnings events (facts about the release)
-- =====================================================================

CREATE TABLE IF NOT EXISTS eqr.earnings_events (
    event_id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    report_ts_utc TIMESTAMP NOT NULL, -- stored in UTC (no tz)
    amc_bmo TEXT NOT NULL CHECK (amc_bmo IN ('AMC', 'BMO', 'INTRADAY', 'UNKNOWN')),
    eps_actual DOUBLE PRECISION,
    eps_consensus DOUBLE PRECISION,
    eps_surprise_pct DOUBLE PRECISION,
    et_date DATE NOT NULL, -- Eastern calendar date of the event (for dedupe/joins)
    source TEXT, -- 'yfinance' | 'nasdaq' | etc.
    CONSTRAINT fk_events_ticker FOREIGN KEY (ticker) REFERENCES eqr.tickers(ticker),
    CONSTRAINT uniq_event_by_et_date UNIQUE (ticker, et_date)
);

-- Common lookups: by ticker + exact timestamp
CREATE INDEX IF NOT EXISTS earnings_events_ticker_ts_idx
    ON eqr.earnings_events (ticker, report_ts_utc);

COMMENT ON TABLE eqr.earnings_events IS 'Per-event EPS/consensus/surprise with precise UTC time and ET calendar date.';

-- =====================================================================
-- 4) Precomputed outcomes (fast for BI/analysis)
-- =====================================================================

CREATE TABLE IF NOT EXISTS eqr.event_outcomes (
    event_id BIGINT PRIMARY KEY 
        REFERENCES eqr.earnings_events(event_id) ON DELETE CASCADE,
    ticker TEXT,
    report_ts_utc TIMESTAMP,
    amc_bmo TEXT,
    eps_surprise_pct DOUBLE PRECISION,
    ar_1d DOUBLE PRECISION,
    ar_3d DOUBLE PRECISION,
    ar_1w DOUBLE PRECISION,
    computed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS event_outcomes_ticker_idx
    ON eqr.event_outcomes (ticker);

COMMENT ON TABLE eqr.event_outcomes IS 'Precomputed abnormal returns per earnings event for fast visualization.';

