-- 001_init_tables.down.sql
-- Reverse the initial tables migration

-- Drop tables in reverse order (dependencies first)
DROP TABLE IF EXISTS eqr.event_outcomes;
DROP TABLE IF EXISTS eqr.earnings_events;
DROP TABLE IF EXISTS eqr.prices;
DROP TABLE IF EXISTS eqr.tickers;