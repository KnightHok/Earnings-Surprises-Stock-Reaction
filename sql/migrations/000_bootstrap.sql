-- 000_bootstrap.sql
-- Create schema and a simple ledger to track applied migrations

CREATE SCHEMA IF NOT EXISTS eqr;

CREATE TABLE IF NOT EXISTS eqr.schema_migrations (
  id        TEXT PRIMARY KEY,          -- e.g., '000_bootstrap.sql'
  applied_at TIMESTAMP NOT NULL DEFAULT NOW()
);
