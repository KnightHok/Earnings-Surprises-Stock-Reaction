-- 000_bootstrap.down.sql
-- Reverse the bootstrap migration

DROP TABLE IF EXISTS eqr.schema_migrations;
DROP SCHEMA IF EXISTS eqr CASCADE;