PG_CONTAINER ?= eqr_postgres
PG_IMAGE     ?= postgres:16-alpine
MIGRATIONS_DIR ?= sql/migrations

DB_USER     ?= root
DB_PASSWORD ?= password
DB_NAME     ?= eqr
DB_PORT     ?= 1515

MIGRATE_BIN ?= migrate

DB_URL ?= postgres://$(DB_USER):$(DB_PASSWORD)@localhost:$(DB_PORT)/$(DB_NAME)?sslmode=disable

help: ## Show this help message
	@echo "Earnings Project - Available Commands:"
	@echo ""
	@echo "Database Setup:"
	@echo "  postgres       - Start PostgreSQL container"
	@echo "  stop_postgres  - Stop and remove PostgreSQL container"
	@echo "  createdb       - Create database inside container"
	@echo ""
	@echo "Database Migrations:"
	@echo "  newmigration name=<name> - Create new migration file"
	@echo "  migrateup      - Run all pending migrations"
	@echo "  migratedown    - Rollback last migration"
	@echo ""
	@echo "Data Loading:"
	@echo "  upsert-prices  - Load data/prices.csv into prices table"
	@echo "  upsert-events  - Load data/events.csv into earnings_events table"
	@echo ""
	@echo "Configuration:"
	@echo "  DB_PORT=$(DB_PORT) DB_NAME=$(DB_NAME) DB_USER=$(DB_USER)"

postgres:
	docker run --name $(PG_CONTAINER) -p $(DB_PORT):5432 -e POSTGRES_USER=$(DB_USER) -e POSTGRES_PASSWORD=$(DB_PASSWORD) -d $(PG_IMAGE)

stop_postgres:
	docker stop $(PG_CONTAINER) || true
	docker rm $(PG_CONTAINER) || true

createdb:
	docker exec -it $(PG_CONTAINER) createdb --username=$(DB_USER) --owner=$(DB_USER) eqr

newmigration:
	$(if $(name),,$(error usage: make newmigration name=your_change))
	$(MIGRATE_BIN) create -ext sql -dir $(MIGRATIONS_DIR) -seq $(name)

migrateup:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" -verbose up

migratedown:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" -verbose down 1

# --- Staging + UPSERT loaders (use these by default) ---
# Expect files at: data/prices.csv and data/events.csv

upsert-prices:
# 1) ensure staging table exists (no constraints)
	psql "$(DB_URL)" -v ON_ERROR_STOP=1 -c "CREATE TABLE IF NOT EXISTS eqr._stage_prices (LIKE eqr.prices INCLUDING DEFAULTS EXCLUDING CONSTRAINTS);"
# 2) empty staging table
	psql "$(DB_URL)" -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE eqr._stage_prices;"
# 3) load CSV into staging table
	psql "$(DB_URL)" -v ON_ERROR_STOP=1 -c "\copy eqr._stage_prices(ticker,dt,close,ret) FROM 'data/prices.csv' CSV HEADER"
# 4) upser to main table / target
	psql "$(DB_URL)" -v ON_ERROR_STOP=1 -c "\
	INSERT INTO eqr.prices (ticker,dt,close,ret) \
	SELECT ticker,dt,close,ret FROM eqr._stage_prices \
	ON CONFLICT (ticker,dt) DO UPDATE SET \
		close = EXCLUDED.close, \
		ret = EXCLUDED.ret;"

upsert-events:
# 1) ensure staging table exists (no constraints)
	psql "$(DB_URL)" -v ON_ERROR_STOP=1 -c "CREATE TABLE IF NOT EXISTS eqr._stage_events (LIKE eqr.earnings_events INCLUDING DEFAULTS EXCLUDING CONSTRAINTS);"
# 2) empty staging table
	psql "$(DB_URL)" -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE eqr._stage_events;"
# 3) load CSV into staging table
	psql "$(DB_URL)" -v ON_ERROR_STOP=1 -c "\
	\copy eqr._stage_events(ticker,report_ts_utc,amc_bmo,eps_actual,eps_consensus,eps_surprise_pct,et_date,source) \
		FROM 'data/events.csv' CSV HEADER"
# 4) upsert ot target using your unique (ticker, et_date)
	psql "$(DB_URL)" -v ON_ERROR_STOP=1 -c "\
	INSERT INTO eqr.earnings_events \
		(ticker,report_ts_utc,amc_bmo,eps_actual,eps_consensus,eps_surprise_pct,et_date,source) \
	SELECT ticker,report_ts_utc,amc_bmo,eps_actual,eps_consensus,eps_surprise_pct,et_date,source \
	FROM eqr._stage_events \
	ON CONFLICT (ticker, et_date) DO UPDATE SET \
		eps_actual = EXCLUDED.eps_actual, \
		eps_consensus = EXCLUDED.eps_consensus, \
		eps_surprise_pct = EXCLUDED.eps_surprise_pct, \
		report_ts_utc = EXCLUDED.report_ts_utc, \
		amc_bmo = EXCLUDED.amc_bmo, \
		source = EXCLUDED.source;"

.PHONY: help postgres stop_postgres newmigration migrateup migratedown upsert-prices upsert-events

