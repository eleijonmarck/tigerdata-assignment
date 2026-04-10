#!/bin/bash
set -e

echo "Starting data ingestion from CSV..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    \COPY cpu_usage FROM '/docker-entrypoint-initdb.d/cpu_usage.csv' CSV HEADER;
EOSQL

echo "Data ingestion complete."
