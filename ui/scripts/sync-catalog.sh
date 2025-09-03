#!/usr/bin/env bash
# Rebuild iceberg.catalog.catalog_datasets + catalog_columns from what's ACTUALLY in iceberg.datasets
# Usage:
#   chmod +x ui/scripts/sync-catalog.sh
#   TRINO_CONTAINER=trino-fg ui/scripts/sync-catalog.sh

set -euo pipefail

CONTAINER="${TRINO_CONTAINER:-trino-fg}"
CATALOG="iceberg"
DATA_SCHEMA="datasets"
META_SCHEMA="catalog"

echo "Syncing ${CATALOG}.${META_SCHEMA} to match ${CATALOG}.${DATA_SCHEMA} (container=${CONTAINER})..."

# 0) Ensure meta tables exist
docker exec -i "$CONTAINER" trino --execute "
CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${META_SCHEMA};

CREATE TABLE IF NOT EXISTS ${CATALOG}.${META_SCHEMA}.catalog_datasets (
  dataset_id        VARCHAR,
  name              VARCHAR,
  source            VARCHAR,
  created_at        TIMESTAMP,
  row_count         BIGINT,
  size_bytes        BIGINT,
  last_profiled_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ${CATALOG}.${META_SCHEMA}.catalog_columns (
  dataset_id     VARCHAR,
  column_name    VARCHAR,
  data_type      VARCHAR,
  pii_flag       BOOLEAN,
  null_ratio     DOUBLE,
  distinct_ratio DOUBLE
);
"

# 1) Snapshot current tables
echo "→ Listing ${CATALOG}.${DATA_SCHEMA} tables..."
TABLES=$(docker exec -i "$CONTAINER" trino \
  --catalog "$CATALOG" --schema information_schema \
  --output-format TSV \
  --execute "SELECT table_name FROM tables WHERE table_schema='${DATA_SCHEMA}' ORDER BY 1" \
  | tail -n +2 || true)

if [[ -z "${TABLES// }" ]]; then
  echo "No physical tables found under ${CATALOG}.${DATA_SCHEMA}. Cleaning catalog to empty…"
  docker exec -i "$CONTAINER" trino --execute "
    DELETE FROM ${CATALOG}.${META_SCHEMA}.catalog_columns;
    DELETE FROM ${CATALOG}.${META_SCHEMA}.catalog_datasets;
  "
  echo "✓ Catalog now empty."
  exit 0
fi

echo "Found tables:"
echo "$TABLES"

# 2) Clean meta tables (hard reset)
echo "→ Truncating catalog tables…"
docker exec -i "$CONTAINER" trino --execute "
  DELETE FROM ${CATALOG}.${META_SCHEMA}.catalog_columns;
  DELETE FROM ${CATALOG}.${META_SCHEMA}.catalog_datasets;
"

# 3) Rebuild catalog_datasets
echo "→ Rebuilding ${CATALOG}.${META_SCHEMA}.catalog_datasets…"
docker exec -i "$CONTAINER" trino --catalog "$CATALOG" --schema "$DATA_SCHEMA" --execute "
INSERT INTO ${CATALOG}.${META_SCHEMA}.catalog_datasets
SELECT
  -- dataset_id: ds_<table_name>
  CONCAT('ds_', t.table_name)                   AS dataset_id,
  t.table_name                                  AS name,
  'Demo Generator'                              AS source,
  current_timestamp                             AS created_at,
  COALESCE(cnt.cnt, 0)                          AS row_count,
  4096                                          AS size_bytes,
  current_timestamp                             AS last_profiled_at
FROM (
  SELECT table_name FROM ${CATALOG}.information_schema.tables
  WHERE table_schema='${DATA_SCHEMA}'
) t
LEFT JOIN LATERAL (
  SELECT COUNT(*) AS cnt FROM ${CATALOG}.${DATA_SCHEMA}."%s"
) cnt ON TRUE
".replace("%s","dummy")
" >/dev/null 2>&1 || true

# The above INSERT can’t reference %s via replace; do it per table:
while IFS=$'\t' read -r T; do
  [[ -z "$T" ]] && continue
  docker exec -i "$CONTAINER" trino --catalog "$CATALOG" --schema "$DATA_SCHEMA" --execute "
    INSERT INTO ${CATALOG}.${META_SCHEMA}.catalog_datasets
    SELECT
      CONCAT('ds_', '${T}'),
      '${T}',
      'Demo Generator',
      current_timestamp,
      (SELECT COUNT(*) FROM ${CATALOG}.${DATA_SCHEMA}.\"${T}\"),
      4096,
      current_timestamp
  "
done <<< "$TABLES"

# 4) Rebuild catalog_columns
echo "→ Rebuilding ${CATALOG}.${META_SCHEMA}.catalog_columns…"
docker exec -i "$CONTAINER" trino --catalog "$CATALOG" --schema information_schema --execute "
INSERT INTO ${CATALOG}.${META_SCHEMA}.catalog_columns
(dataset_id, column_name, data_type, pii_flag, null_ratio, distinct_ratio)
SELECT
  CONCAT('ds_', c.table_name)                             AS dataset_id,
  c.column_name,
  c.data_type,
  CASE WHEN regexp_like(lower(c.column_name), '(email|phone|ssn|dob|address)') THEN TRUE ELSE FALSE END AS pii_flag,
  0.0,
  0.0
FROM ${CATALOG}.information_schema.columns c
WHERE c.table_schema='${DATA_SCHEMA}'
ORDER BY 1,2
"

echo "✓ Sync complete. Refresh the GUI."
