#!/usr/bin/env bash
set -euo pipefail

# ---- config (change these 3 and you're done) --------------------
SERVER="${SERVER:-http://localhost:8080}"   # or http://host.docker.internal:8080
USER="${USER:-web}"
DATASET_ID="${DATASET_ID:-github_issues_1}" # the raw & catalog dataset_id
DATASET_NAME="${DATASET_NAME:-GitHub Issues}"
SOURCE_NAME="${SOURCE_NAME:-GitHub}"
LINEAGE_SRC="${LINEAGE_SRC:-github_issues_raw}" # optional upstream id
# -----------------------------------------------------------------

# Use a throwaway JRE container to run the Trino CLI (no installs needed)
run_trino() {
  local sql="$1"
  docker run --rm -i eclipse-temurin:17-jre sh -lc "
    curl -sL -o trino https://repo1.maven.org/maven2/io/trino/trino-cli/435/trino-cli-435-executable.jar &&
    chmod +x trino &&
    ./trino --server '${SERVER}' --user '${USER}' --output-format ALIGNED --execute \"${sql}\"
  "
}

echo "== 1) CREATE TABLE iceberg.datasets.${DATASET_ID}"
run_trino "
CREATE TABLE IF NOT EXISTS iceberg.datasets.${DATASET_ID} (
  id BIGINT,
  repo VARCHAR,
  number BIGINT,
  title VARCHAR,
  body VARCHAR,
  state VARCHAR,
  author_login VARCHAR,
  assignee_login VARCHAR,
  labels VARCHAR,
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6),
  closed_at TIMESTAMP(6)
) WITH (format='PARQUET');
"

echo "== 2) INSERT catalog_datasets"
run_trino "
INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('${DATASET_ID}','${DATASET_NAME}','${SOURCE_NAME}',current_timestamp,NULL,NULL,current_timestamp);
" || true  # ok if it already exists

echo "== 3) INSERT catalog_columns (auto-generate from information_schema, with retry)"
# Make it idempotent so retries won't duplicate rows
run_trino "DELETE FROM iceberg.catalog.catalog_columns WHERE dataset_id='${DATASET_ID}';" || true

# Retry loop because information_schema can lag briefly after CREATE TABLE
max_tries=10
sleep_sec=1
for try in $(seq 1 $max_tries); do
  out="$(run_trino "
INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT
  '${DATASET_ID}' AS dataset_id,
  c.column_name,
  c.data_type,
  FALSE AS pii_flag,
  NULL  AS null_ratio,
  NULL  AS distinct_ratio,
  CASE
    WHEN c.column_name LIKE '%_id' OR c.column_name IN ('id','number') THEN TRUE
    WHEN c.column_name IN ('created','created_at','updated','updated_at','closed_at') THEN TRUE
    ELSE FALSE
  END AS indexed
FROM information_schema.columns c
WHERE c.table_catalog='iceberg'
  AND c.table_schema='datasets'
  AND c.table_name='${DATASET_ID}'
ORDER BY c.ordinal_position;
")"
  echo "$out"
  if echo "$out" | grep -q 'INSERT: [1-9]'; then
    break
  fi
  if [[ $try -eq $max_tries ]]; then
    echo "ERROR: information_schema never exposed columns for ${DATASET_ID} after $max_tries tries" >&2
    exit 1
  fi
  sleep "$sleep_sec"
done

echo "== 4) INSERT lineage edge (optional)"
run_trino "
INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('${LINEAGE_SRC}','${DATASET_ID}','Replication',current_timestamp);
" || true  # optional; ignore if you don't want an edge

echo "== Done: card '${DATASET_ID}' created."

