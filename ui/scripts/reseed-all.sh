#!/usr/bin/env bash
# Rebuild demo data AND catalog in one go using Trino CLI inside container.
# Works on macOS bash 3.2 (no associative arrays).
#
# Usage:
#   chmod +x ui/scripts/reseed-all.sh
#   TRINO_CONTAINER=trino-fg ENTITIES=30 ROWS=1500 ui/scripts/reseed-all.sh
#
# Env (optional):
#   TRINO_CONTAINER  default: trino-fg
#   CATALOG          default: iceberg
#   DATA_SCHEMA      default: datasets
#   META_SCHEMA      default: catalog
#   ENTITIES         default: 20
#   ROWS             default: 1000
#   WAREHOUSE        default: s3://iceberg-warehouse

set -euo pipefail

CONTAINER="${TRINO_CONTAINER:-trino-fg}"
CATALOG="${CATALOG:-iceberg}"
DATA_SCHEMA="${DATA_SCHEMA:-datasets}"
META_SCHEMA="${META_SCHEMA:-catalog}"
ENTITIES="${ENTITIES:-20}"
ROWS="${ROWS:-1000}"
WAREHOUSE="${WAREHOUSE:-s3://iceberg-warehouse}"

echo "== reseed-all =="
echo "container=${CONTAINER}"
echo "catalog=${CATALOG}, data_schema=${DATA_SCHEMA}, meta_schema=${META_SCHEMA}"
echo "entities=${ENTITIES}, rows=${ROWS}"
echo "warehouse=${WAREHOUSE}"
echo

run_trino() {
  local CAT="$1"; shift
  local SCH="$1"; shift
  local SQL="$*"
  docker exec -i "${CONTAINER}" trino \
    --catalog "${CAT}" \
    --schema  "${SCH}" \
    --execute "${SQL}"
}

# Ensure schemas exist
echo "[ensure] ${CATALOG}.${META_SCHEMA} & ${CATALOG}.${DATA_SCHEMA}"
run_trino "${CATALOG}" "information_schema" "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${META_SCHEMA}"
run_trino "${CATALOG}" "information_schema" "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${DATA_SCHEMA}"

# Drop ALL tables in data schema
echo "[drop] all tables in ${CATALOG}.${DATA_SCHEMA}"
TABLES="$(run_trino "${CATALOG}" "information_schema" \
  "SELECT table_name FROM tables WHERE table_schema='${DATA_SCHEMA}' ORDER BY 1" \
  | tail -n +2 || true)"
if [ -n "${TABLES// }" ]; then
  echo "${TABLES}" | while IFS=$'\t' read -r T; do
    [ -z "${T}" ] && continue
    echo "  - DROP TABLE ${CATALOG}.${DATA_SCHEMA}.\"${T}\""
    run_trino "${CATALOG}" "${DATA_SCHEMA}" "DROP TABLE IF EXISTS \"${T}\""
  done
else
  echo "  (none found)"
fi

# Wipe catalog tables
echo "[reset] ${CATALOG}.${META_SCHEMA}.catalog_{datasets,columns}"
run_trino "${CATALOG}" "information_schema" "
CREATE TABLE IF NOT EXISTS ${CATALOG}.${META_SCHEMA}.catalog_datasets (
  dataset_id        VARCHAR,
  name              VARCHAR,
  source            VARCHAR,
  created_at        TIMESTAMP,
  row_count         BIGINT,
  size_bytes        BIGINT,
  last_profiled_at  TIMESTAMP
)"
run_trino "${CATALOG}" "information_schema" "
CREATE TABLE IF NOT EXISTS ${CATALOG}.${META_SCHEMA}.catalog_columns (
  dataset_id     VARCHAR,
  column_name    VARCHAR,
  data_type      VARCHAR,
  pii_flag       BOOLEAN,
  null_ratio     DOUBLE,
  distinct_ratio DOUBLE
)"
run_trino "${CATALOG}" "${META_SCHEMA}" "DELETE FROM catalog_columns"
run_trino "${CATALOG}" "${META_SCHEMA}" "DELETE FROM catalog_datasets"

# Candidate names (base|source)
CANDIDATES=(
  "Salesforce_Accounts|Salesforce"
  "Salesforce_Opportunities|Salesforce"
  "Zendesk_Tickets|Zendesk"
  "Zendesk_Users|Zendesk"
  "ServiceNow_Incidents|ServiceNow"
  "ServiceNow_Changes|ServiceNow"
  "OracleEBS_GLJournals|Oracle EBS"
  "OracleEBS_ARReceipts|Oracle EBS"
  "SAPECC_SalesOrders|SAP ECC"
  "SAPECC_MaterialMaster|SAP ECC"
  "Workday_Compensation|Workday HCM"
  "Workday_Positions|Workday HCM"
  "Shopify_Orders|Shopify"
  "Shopify_Customers|Shopify"
  "NetSuite_Transactions|NetSuite"
  "NetSuite_Vendors|NetSuite"
  "GitHub_Issues|GitHub"
  "GitHub_Releases|GitHub"
  "GoogleAnalytics_Events|Google Analytics"
  "GoogleAnalytics_Sessions|Google Analytics"
  "Postgres_Customers|PostgreSQL"
  "Postgres_Invoices|PostgreSQL"
  "SQLServer_Clickstream|SQL Server"
  "Anaplan_Forecasts|Anaplan"
  "Anaplan_ModelExports|Anaplan"
  "Jira_Issues|Jira Cloud"
  "Jira_Users|Jira Cloud"
)
C_LEN="${#CANDIDATES[@]}"

to_table() {
  echo "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g'
}

echo "[create] ${ENTITIES} entities with ${ROWS} rows each"
i=0
while [ "${i}" -lt "${ENTITIES}" ]; do
  idx=$(( i % C_LEN ))
  rep=$(( i / C_LEN + 1 ))
  BASE="${CANDIDATES[$idx]}"
  LOGICAL="${BASE%%|*}"
  SOURCE="${BASE##*|}"
  # add suffix on repeats to keep names unique and businesslike
  if [ "${rep}" -gt 1 ]; then
    LOGICAL="${LOGICAL}_${rep}"
  fi

  TABLE="$(to_table "${LOGICAL}")"
  DATASET_ID="ds_${TABLE}"
  LOCATION="${WAREHOUSE}/datasets/${TABLE}"

  echo "  → ${LOGICAL} [${TABLE}] (source=${SOURCE})"

  # Create table with explicit location
  run_trino "${CATALOG}" "information_schema" "
CREATE TABLE ${CATALOG}.${DATA_SCHEMA}.${TABLE} (
  id              BIGINT,
  username        VARCHAR,
  full_name       VARCHAR,
  email           VARCHAR,
  amount          DECIMAL(12,2),
  credit_score    INTEGER,
  risk_factor     DOUBLE,
  is_active       BOOLEAN,
  country         VARCHAR,
  payment_terms   VARCHAR,
  posted_date     DATE,
  created_at      TIMESTAMP,
  last_login      TIMESTAMP,
  updated_at      TIMESTAMP,
  notes           VARCHAR,
  timezone        VARCHAR,
  currency        VARCHAR,
  tags            ARRAY(VARCHAR),
  prefs           MAP(VARCHAR, VARCHAR),
  login_count     INTEGER,
  discount_pct    DOUBLE,
  lifetime_value  DOUBLE,
  segment         VARCHAR,
  status          VARCHAR,
  churn_prob      DOUBLE
) WITH (
  format = 'PARQUET',
  location = '${LOCATION}'
)"
  # Insert synthetic rows
  run_trino "${CATALOG}" "${DATA_SCHEMA}" "
INSERT INTO ${TABLE}
SELECT
  CAST(seq AS BIGINT)                                  AS id,
  CONCAT('user_', CAST(seq AS VARCHAR))                AS username,
  CONCAT('User ', CAST(seq AS VARCHAR))                AS full_name,
  CONCAT('user', CAST(seq AS VARCHAR), '@example.com') AS email,
  CAST(ROUND( (rand()*10000) + 10, 2) AS DECIMAL(12,2)) AS amount,
  300 + CAST(rand()*550 AS INTEGER)                     AS credit_score,
  ROUND(rand(), 6)                                      AS risk_factor,
  (rand() > 0.2)                                        AS is_active,
  ELEMENT_AT(ARRAY['US','CA','GB','DE','FR','JP'], 1 + CAST(rand()*6 AS INTEGER)) AS country,
  ELEMENT_AT(ARRAY['NET30','NET45','DUE_ON_RECEIPT'], 1 + CAST(rand()*3 AS INTEGER)) AS payment_terms,
  (DATE '2024-01-01' + INTERVAL '1' DAY * CAST(rand()*600 AS INTEGER)) AS posted_date,
  current_timestamp                                                     AS created_at,
  current_timestamp - INTERVAL '1' DAY * CAST(rand()*30 AS INTEGER)     AS last_login,
  current_timestamp                                                     AS updated_at,
  IF(rand() < 0.1, 'manual review', NULL)                               AS notes,
  ELEMENT_AT(ARRAY['UTC','America/Denver','America/New_York','Europe/Berlin'], 1 + CAST(rand()*4 AS INTEGER)) AS timezone,
  ELEMENT_AT(ARRAY['USD','EUR','JPY','GBP'], 1 + CAST(rand()*4 AS INTEGER)) AS currency,
  ARRAY['tier-', ELEMENT_AT(ARRAY['gold','silver','bronze'], 1 + CAST(rand()*3 AS INTEGER))] AS tags,
  MAP(ARRAY['lang','theme'], ARRAY['en', ELEMENT_AT(ARRAY['light','dark'], 1 + CAST(rand()*2 AS INTEGER))]) AS prefs,
  CAST(rand()*200 AS INTEGER)                            AS login_count,
  CAST(ROUND(rand()*100, 2) AS DOUBLE)                   AS discount_pct,
  CAST(ROUND(rand()*100000, 3) AS DOUBLE)                AS lifetime_value,
  ELEMENT_AT(ARRAY['A','B','C'], 1 + CAST(rand()*3 AS INTEGER)) AS segment,
  ELEMENT_AT(ARRAY['trial','active','churned'], 1 + CAST(rand()*3 AS INTEGER)) AS status,
  CAST(ROUND(rand(), 6) AS DOUBLE)                       AS churn_prob
FROM UNNEST(sequence(1, ${ROWS})) AS t(seq)
"

  # Upsert catalog_datasets
  run_trino "${CATALOG}" "${META_SCHEMA}" "
DELETE FROM catalog_datasets WHERE dataset_id='${DATASET_ID}' OR name='${LOGICAL}'"
  run_trino "${CATALOG}" "${META_SCHEMA}" "
INSERT INTO catalog_datasets
SELECT
  '${DATASET_ID}',
  '${LOGICAL}',
  '${SOURCE}',
  current_timestamp,
  (SELECT COUNT(*) FROM ${CATALOG}.${DATA_SCHEMA}.\"${TABLE}\"),
  4096,
  current_timestamp
"
  # catalog_columns
  run_trino "${CATALOG}" "${META_SCHEMA}" "
DELETE FROM catalog_columns WHERE dataset_id='${DATASET_ID}'"
  run_trino "${CATALOG}" "information_schema" "
INSERT INTO ${CATALOG}.${META_SCHEMA}.catalog_columns
(dataset_id, column_name, data_type, pii_flag, null_ratio, distinct_ratio)
SELECT
  '${DATASET_ID}',
  c.column_name,
  c.data_type,
  CASE WHEN regexp_like(lower(c.column_name), '(email|phone|ssn|dob|address)') THEN TRUE ELSE FALSE END,
  0.0,
  0.0
FROM ${CATALOG}.information_schema.columns c
WHERE c.table_schema='${DATA_SCHEMA}' AND c.table_name='${TABLE}'
"
  i=$(( i + 1 ))
done

echo
echo "✓ reseed complete — ${ENTITIES} entities created. Refresh the UI."
