#!/usr/bin/env bash
set -euo pipefail

# ---- Config ----
SERVER="${SERVER:-http://host.docker.internal:8080}"  # Trino URL
USER="${USER:-web}"
TRINO_CLI_VER="${TRINO_CLI_VER:-435}"

# Run a SQL block by writing it to a temp file & using Trino CLI --file (avoids all quoting hell)
run_sql_file() {
  local sql="$1"
  local tmp=".trino_$$_$RANDOM.sql"
  printf "%s\n" "$sql" > "$tmp"
  # mount PWD so the file is visible inside the container
  local out
  out="$(docker run --rm -i -v "$PWD":/work --workdir /work eclipse-temurin:17-jre sh -lc "
    curl -sSL -o trino https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_CLI_VER}/trino-cli-${TRINO_CLI_VER}-executable.jar &&
    chmod +x trino &&
    ./trino --server '${SERVER}' --user '${USER}' \
      --catalog iceberg --schema datasets \
      --output-format ALIGNED \
      --file '$tmp'
  ")"
  rm -f "$tmp"
  printf "%s\n" "$out"
}

create_card() {
  local id="$1" name="$2" source="$3" lineage_src="$4" ddl="$5"

  echo "== [$id] 1) CREATE TABLE"
  run_sql_file "$ddl" >/dev/null

  echo "== [$id] 2) INSERT catalog_datasets (idempotent)"
  run_sql_file "
INSERT INTO iceberg.catalog.catalog_datasets (dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
SELECT '${id}','${name}','${source}', current_timestamp, NULL, NULL, current_timestamp
WHERE NOT EXISTS (SELECT 1 FROM iceberg.catalog.catalog_datasets WHERE dataset_id='${id}');
" | sed -n 's/^/   /p'

  echo "== [$id] 3) INSERT catalog_columns (auto from information_schema, with retry)"
  run_sql_file "DELETE FROM iceberg.catalog.catalog_columns WHERE dataset_id='${id}';" >/dev/null || true

  local insert_cols="
INSERT INTO iceberg.catalog.catalog_columns (dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT
  '${id}' AS dataset_id,
  c.column_name,
  c.data_type,
  FALSE AS pii_flag,
  NULL  AS null_ratio,
  NULL  AS distinct_ratio,
  CASE
    WHEN c.column_name LIKE '%_id' OR c.column_name IN ('id','number') THEN TRUE
    WHEN c.column_name IN ('created','created_at','updated','updated_at','closed_at','resolved','due_at','tran_timestamp') THEN TRUE
    ELSE FALSE
  END AS indexed
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg'
  AND c.table_schema='datasets'
  AND c.table_name='${id}'
ORDER BY c.ordinal_position;
"

  local tries=12 sleep_s=1 ok=""
  for t in $(seq 1 $tries); do
    out="$(run_sql_file "$insert_cols" || true)"
    echo "$out" | sed -n 's/^/   /p'
    if echo "$out" | grep -q 'INSERT: [1-9]'; then ok="yes"; break; fi
    sleep "$sleep_s"
  done
  if [[ -z "$ok" ]]; then
    echo "!! $id: information_schema never showed columns after ${tries}s; continuing" >&2
  fi

  echo "== [$id] 4) INSERT lineage edge (idempotent)"
  run_sql_file "
INSERT INTO iceberg.catalog.catalog_lineage_edges (src_dataset_id,dst_dataset_id,transform_type,updated_at)
SELECT '${lineage_src}','${id}','Replication',current_timestamp
WHERE NOT EXISTS (
  SELECT 1 FROM iceberg.catalog.catalog_lineage_edges
  WHERE src_dataset_id='${lineage_src}' AND dst_dataset_id='${id}' AND transform_type='Replication'
);
" | sed -n 's/^/   /p'

  echo "== [$id] Done."
}

# ---- DDL blocks (plain strings) ----
DDL_SALESFORCE_OPPS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.salesforce_opportunities_1 (
  id VARCHAR,
  accountid VARCHAR,
  ownerid VARCHAR,
  name VARCHAR,
  stagename VARCHAR,
  amount DOUBLE,
  closedate DATE,
  probability DOUBLE,
  type VARCHAR,
  leadsource VARCHAR,
  nextstep VARCHAR,
  forecastcategory VARCHAR,
  isclosed BOOLEAN,
  iswon BOOLEAN,
  createddate TIMESTAMP(6),
  lastmodifieddate TIMESTAMP(6),
  description VARCHAR
) WITH (format='PARQUET');
SQL
)"

DDL_SHOPIFY_ORDERS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.shopify_orders_1 (
  id BIGINT,
  order_number VARCHAR,
  customer_id BIGINT,
  email VARCHAR,
  total_price DECIMAL(12,2),
  currency VARCHAR,
  financial_status VARCHAR,
  fulfillment_status VARCHAR,
  tags VARCHAR,
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_ZENDESK_TICKETS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.zendesk_tickets_1 (
  id BIGINT,
  subject VARCHAR,
  description VARCHAR,
  status VARCHAR,
  priority VARCHAR,
  type VARCHAR,
  requester_id BIGINT,
  assignee_id BIGINT,
  group_id BIGINT,
  organization_id BIGINT,
  tags VARCHAR,
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6),
  due_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_JIRA_ISSUES_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.jira_issues_1 (
  id BIGINT,
  issue_key VARCHAR,
  summary VARCHAR,
  description VARCHAR,
  issue_type VARCHAR,
  status VARCHAR,
  priority VARCHAR,
  reporter_id BIGINT,
  assignee_id BIGINT,
  created TIMESTAMP(6),
  updated TIMESTAMP(6),
  resolved TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_GITHUB_ISSUES_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.github_issues_1 (
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
SQL
)"

DDL_SERVICENOW_INCIDENTS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.servicenow_incidents_1 (
  sys_id VARCHAR,
  number VARCHAR,
  short_description VARCHAR,
  description VARCHAR,
  state VARCHAR,
  priority VARCHAR,
  assignment_group VARCHAR,
  assigned_to VARCHAR,
  category VARCHAR,
  subcategory VARCHAR,
  opened_at TIMESTAMP(6),
  updated_at TIMESTAMP(6),
  closed_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_NETSUITE_TXNS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.netsuite_transactions_1 (
  id BIGINT,
  tran_type VARCHAR,
  entity_id BIGINT,
  memo VARCHAR,
  amount DECIMAL(14,2),
  currency VARCHAR,
  status VARCHAR,
  tran_timestamp TIMESTAMP(6),
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_POSTGRES_CUSTOMERS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.postgres_customers_1 (
  id BIGINT,
  email VARCHAR,
  full_name VARCHAR,
  country VARCHAR,
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_POSTGRES_INVOICES_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.postgres_invoices_1 (
  id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(12,2),
  currency VARCHAR,
  status VARCHAR,
  issued_at TIMESTAMP(6),
  paid_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_GA_EVENTS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.googleanalytics_events_1 (
  event_id BIGINT,
  user_id VARCHAR,
  session_id VARCHAR,
  event_name VARCHAR,
  event_params VARCHAR,
  event_timestamp TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_WORKDAY_POSITIONS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.workday_positions_1 (
  position_id VARCHAR,
  worker_id VARCHAR,
  title VARCHAR,
  org VARCHAR,
  location VARCHAR,
  effective_date DATE,
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_ANAPLAN_MODELEXPORTS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.anaplan_modelexports_1 (
  export_id VARCHAR,
  model_id VARCHAR,
  name VARCHAR,
  status VARCHAR,
  started_at TIMESTAMP(6),
  finished_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_ORACLE_EBS_GLJOURNALS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.oracleebs_gljournals_1 (
  journal_id BIGINT,
  ledger VARCHAR,
  period VARCHAR,
  category VARCHAR,
  status VARCHAR,
  entered_dr DECIMAL(18,2),
  entered_cr DECIMAL(18,2),
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_SAP_ECC_MATERIALMASTER_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.sapecc_materialmaster_1 (
  material_id VARCHAR,
  material_type VARCHAR,
  description VARCHAR,
  plant VARCHAR,
  base_uom VARCHAR,
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_SQLSERVER_CLICKSTREAM_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.sqlserver_clickstream_1 (
  event_id BIGINT,
  user_id VARCHAR,
  url VARCHAR,
  referrer VARCHAR,
  user_agent VARCHAR,
  event_time TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_GITHUB_RELEASES_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.github_releases_1 (
  id BIGINT,
  repo VARCHAR,
  tag_name VARCHAR,
  name VARCHAR,
  draft BOOLEAN,
  prerelease BOOLEAN,
  published_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_ZENDESK_USERS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.zendesk_users_1 (
  id BIGINT,
  name VARCHAR,
  email VARCHAR,
  role VARCHAR,
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_SHOPIFY_CUSTOMERS_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.shopify_customers_1 (
  id BIGINT,
  email VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  state VARCHAR,
  tags VARCHAR,
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_SERVICENOW_CHANGES_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.servicenow_changes_1 (
  sys_id VARCHAR,
  number VARCHAR,
  short_description VARCHAR,
  description VARCHAR,
  state VARCHAR,
  priority VARCHAR,
  opened_at TIMESTAMP(6),
  updated_at TIMESTAMP(6),
  closed_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

DDL_WORKDAY_COMPENSATION_1="$(cat <<'SQL'
CREATE TABLE IF NOT EXISTS iceberg.datasets.workday_compensation_1 (
  comp_event_id VARCHAR,
  worker_id VARCHAR,
  comp_plan VARCHAR,
  amount DECIMAL(14,2),
  currency VARCHAR,
  effective_date DATE,
  created_at TIMESTAMP(6),
  updated_at TIMESTAMP(6)
) WITH (format='PARQUET');
SQL
)"

# ---- Build 20 cards ----
create_card "salesforce_opportunities_1" "Salesforce Opportunities" "Salesforce"     "salesforce_opportunities_raw"  "$DDL_SALESFORCE_OPPS_1"
create_card "shopify_orders_1"           "Shopify Orders"           "Shopify"       "shopify_orders_raw"            "$DDL_SHOPIFY_ORDERS_1"
create_card "zendesk_tickets_1"          "Zendesk Tickets"          "Zendesk"       "zendesk_tickets_raw"           "$DDL_ZENDESK_TICKETS_1"
create_card "jira_issues_1"              "Jira Issues"              "Jira Cloud"    "jira_issues_raw"               "$DDL_JIRA_ISSUES_1"
create_card "github_issues_1"            "GitHub Issues"            "GitHub"        "github_issues_raw"             "$DDL_GITHUB_ISSUES_1"
create_card "servicenow_incidents_1"     "ServiceNow Incidents"     "ServiceNow"    "servicenow_incidents_raw"      "$DDL_SERVICENOW_INCIDENTS_1"
create_card "netsuite_transactions_1"    "NetSuite Transactions"    "NetSuite"      "netsuite_transactions_raw"     "$DDL_NETSUITE_TXNS_1"
create_card "postgres_customers_1"       "PostgreSQL Customers"     "PostgreSQL"    "postgres_customers_raw"        "$DDL_POSTGRES_CUSTOMERS_1"
create_card "postgres_invoices_1"        "PostgreSQL Invoices"      "PostgreSQL"    "postgres_invoices_raw"         "$DDL_POSTGRES_INVOICES_1"
create_card "googleanalytics_events_1"   "Google Analytics Events"  "Google Analytics" "googleanalytics_events_raw"   "$DDL_GA_EVENTS_1"
create_card "workday_positions_1"        "Workday Positions"        "Workday HCM"   "workday_positions_raw"         "$DDL_WORKDAY_POSITIONS_1"
create_card "anaplan_modelexports_1"     "Anaplan Model Exports"    "Anaplan"       "anaplan_modelexports_raw"      "$DDL_ANAPLAN_MODELEXPORTS_1"
create_card "oracleebs_gljournals_1"     "Oracle EBS GL Journals"   "Oracle EBS"    "oracleebs_gljournals_raw"      "$DDL_ORACLE_EBS_GLJOURNALS_1"
create_card "sapecc_materialmaster_1"    "SAP ECC Material Master"  "SAP ECC"       "sapecc_materialmaster_raw"     "$DDL_SAP_ECC_MATERIALMASTER_1"
create_card "sqlserver_clickstream_1"     "SQL Server Clickstream"   "SQL Server"    "sqlserver_clickstream_raw"     "$DDL_SQLSERVER_CLICKSTREAM_1"
create_card "github_releases_1"          "GitHub Releases"          "GitHub"        "github_releases_raw"           "$DDL_GITHUB_RELEASES_1"
create_card "zendesk_users_1"            "Zendesk Users"            "Zendesk"       "zendesk_users_raw"             "$DDL_ZENDESK_USERS_1"
create_card "shopify_customers_1"        "Shopify Customers"        "Shopify"       "shopify_customers_raw"         "$DDL_SHOPIFY_CUSTOMERS_1"
create_card "servicenow_changes_1"       "ServiceNow Changes"       "ServiceNow"    "servicenow_changes_raw"        "$DDL_SERVICENOW_CHANGES_1"
create_card "workday_compensation_1"     "Workday Compensation"     "Workday HCM"   "workday_compensation_raw"      "$DDL_WORKDAY_COMPENSATION_1"

