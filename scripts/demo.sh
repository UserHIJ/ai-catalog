#!/usr/bin/env bash
set -euo pipefail

WAREHOUSE="s3://iceberg-warehouse"
TRINO="docker exec -i trino-fg trino --catalog iceberg"

SOURCES=(
  "SAP ECC" "Oracle EBS" "SQL Server" "PostgreSQL"
  "Anaplan" "Shopify" "Salesforce" "Workday HCM"
  "NetSuite" "Zendesk" "Jira Cloud" "GitHub"
  "ServiceNow" "Google Analytics"
)

for i in $(seq -w 1 40); do
  DATASET_ID="ds_entity_${i}"

  # rotate source
  idx=$(( (10#$i - 1) % ${#SOURCES[@]} ))
  SRC="${SOURCES[$idx]}"

  # pick a business-ish base name per source
  case "$SRC" in
    "SAP ECC")         OPTS=(sap_sales_orders sap_gl_transactions sap_material_master sap_customer_master) ;;
    "Oracle EBS")      OPTS=(ebs_ap_invoices ebs_ar_receipts ebs_po_lines ebs_gl_journals) ;;
    "SQL Server")      OPTS=(sqlsrv_web_orders sqlsrv_page_views sqlsrv_clickstream sqlsrv_erp_orders) ;;
    "PostgreSQL")      OPTS=(pg_orders pg_invoices pg_user_events pg_customers) ;;
    "Anaplan")         OPTS=(anaplan_model_exports anaplan_scenarios anaplan_forecasts anaplan_cost_centers) ;;
    "Shopify")         OPTS=(shopify_orders shopify_customers shopify_products shopify_transactions) ;;
    "Salesforce")      OPTS=(sf_accounts sf_opportunities sf_contacts sf_cases) ;;
    "Workday HCM")     OPTS=(workday_workers workday_compensation workday_time_off workday_positions) ;;
    "NetSuite")        OPTS=(netsuite_transactions netsuite_items netsuite_vendors netsuite_customers) ;;
    "Zendesk")         OPTS=(zendesk_tickets zendesk_users zendesk_comments zendesk_satisfaction) ;;
    "Jira Cloud")      OPTS=(jira_issues jira_projects jira_users jira_boards) ;;
    "GitHub")          OPTS=(github_commits github_issues github_prs github_releases) ;;
    "ServiceNow")      OPTS=(servicenow_incidents servicenow_requests servicenow_changes servicenow_problems) ;;
    "Google Analytics") OPTS=(ga_sessions ga_events ga_pages ga_traffic) ;;
  esac

  opt_idx=$(( (10#$i - 1) % ${#OPTS[@]} ))
  BASE="${OPTS[$opt_idx]}"
  NEW_NAME="${BASE}_${i}"

  echo "Updating ${DATASET_ID} → ${NEW_NAME} (${SRC})"

  $TRINO --schema catalog --execute "
    UPDATE catalog_datasets
    SET name='${NEW_NAME}', source='${SRC}'
    WHERE dataset_id='${DATASET_ID}'
  "
done

