#!/usr/bin/env bash
set -euo pipefail

# helper: do NOT change; matches your working pattern
trinoi() {
  docker exec -i trino-fg trino \
    --server http://localhost:8080 \
    --catalog iceberg \
    --schema datasets \
    --user web \
    "$@"
}

############################################
# 1) Oracle EBS AR Receipts
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.oracleebs_ar_receipts_1 (
  receipt_id           BIGINT,
  cash_receipt_id      BIGINT,
  receipt_number       VARCHAR,
  customer_trx_id      BIGINT,
  customer_id          BIGINT,
  payer_site_use_id    BIGINT,
  org_id               BIGINT,
  receipt_date         DATE,
  gl_date              DATE,
  currency_code        VARCHAR,
  amount               DECIMAL(15,2),
  applied_amount       DECIMAL(15,2),
  unapplied_amount     DECIMAL(15,2),
  remittance_bank_acct VARCHAR,
  status               VARCHAR,
  payment_method       VARCHAR,
  exchange_rate        DECIMAL(18,8),
  exchange_rate_type   VARCHAR,
  exchange_rate_date   DATE,
  creation_date        TIMESTAMP(6),
  created_by           BIGINT,
  last_update_date     TIMESTAMP(6),
  last_updated_by      BIGINT,
  comments             VARCHAR,
  attribute1           VARCHAR,
  attribute2           VARCHAR,
  attribute3           VARCHAR,
  attribute4           VARCHAR,
  attribute5           VARCHAR
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('oracleebs_ar_receipts_1','Oracle EBS AR Receipts','Oracle EBS',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'oracleebs_ar_receipts_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name IN ('receipt_id','cash_receipt_id','customer_id','customer_trx_id') THEN TRUE
         WHEN c.column_name IN ('receipt_date','gl_date','creation_date','last_update_date','exchange_rate_date') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='oracleebs_ar_receipts_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('oracleebs_ar_receipts_raw','oracleebs_ar_receipts_1','Replication',current_timestamp)"
sleep 5

############################################
# 2) Oracle EBS PO Headers
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.oracleebs_po_headers_1 (
  po_header_id        BIGINT,
  segment1            VARCHAR,   -- PO number
  type_lookup_code    VARCHAR,
  vendor_id           BIGINT,
  vendor_site_id      BIGINT,
  agent_id            BIGINT,
  org_id              BIGINT,
  ship_to_location_id BIGINT,
  bill_to_location_id BIGINT,
  currency_code       VARCHAR,
  rate                DECIMAL(18,8),
  rate_date           DATE,
  terms_id            BIGINT,
  approval_status     VARCHAR,
  authorization_status VARCHAR,
  closed_code         VARCHAR,
  cancelled_date      DATE,
  note_to_vendor      VARCHAR,
  creation_date       TIMESTAMP(6),
  last_update_date    TIMESTAMP(6)
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('oracleebs_po_headers_1','Oracle EBS PO Headers','Oracle EBS',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'oracleebs_po_headers_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name IN ('po_header_id','vendor_id','vendor_site_id','agent_id') THEN TRUE
         WHEN c.column_name IN ('creation_date','last_update_date','rate_date','cancelled_date') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='oracleebs_po_headers_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('oracleebs_po_headers_raw','oracleebs_po_headers_1','Replication',current_timestamp)"
sleep 5

############################################
# 3) Oracle EBS PO Lines (big-ish)
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.oracleebs_po_lines_1 (
  po_line_id         BIGINT,
  po_header_id       BIGINT,
  line_num           BIGINT,
  item_id            BIGINT,
  item_description   VARCHAR,
  category_id        BIGINT,
  quantity_ordered   DECIMAL(18,4),
  quantity_received  DECIMAL(18,4),
  quantity_billed    DECIMAL(18,4),
  unit_price         DECIMAL(18,6),
  uom_code           VARCHAR,
  need_by_date       DATE,
  promised_date      DATE,
  ship_to_location_id BIGINT,
  deliver_to_location_id BIGINT,
  charge_account_id  BIGINT,
  destination_type_code VARCHAR,
  closed_code        VARCHAR,
  cancelled_flag     BOOLEAN,
  attribute1         VARCHAR,
  attribute2         VARCHAR,
  attribute3         VARCHAR,
  attribute4         VARCHAR,
  attribute5         VARCHAR,
  creation_date      TIMESTAMP(6),
  last_update_date   TIMESTAMP(6)
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('oracleebs_po_lines_1','Oracle EBS PO Lines','Oracle EBS',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'oracleebs_po_lines_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name IN ('po_line_id','po_header_id','item_id','category_id') THEN TRUE
         WHEN c.column_name IN ('need_by_date','promised_date','creation_date','last_update_date') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='oracleebs_po_lines_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('oracleebs_po_lines_raw','oracleebs_po_lines_1','Replication',current_timestamp)"
sleep 5

############################################
# 4) Oracle EBS OM Orders (headers)
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.oracleebs_om_orders_1 (
  header_id          BIGINT,
  order_number       BIGINT,
  order_type_id      BIGINT,
  org_id             BIGINT,
  sold_to_org_id     BIGINT,
  ship_to_org_id     BIGINT,
  invoice_to_org_id  BIGINT,
  price_list_id      BIGINT,
  salesrep_id        BIGINT,
  currency_code      VARCHAR,
  booked_flag        BOOLEAN,
  status_code        VARCHAR,
  ordered_date       TIMESTAMP(6),
  request_date       TIMESTAMP(6),
  customer_po_number VARCHAR,
  freight_terms_code VARCHAR,
  shipping_method_code VARCHAR,
  tax_exempt_flag    BOOLEAN,
  tax_exempt_number  VARCHAR,
  last_update_date   TIMESTAMP(6),
  creation_date      TIMESTAMP(6)
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('oracleebs_om_orders_1','Oracle EBS OM Orders','Oracle EBS',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'oracleebs_om_orders_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name IN ('header_id','order_number','order_type_id','sold_to_org_id','ship_to_org_id') THEN TRUE
         WHEN c.column_name IN ('ordered_date','request_date','last_update_date','creation_date') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='oracleebs_om_orders_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('oracleebs_om_orders_raw','oracleebs_om_orders_1','Replication',current_timestamp)"
sleep 5

############################################
# 5) Oracle EBS Inventory Items
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.oracleebs_inventory_items_1 (
  inventory_item_id  BIGINT,
  organization_id    BIGINT,
  segment1           VARCHAR,  -- item number
  description        VARCHAR,
  item_type          VARCHAR,
  item_status        VARCHAR,
  primary_uom_code   VARCHAR,
  inventory_item_flag BOOLEAN,
  stock_enabled_flag  BOOLEAN,
  transactable_flag   BOOLEAN,
  purchasable_flag    BOOLEAN,
  orderable_on_web_flag BOOLEAN,
  customer_order_flag BOOLEAN,
  shippable_item_flag BOOLEAN,
  returnable_flag     BOOLEAN,
  planning_make_buy_code VARCHAR,
  make_or_buy         VARCHAR,
  lead_time_cum_mfg   BIGINT,
  lead_time_cum_total BIGINT,
  weight_uom_code     VARCHAR,
  unit_weight         DECIMAL(14,4),
  volume_uom_code     VARCHAR,
  unit_volume         DECIMAL(14,4),
  creation_date       TIMESTAMP(6),
  last_update_date    TIMESTAMP(6)
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('oracleebs_inventory_items_1','Oracle EBS Inventory Items','Oracle EBS',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'oracleebs_inventory_items_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name IN ('inventory_item_id','organization_id') THEN TRUE
         WHEN c.column_name IN ('creation_date','last_update_date') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='oracleebs_inventory_items_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('oracleebs_inventory_items_raw','oracleebs_inventory_items_1','Replication',current_timestamp)"
sleep 5

############################################
# 6) Oracle Fusion HCM Workers (BIG: ~60 cols)
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.oracle_fusion_hcm_workers_1 (
  worker_id           VARCHAR,
  person_number       VARCHAR,
  user_guid           VARCHAR,
  first_name          VARCHAR,
  last_name           VARCHAR,
  middle_names        VARCHAR,
  preferred_name      VARCHAR,
  full_name           VARCHAR,
  gender_code         VARCHAR,
  birth_date          DATE,
  nationality         VARCHAR,
  marital_status      VARCHAR,
  national_id         VARCHAR,
  email_work          VARCHAR,
  email_personal      VARCHAR,
  phone_mobile        VARCHAR,
  phone_work          VARCHAR,
  legal_entity_id     VARCHAR,
  business_unit_id    VARCHAR,
  department_id       VARCHAR,
  job_id              VARCHAR,
  position_id         VARCHAR,
  grade_id            VARCHAR,
  manager_person_id   VARCHAR,
  location_id         VARCHAR,
  assignment_status   VARCHAR,
  employment_type     VARCHAR,
  time_type           VARCHAR,
  fte                 DECIMAL(5,2),
  salary_amount       DECIMAL(14,2),
  salary_currency     VARCHAR,
  pay_frequency       VARCHAR,
  hire_date           DATE,
  start_date          DATE,
  seniority_date      DATE,
  termination_date    DATE,
  rehire_date         DATE,
  work_address_line1  VARCHAR,
  work_address_line2  VARCHAR,
  work_city           VARCHAR,
  work_state          VARCHAR,
  work_postal_code    VARCHAR,
  work_country        VARCHAR,
  home_address_line1  VARCHAR,
  home_address_line2  VARCHAR,
  home_city           VARCHAR,
  home_state          VARCHAR,
  home_postal_code    VARCHAR,
  home_country        VARCHAR,
  cost_center         VARCHAR,
  project_code        VARCHAR,
  manager_level       VARCHAR,
  union_member_flag   BOOLEAN,
  expatriate_flag     BOOLEAN,
  visa_type           VARCHAR,
  visa_expiry_date    DATE,
  disability_flag     BOOLEAN,
  veteran_status      VARCHAR,
  effective_start     TIMESTAMP(6),
  effective_end       TIMESTAMP(6),
  last_update_date    TIMESTAMP(6)
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('oracle_fusion_hcm_workers_1','Oracle Fusion HCM Workers','Oracle Fusion',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'oracle_fusion_hcm_workers_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name LIKE '%_id' OR c.column_name IN ('worker_id','person_number','manager_person_id') THEN TRUE
         WHEN c.column_name IN ('hire_date','start_date','seniority_date','termination_date','rehire_date','effective_start','effective_end','last_update_date') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='oracle_fusion_hcm_workers_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('oracle_fusion_hcm_workers_raw','oracle_fusion_hcm_workers_1','Replication',current_timestamp)"
sleep 5

############################################
# 7) Slack Messages
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.slack_messages_1 (
  team_id         VARCHAR,
  channel_id      VARCHAR,
  channel_name    VARCHAR,
  message_id      VARCHAR,
  ts              TIMESTAMP(6),
  user_id         VARCHAR,
  user_real_name  VARCHAR,
  is_bot          BOOLEAN,
  bot_id          VARCHAR,
  thread_ts       TIMESTAMP(6),
  parent_user_id  VARCHAR,
  text            VARCHAR,
  permalink       VARCHAR,
  reactions       VARCHAR,
  files           VARCHAR,
  client_msg_id   VARCHAR,
  subtype         VARCHAR,
  edited_ts       TIMESTAMP(6),
  deleted_flag    BOOLEAN,
  pinned_flag     BOOLEAN,
  imported_flag   BOOLEAN
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('slack_messages_1','Slack Messages','Slack',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'slack_messages_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name IN ('channel_id','message_id','user_id') THEN TRUE
         WHEN c.column_name IN ('ts','thread_ts','edited_ts') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='slack_messages_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('slack_messages_raw','slack_messages_1','Replication',current_timestamp)"
sleep 5

############################################
# 8) DB2 z/OS Core Banking Accounts (BIG: ~55 cols)
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.db2_zos_corebank_accounts_1 (
  acct_number         VARCHAR,
  cust_id             VARCHAR,
  acct_type           VARCHAR,
  product_code        VARCHAR,
  branch_code         VARCHAR,
  status_code         VARCHAR,
  currency_code       VARCHAR,
  open_date           DATE,
  close_date          DATE,
  ledger_balance      DECIMAL(18,2),
  available_balance   DECIMAL(18,2),
  overdraft_limit     DECIMAL(18,2),
  interest_rate       DECIMAL(9,6),
  accrued_interest    DECIMAL(18,2),
  last_stmt_date      DATE,
  next_stmt_date      DATE,
  last_txn_ts         TIMESTAMP(6),
  risk_grade          VARCHAR,
  aml_flag            BOOLEAN,
  kyc_flag            BOOLEAN,
  dormant_flag        BOOLEAN,
  hold_flag           BOOLEAN,
  freeze_flag         BOOLEAN,
  charge_off_flag     BOOLEAN,
  write_off_amount    DECIMAL(18,2),
  days_past_due       INTEGER,
  collections_flag    BOOLEAN,
  tax_withhold_flag   BOOLEAN,
  tax_withhold_rate   DECIMAL(7,4),
  addr_line1          VARCHAR,
  addr_line2          VARCHAR,
  city                VARCHAR,
  state               VARCHAR,
  postal_code         VARCHAR,
  country             VARCHAR,
  phone_home          VARCHAR,
  phone_mobile        VARCHAR,
  email_primary       VARCHAR,
  rel_manager_id      VARCHAR,
  fee_plan_code       VARCHAR,
  fee_waive_flag      BOOLEAN,
  rewards_tier        VARCHAR,
  rewards_points      BIGINT,
  escheat_flag        BOOLEAN,
  escheat_date        DATE,
  last_review_date    DATE,
  next_review_date    DATE,
  create_ts           TIMESTAMP(6),
  update_ts           TIMESTAMP(6),
  attr1               VARCHAR,
  attr2               VARCHAR,
  attr3               VARCHAR,
  attr4               VARCHAR,
  attr5               VARCHAR
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('db2_zos_corebank_accounts_1','DB2 z/OS Core Banking Accounts','DB2 z/OS',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'db2_zos_corebank_accounts_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name IN ('acct_number','cust_id') THEN TRUE
         WHEN c.column_name IN ('open_date','close_date','last_stmt_date','next_stmt_date','last_txn_ts','create_ts','update_ts') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='db2_zos_corebank_accounts_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('db2_zos_corebank_accounts_raw','db2_zos_corebank_accounts_1','Replication',current_timestamp)"
sleep 5

############################################
# 9) DB2 UDB Orders
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.db2_udb_orders_1 (
  order_id        BIGINT,
  customer_id     BIGINT,
  order_status    VARCHAR,
  order_date      TIMESTAMP(6),
  ship_date       TIMESTAMP(6),
  currency        VARCHAR,
  subtotal        DECIMAL(14,2),
  tax_amount      DECIMAL(14,2),
  shipping_amount DECIMAL(14,2),
  total_amount    DECIMAL(14,2),
  ship_name       VARCHAR,
  ship_addr1      VARCHAR,
  ship_addr2      VARCHAR,
  ship_city       VARCHAR,
  ship_state      VARCHAR,
  ship_postal     VARCHAR,
  ship_country    VARCHAR,
  bill_name       VARCHAR,
  bill_addr1      VARCHAR,
  bill_addr2      VARCHAR,
  bill_city       VARCHAR,
  bill_state      VARCHAR,
  bill_postal     VARCHAR,
  bill_country    VARCHAR,
  created_at      TIMESTAMP(6),
  updated_at      TIMESTAMP(6)
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('db2_udb_orders_1','DB2 UDB Orders','DB2 UDB',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'db2_udb_orders_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name IN ('order_id','customer_id') THEN TRUE
         WHEN c.column_name IN ('order_date','ship_date','created_at','updated_at') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='db2_udb_orders_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('db2_udb_orders_raw','db2_udb_orders_1','Replication',current_timestamp)"
sleep 5

############################################
# 10) Oracle EBS AR Customers
############################################
trinoi --execute "CREATE TABLE IF NOT EXISTS iceberg.datasets.oracleebs_ar_customers_1 (
  customer_id         BIGINT,
  account_number      VARCHAR,
  party_id            BIGINT,
  party_name          VARCHAR,
  customer_type       VARCHAR,
  status              VARCHAR,
  primary_site_use_id BIGINT,
  bill_to_site_use_id BIGINT,
  ship_to_site_use_id BIGINT,
  tax_reference       VARCHAR,
  payment_terms_id    BIGINT,
  credit_limit        DECIMAL(15,2),
  credit_class        VARCHAR,
  credit_hold_flag    BOOLEAN,
  currency_code       VARCHAR,
  phone_number        VARCHAR,
  email_address       VARCHAR,
  address_line1       VARCHAR,
  address_line2       VARCHAR,
  city                VARCHAR,
  state               VARCHAR,
  postal_code         VARCHAR,
  country             VARCHAR,
  creation_date       TIMESTAMP(6),
  last_update_date    TIMESTAMP(6),
  attribute1          VARCHAR,
  attribute2          VARCHAR,
  attribute3          VARCHAR
) WITH (format='PARQUET')"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_datasets
(dataset_id,name,source,created_at,row_count,size_bytes,last_profiled_at)
VALUES ('oracleebs_ar_customers_1','Oracle EBS AR Customers','Oracle EBS',current_timestamp,NULL,NULL,current_timestamp)"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_columns
(dataset_id,column_name,data_type,pii_flag,null_ratio,distinct_ratio,indexed)
SELECT 'oracleebs_ar_customers_1', c.column_name, c.data_type, FALSE, NULL, NULL,
       CASE
         WHEN c.column_name IN ('customer_id','party_id','primary_site_use_id','bill_to_site_use_id','ship_to_site_use_id') THEN TRUE
         WHEN c.column_name IN ('creation_date','last_update_date') THEN TRUE
         ELSE FALSE
       END
FROM iceberg.information_schema.columns c
WHERE c.table_catalog='iceberg' AND c.table_schema='datasets' AND c.table_name='oracleebs_ar_customers_1'
ORDER BY c.ordinal_position"
sleep 5

trinoi --execute "INSERT INTO iceberg.catalog.catalog_lineage_edges
(src_dataset_id,dst_dataset_id,transform_type,updated_at)
VALUES ('oracleebs_ar_customers_raw','oracleebs_ar_customers_1','Replication',current_timestamp)"
sleep 5
