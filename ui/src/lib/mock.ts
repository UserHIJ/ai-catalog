// src/lib/mock.ts
import { Dataset, Column, LineageEdge } from "./types";

export const mockDatasets: Dataset[] = [
  { dataset_id: "ds_001", name: "sap_orders",           source: "SAP ECC",          row_count: 1_250_000, size_bytes: 95_000_000,  last_profiled_at: "2025-08-20T12:01:00Z" },
  { dataset_id: "ds_002", name: "sap_customers",        source: "SAP ECC",          row_count: 98_500,    size_bytes: 11_800_000,  last_profiled_at: "2025-08-20T12:02:00Z" },
  { dataset_id: "ds_003", name: "oracle_invoices",      source: "Oracle EBS",       row_count: 2_300_000, size_bytes: 180_000_000, last_profiled_at: "2025-08-20T12:03:00Z" },
  { dataset_id: "ds_004", name: "oracle_suppliers",     source: "Oracle EBS",       row_count: 120_000,   size_bytes: 13_000_000,  last_profiled_at: "2025-08-20T12:04:00Z" },
  { dataset_id: "ds_005", name: "sqlserver_payroll",    source: "SQL Server",       row_count: 240_000,   size_bytes: 17_000_000,  last_profiled_at: "2025-08-20T12:05:00Z" },
  { dataset_id: "ds_006", name: "sqlserver_expenses",   source: "SQL Server",       row_count: 88_000,    size_bytes: 6_000_000,   last_profiled_at: "2025-08-20T12:06:00Z" },
  { dataset_id: "ds_007", name: "postgres_orders",      source: "PostgreSQL",       row_count: 750_000,   size_bytes: 58_000_000,  last_profiled_at: "2025-08-20T12:07:00Z" },
  { dataset_id: "ds_008", name: "postgres_inventory",   source: "PostgreSQL",       row_count: 340_000,   size_bytes: 27_000_000,  last_profiled_at: "2025-08-20T12:08:00Z" },
  { dataset_id: "ds_009", name: "anaplan_forecasts",    source: "Anaplan",          row_count: 42_000,    size_bytes: 5_000_000,   last_profiled_at: "2025-08-20T12:09:00Z" },
  { dataset_id: "ds_010", name: "anaplan_budgets",      source: "Anaplan",          row_count: 15_000,    size_bytes: 2_200_000,   last_profiled_at: "2025-08-20T12:10:00Z" },
  { dataset_id: "ds_011", name: "shopify_orders",       source: "Shopify",          row_count: 1_800_000, size_bytes: 140_000_000, last_profiled_at: "2025-08-20T12:11:00Z" },
  { dataset_id: "ds_012", name: "shopify_products",     source: "Shopify",          row_count: 65_000,    size_bytes: 8_000_000,   last_profiled_at: "2025-08-20T12:12:00Z" },
  { dataset_id: "ds_013", name: "salesforce_opps",      source: "Salesforce",       row_count: 220_000,   size_bytes: 19_000_000,  last_profiled_at: "2025-08-20T12:13:00Z" },
  { dataset_id: "ds_014", name: "salesforce_accounts",  source: "Salesforce",       row_count: 80_000,    size_bytes: 9_000_000,   last_profiled_at: "2025-08-20T12:14:00Z" },
  { dataset_id: "ds_015", name: "workday_workers",      source: "Workday HCM",      row_count: 45_000,    size_bytes: 6_500_000,   last_profiled_at: "2025-08-20T12:15:00Z" },
  { dataset_id: "ds_016", name: "workday_salaries",     source: "Workday HCM",      row_count: 45_000,    size_bytes: 7_500_000,   last_profiled_at: "2025-08-20T12:16:00Z" },
  { dataset_id: "ds_017", name: "netsuite_journal",     source: "NetSuite",         row_count: 300_000,   size_bytes: 28_000_000,  last_profiled_at: "2025-08-20T12:17:00Z" },
  { dataset_id: "ds_018", name: "netsuite_customers",   source: "NetSuite",         row_count: 120_000,   size_bytes: 12_000_000,  last_profiled_at: "2025-08-20T12:18:00Z" },
  { dataset_id: "ds_019", name: "zendesk_tickets",      source: "Zendesk",          row_count: 950_000,   size_bytes: 55_000_000,  last_profiled_at: "2025-08-20T12:19:00Z" },
  { dataset_id: "ds_020", name: "zendesk_agents",       source: "Zendesk",          row_count: 5_400,     size_bytes: 600_000,     last_profiled_at: "2025-08-20T12:20:00Z" },
  { dataset_id: "ds_021", name: "jira_issues",          source: "Jira Cloud",       row_count: 2_200_000, size_bytes: 180_000_000, last_profiled_at: "2025-08-20T12:21:00Z" },
  { dataset_id: "ds_022", name: "jira_projects",        source: "Jira Cloud",       row_count: 1_200,     size_bytes: 150_000,     last_profiled_at: "2025-08-20T12:22:00Z" },
  { dataset_id: "ds_023", name: "github_commits",       source: "GitHub",           row_count: 3_500_000, size_bytes: 260_000_000, last_profiled_at: "2025-08-20T12:23:00Z" },
  { dataset_id: "ds_024", name: "servicenow_incidents", source: "ServiceNow",       row_count: 670_000,   size_bytes: 47_000_000,  last_profiled_at: "2025-08-20T12:24:00Z" },
  { dataset_id: "ds_025", name: "google_analytics",     source: "Google Analytics", row_count: 12_000_000,size_bytes: 720_000_000, last_profiled_at: "2025-08-20T12:25:00Z" },
];

// --- helpers to generate plausible schemas/lineage ---

function baseSchemaFor(name: string) {
  if (name.includes("orders")) {
    return [
      { column_name: "order_id", data_type: "INTEGER", pii_flag: false },
      { column_name: "customer_id", data_type: "INTEGER", pii_flag: false },
      { column_name: "order_date", data_type: "TIMESTAMP", pii_flag: false },
      { column_name: "total_amount", data_type: "DECIMAL(18,2)", pii_flag: false },
      { column_name: "notes", data_type: "VARCHAR", pii_flag: true },
    ];
  }
  if (name.includes("customers") || name.includes("accounts")) {
    return [
      { column_name: "customer_id", data_type: "INTEGER", pii_flag: false },
      { column_name: "email", data_type: "VARCHAR", pii_flag: true },
      { column_name: "full_name", data_type: "VARCHAR", pii_flag: true },
      { column_name: "created_at", data_type: "TIMESTAMP", pii_flag: false },
    ];
  }
  if (name.includes("invoices") || name.includes("payments") || name.includes("payroll") || name.includes("salaries")) {
    return [
      { column_name: "txn_id", data_type: "INTEGER", pii_flag: false },
      { column_name: "amount", data_type: "DECIMAL(18,2)", pii_flag: false },
      { column_name: "currency", data_type: "VARCHAR", pii_flag: false },
      { column_name: "posted_at", data_type: "TIMESTAMP", pii_flag: false },
    ];
  }
  if (name.includes("tickets") || name.includes("incidents") || name.includes("issues")) {
    return [
      { column_name: "ticket_id", data_type: "INTEGER", pii_flag: false },
      { column_name: "status", data_type: "VARCHAR", pii_flag: false },
      { column_name: "priority", data_type: "VARCHAR", pii_flag: false },
      { column_name: "opened_at", data_type: "TIMESTAMP", pii_flag: false },
      { column_name: "requester_email", data_type: "VARCHAR", pii_flag: true },
    ];
  }
  // default generic schema
  return [
    { column_name: "id", data_type: "INTEGER", pii_flag: false },
    { column_name: "name", data_type: "VARCHAR", pii_flag: true },
    { column_name: "updated_at", data_type: "TIMESTAMP", pii_flag: false },
  ];
}

function randRatio(seed: number) {
  // deterministic-ish small ratios between 0 and 0.2
  const x = Math.abs(Math.sin(seed) * 0.2);
  return Math.round(x * 1000) / 1000;
}

// --- exported columns/lineage ---

export const mockColumns: Column[] = mockDatasets.flatMap((d, idx) => {
  const schema = baseSchemaFor(d.name);
  return schema.map((c, j) => ({
    dataset_id: d.dataset_id,
    column_name: c.column_name,
    data_type: c.data_type,
    pii_flag: c.pii_flag,
    null_ratio: randRatio(idx * 7 + j),
    distinct_ratio: 1 - randRatio(idx * 11 + j) / 2,
  }));
});

export const mockLineage: LineageEdge[] = mockDatasets.flatMap((d, idx) => {
  const up: LineageEdge = {
    src_dataset_id: `${d.source.replace(/\s+/g, "_").toLowerCase()}_${d.name}_raw`,
    dst_dataset_id: d.dataset_id,
    transform_type: "ingest",
    updated_at: new Date(Date.parse(d.last_profiled_at || new Date().toISOString()) - 3600_000).toISOString(),
  };
  // add a downstream edge for some datasets
  const downMaybe =
    idx % 3 === 0
      ? [{
          src_dataset_id: d.dataset_id,
          dst_dataset_id: `${d.name}_mart`,
          transform_type: "model",
          updated_at: d.last_profiled_at || new Date().toISOString(),
        } as LineageEdge]
      : [];
  return [up, ...downMaybe];
});

