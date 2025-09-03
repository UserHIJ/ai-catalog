/**
 * Reseed businesslike demo entities in Iceberg + refresh catalog metadata.
 *
 * What it does
 * ------------
 * 1) Drops **all** tables in `iceberg.datasets` (be careful).
 * 2) Creates N new tables with realistic business names (e.g., Salesforce_Accounts).
 * 3) Seeds each with ~24 mixed columns (1 PII) and ROWS records (default 1000).
 * 4) Upserts `iceberg.catalog.catalog_datasets` and `iceberg.catalog.catalog_columns`.
 *
 * Config (env vars)
 * -----------------
 *   TRINO_URL=http://localhost:8080
 *   TRINO_USER=dev
 *   WAREHOUSE=s3://iceberg-warehouse        # absolute URI; not ${WAREHOUSE}
 *   ENTITIES=25                              # how many to create (default 20)
 *   ROWS=1000                                # rows per table (default 1000)
 *
 * Run
 * ---
 *   node ui/scripts/reseed-business.mjs
 */

const TRINO_URL  = process.env.TRINO_URL  || "http://localhost:8080";
const TRINO_USER = process.env.TRINO_USER || "dev";
const WAREHOUSE  = process.env.WAREHOUSE  || "s3://iceberg-warehouse";
const ENTITIES   = Number(process.env.ENTITIES || 20);
const ROWS       = Number(process.env.ROWS || 1000);

const CATALOG = "iceberg";
const DATA_SCHEMA = "datasets";
const META_SCHEMA = "catalog";

if (!/^s3a?:\/\//i.test(WAREHOUSE)) {
  console.error(`WAREHOUSE must be s3:// or s3a://. Got: ${WAREHOUSE}`);
  process.exit(1);
}

// -------------------- Trino helpers --------------------

/** Execute ONE statement; return the final payload (to read rows). */
async function trinoQuery(sql) {
  const r = await fetch(`${TRINO_URL}/v1/statement`, {
    method: "POST",
    headers: {
      "Content-Type": "text/plain",
      "X-Trino-User": TRINO_USER,
    },
    body: sql,
  });
  if (!r.ok) throw new Error(`Trino HTTP ${r.status} ${r.statusText}`);

  let data = await r.json();
  let last = data;
  while (data.nextUri) {
    const f = await fetch(data.nextUri);
    if (!f.ok) throw new Error(`Trino follow ${f.status} ${f.statusText}`);
    data = await f.json();
    if (data.error) throw new Error(data.error.message || JSON.stringify(data.error));
    last = data;
  }
  if (last.error) throw new Error(last.error.message);
  return last;
}

/** Execute ONE statement and ignore any result rows. */
async function trinoExec(sql) {
  await trinoQuery(sql);
}

// -------------------- Name planning --------------------

/** Businesslike logical dataset names + vendor/source label. */
const CANDIDATES = [
  { name: "Salesforce_Accounts",           source: "Salesforce" },
  { name: "Salesforce_Opportunities",      source: "Salesforce" },
  { name: "Zendesk_Tickets",               source: "Zendesk" },
  { name: "Zendesk_Users",                 source: "Zendesk" },
  { name: "ServiceNow_Incidents",          source: "ServiceNow" },
  { name: "ServiceNow_Changes",            source: "ServiceNow" },
  { name: "OracleEBS_GLJournals",          source: "Oracle EBS" },
  { name: "OracleEBS_ARReceipts",          source: "Oracle EBS" },
  { name: "SAPECC_SalesOrders",            source: "SAP ECC" },
  { name: "SAPECC_MaterialMaster",         source: "SAP ECC" },
  { name: "Workday_Compensation",          source: "Workday HCM" },
  { name: "Workday_Positions",             source: "Workday HCM" },
  { name: "Shopify_Orders",                source: "Shopify" },
  { name: "Shopify_Customers",             source: "Shopify" },
  { name: "NetSuite_Transactions",         source: "NetSuite" },
  { name: "NetSuite_Vendors",              source: "NetSuite" },
  { name: "GitHub_Issues",                 source: "GitHub" },
  { name: "GitHub_Releases",               source: "GitHub" },
  { name: "GoogleAnalytics_Events",        source: "Google Analytics" },
  { name: "GoogleAnalytics_Sessions",      source: "Google Analytics" },
  { name: "Postgres_Customers",            source: "PostgreSQL" },
  { name: "Postgres_Invoices",             source: "PostgreSQL" },
  { name: "SQLServer_Clickstream",         source: "SQL Server" },
  { name: "Anaplan_Forecasts",             source: "Anaplan" },
  { name: "Anaplan_ModelExports",          source: "Anaplan" },
  { name: "Jira_Issues",                   source: "Jira Cloud" },
  { name: "Jira_Users",                    source: "Jira Cloud" },
];

/** sanitize logical name → physical table name */
function toTableName(logical) {
  return logical.replace(/[^A-Za-z0-9]+/g, "_").toLowerCase();
}

/** create unique series of (logical name, source, table name, dataset_id) */
function planEntities(n) {
  const out = [];
  let i = 0, serial = 1;
  while (out.length < n) {
    const base = CANDIDATES[i % CANDIDATES.length];
    let logical = base.name;
    // Avoid duplicate logical names by suffixing serial if needed
    if (out.some((e) => e.logical === logical)) logical = `${logical}_${serial}`;
    const table = toTableName(logical);
    const datasetId = `ds_${table}`; // stable id your UI uses
    out.push({ logical, source: base.source, table, datasetId });
    i++; serial++;
  }
  return out;
}

// -------------------- DDL / DML --------------------

function createTable(table, location) {
  return `
CREATE TABLE ${CATALOG}.${DATA_SCHEMA}.${table} (
  id              BIGINT,
  username        VARCHAR,
  full_name       VARCHAR,
  email           VARCHAR,             -- PII (keep one)
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
  location = '${location}'
)
`;
}

function insertRows(table, rows) {
  return `
INSERT INTO ${CATALOG}.${DATA_SCHEMA}.${table}
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
FROM UNNEST(sequence(1, ${rows})) AS t(seq)
`;
}

function ensureMetaTables() {
  return [
    `
CREATE TABLE IF NOT EXISTS ${CATALOG}.${META_SCHEMA}.catalog_datasets (
  dataset_id        VARCHAR,
  name              VARCHAR,
  source            VARCHAR,
  created_at        TIMESTAMP,
  row_count         BIGINT,
  size_bytes        BIGINT,
  last_profiled_at  TIMESTAMP
)
`,
    `
CREATE TABLE IF NOT EXISTS ${CATALOG}.${META_SCHEMA}.catalog_columns (
  dataset_id     VARCHAR,
  column_name    VARCHAR,
  data_type      VARCHAR,
  pii_flag       BOOLEAN,
  null_ratio     DOUBLE,
  distinct_ratio DOUBLE
)
`,
  ];
}

function upsertDataset(datasetId, logicalName, source, table) {
  return [
    `
DELETE FROM ${CATALOG}.${META_SCHEMA}.catalog_datasets
WHERE dataset_id='${datasetId}' OR name='${logicalName}'
`,
    `
INSERT INTO ${CATALOG}.${META_SCHEMA}.catalog_datasets
SELECT
  '${datasetId}',
  '${logicalName}',
  '${source}',
  current_timestamp,
  cnt,
  4096,
  current_timestamp
FROM (SELECT COUNT(*) AS cnt FROM ${CATALOG}.${DATA_SCHEMA}.${table})
`,
  ];
}

function refreshColumns(datasetId, table) {
  return [
    `
DELETE FROM ${CATALOG}.${META_SCHEMA}.catalog_columns
WHERE dataset_id='${datasetId}'
`,
    `
INSERT INTO ${CATALOG}.${META_SCHEMA}.catalog_columns
(dataset_id, column_name, data_type, pii_flag, null_ratio, distinct_ratio)
SELECT
  '${datasetId}',
  c.column_name,
  c.data_type,
  CASE WHEN regexp_like(lower(c.column_name), '(email|phone|ssn|dob|address)') THEN TRUE ELSE FALSE END,
  0.0,
  0.0
FROM ${CATALOG}.information_schema.columns c
WHERE c.table_schema='${DATA_SCHEMA}' AND c.table_name='${table}'
`,
  ];
}

// -------------------- Main flow --------------------

async function dropAllEntities() {
  console.log(`Dropping ALL tables in ${CATALOG}.${DATA_SCHEMA} …`);
  const list = await trinoQuery(`
    SELECT table_name
    FROM ${CATALOG}.information_schema.tables
    WHERE table_schema='${DATA_SCHEMA}'
  `);
  const rows = list.data || [];
  if (!rows.length) {
    console.log("No tables to drop.");
    return;
  }
  for (const [table_name] of rows) {
    const t = String(table_name);
    console.log(`- DROP TABLE ${CATALOG}.${DATA_SCHEMA}.${t}`);
    await trinoExec(`DROP TABLE IF EXISTS ${CATALOG}.${DATA_SCHEMA}.${t}`);
  }
  console.log("✓ Drops complete.");
}

async function main() {
  console.log(`Rebuilding ${ENTITIES} businesslike entities → ${CATALOG}.${DATA_SCHEMA} via ${TRINO_URL}`);

  // Ensure metadata tables
  for (const stmt of ensureMetaTables()) {
    await trinoExec(stmt.trim());
  }

  // Nuke existing demo entities
  await dropAllEntities();

  // Plan and create new set
  const plan = planEntities(ENTITIES);
  for (const { logical, source, table, datasetId } of plan) {
    const location = `${WAREHOUSE}/datasets/${table}`;
    console.log(`\n→ Creating ${logical} [${table}] @ ${location}`);

    await trinoExec(createTable(table, location).trim());
    await trinoExec(insertRows(table, ROWS).trim());
    for (const stmt of upsertDataset(datasetId, logical, source, table)) {
      await trinoExec(stmt.trim());
    }
    for (const stmt of refreshColumns(datasetId, table)) {
      await trinoExec(stmt.trim());
    }

    console.log(`✓ ${logical} seeded (${ROWS} rows) — dataset_id=${datasetId}, source=${source}`);
  }

  console.log(`\nALL DONE ✅  Refresh the UI and hit Publish on any tile.`);
}

main().catch((e) => {
  console.error("Reseed failed:", e?.stack || e?.message || String(e));
  process.exit(1);
});

