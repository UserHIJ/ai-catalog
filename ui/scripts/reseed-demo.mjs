/**
 * Reseed demo entities in Iceberg + refresh catalog metadata.
 * - Drops & recreates entity_001…entity_041 under iceberg.datasets
 * - Explicit S3/S3a location (no ${WAREHOUSE} surprises)
 * - ~24 mixed columns (1 PII), ROWS records each
 * - Upserts iceberg.catalog.catalog_datasets / catalog_columns
 *
 * ENV:
 *   TRINO_URL=http://localhost:8080
 *   TRINO_USER=dev
 *   WAREHOUSE=s3://iceberg-warehouse            # absolute URI, not ${WAREHOUSE}
 *   ENTITIES=41                                  # optional (default 41)
 *   ROWS=1000                                    # optional (default 1000)
 */

const TRINO_URL  = process.env.TRINO_URL  || "http://localhost:8080";
const TRINO_USER = process.env.TRINO_USER || "dev";
const WAREHOUSE  = process.env.WAREHOUSE  || "s3://iceberg-warehouse";
const ENTITIES   = Number(process.env.ENTITIES || 41);
const ROWS       = Number(process.env.ROWS || 1000);

const CATALOG = "iceberg";
const DATA_SCHEMA = "datasets";
const META_SCHEMA = "catalog";

if (!/^s3a?:\/\//i.test(WAREHOUSE)) {
  console.error(`WAREHOUSE must be s3:// or s3a://. Got: ${WAREHOUSE}`);
  process.exit(1);
}

// Trino REST helper: send EXACTLY ONE statement per call, follow nextUri.
async function trino(sql) {
  const start = Date.now();
  const r = await fetch(`${TRINO_URL}/v1/statement`, {
    method: "POST",
    headers: {
      "Content-Type": "text/plain",
      "X-Trino-User": TRINO_USER,
      // No X-Trino-Catalog/Schema → we fully-qualify names in SQL
    },
    body: sql,
  });
  if (!r.ok) throw new Error(`Trino HTTP ${r.status} ${r.statusText}`);

  let data = await r.json();
  while (data.nextUri) {
    const f = await fetch(data.nextUri);
    if (!f.ok) throw new Error(`Trino follow ${f.status} ${f.statusText}`);
    data = await f.json();
    if (data.error) {
      const msg = data.error.message || JSON.stringify(data.error);
      throw new Error(msg);
    }
  }
  if (data.error) throw new Error(data.error.message);

  const elapsed = Date.now() - start;
  return { stats: data.stats, elapsed };
}

function pad(n) { return String(n).padStart(3, "0"); }

function ddlCreate(table, location) {
  return `
DROP TABLE IF EXISTS ${CATALOG}.${DATA_SCHEMA}.${table}
`;
  // ^ we *must* send single statements; drop is done separately in main()
}

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
    // we assume schema iceberg.catalog exists (you listed it earlier)
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

function upsertDataset(datasetId, table, source) {
  return [
    `
DELETE FROM ${CATALOG}.${META_SCHEMA}.catalog_datasets
WHERE dataset_id='${datasetId}' OR name='${table}'
`,
    `
INSERT INTO ${CATALOG}.${META_SCHEMA}.catalog_datasets
SELECT
  '${datasetId}',
  '${table}',
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

async function main() {
  console.log(`Reseeding ${ENTITIES} entities → ${CATALOG}.${DATA_SCHEMA} via ${TRINO_URL}`);
  for (const stmt of ensureMetaTables()) {
    await trino(stmt.trim());
  }

  const sources = [
    "SAP ECC","Oracle EBS","Salesforce","ServiceNow","Zendesk",
    "Anaplan","Workday HCM","PostgreSQL","SQL Server","GitHub",
    "Google Analytics","Shopify","NetSuite"
  ];

  for (let i = 1; i <= ENTITIES; i++) {
    const num = pad(i);
    const table = `entity_${num}`;
    const datasetId = `ds_entity_${num}`;
    const source = sources[i % sources.length];
    const location = `${WAREHOUSE}/datasets/${table}`;

    console.log(`\n→ ${table} @ ${location}`);

    // drop
    await trino(`DROP TABLE IF EXISTS ${CATALOG}.${DATA_SCHEMA}.${table}`);
    // create
    await trino(createTable(table, location).trim());
    // insert rows
    await trino(insertRows(table, ROWS).trim());
    // dataset upsert
    for (const stmt of upsertDataset(datasetId, table, source)) {
      await trino(stmt.trim());
    }
    // columns refresh
    for (const stmt of refreshColumns(datasetId, table)) {
      await trino(stmt.trim());
    }

    console.log(`✓ ${table} seeded (${ROWS} rows) — dataset_id=${datasetId}`);
  }

  console.log(`\nALL DONE ✅  Refresh the UI and hit Publish on any tile.`);
}

main().catch((e) => {
  console.error("Reseed failed:", e?.stack || e?.message || String(e));
  process.exit(1);
});

