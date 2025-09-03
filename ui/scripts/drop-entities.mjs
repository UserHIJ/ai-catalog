/**
 * Drop all tables in iceberg.datasets (properly accumulate Trino pages).
 *
 * Run:
 *   TRINO_URL=http://localhost:8080 TRINO_USER=dev node ui/scripts/drop-entities.mjs
 */

const TRINO_URL  = process.env.TRINO_URL  || "http://localhost:8080";
const TRINO_USER = process.env.TRINO_USER || "dev";
const CATALOG    = "iceberg";
const SCHEMA     = "datasets";

async function trino(stmt, { catalog, schema, label } = {}) {
  const sql = stmt.trim();
  console.log(`\n=== ${label || "QUERY"} ===`);
  console.log(`TRINO_URL=${TRINO_URL}`);
  if (catalog) console.log(`CATALOG=${catalog}`);
  if (schema)  console.log(`SCHEMA=${schema}`);
  console.log(`SQL>\n${sql}\n---`);

  const headers = {
    "Content-Type": "text/plain",
    "X-Trino-User": TRINO_USER,
    "X-Presto-User": TRINO_USER,
  };
  if (catalog) {
    headers["X-Trino-Catalog"] = catalog;
    headers["X-Presto-Catalog"] = catalog;
  }
  if (schema) {
    headers["X-Trino-Schema"] = schema;
    headers["X-Presto-Schema"] = schema;
  }

  // fire first page
  let r = await fetch(`${TRINO_URL}/v1/statement`, { method: "POST", headers, body: sql });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`HTTP ${r.status} ${r.statusText}\n${t}`);
  }

  let page = await r.json();
  let allCols = page.columns || null;
  let rows = Array.isArray(page.data) ? page.data.slice() : [];
  let hops = 0;

  // follow until done, ACCUMULATING rows
  while (page.nextUri) {
    const f = await fetch(page.nextUri);
    if (!f.ok) throw new Error(`follow ${f.status} ${f.statusText}`);
    page = await f.json();
    hops++;
    if (page.error) break;
    if (!allCols && page.columns) allCols = page.columns;
    if (Array.isArray(page.data)) rows.push(...page.data);
  }

  if (page.error) {
    const msg = page.error.message || JSON.stringify(page.error);
    throw new Error(msg);
  }

  const colsStr = allCols ? allCols.map(c => `${c.name}:${c.type}`).join(", ") : "(none)";
  console.log(`Columns: ${colsStr}`);
  console.log(`Rows returned: ${rows.length}`);
  if (rows.length) {
    console.log(`Preview:\n${rows.slice(0, 10).map(r => JSON.stringify(r)).join("\n")}`);
  }
  console.log(`Follow-ups: ${hops}`);

  return { columns: allCols || [], data: rows };
}

async function main() {
  console.log(`Dropping ALL tables in ${CATALOG}.${SCHEMA} …`);

  // sanity: this should now show 1 row
  await trino("SELECT current_timestamp", { label: "ping coordinator" });

  // show catalogs/schemas (should now have data)
  await trino("SHOW CATALOGS", { label: "SHOW CATALOGS" });
  await trino("SHOW SCHEMAS FROM iceberg", { label: "SHOW SCHEMAS FROM iceberg" });

  // list tables using SHOW TABLES (best signal)
  const tablesRes = await trino(`SHOW TABLES FROM ${CATALOG}.${SCHEMA}`, {
    label: `SHOW TABLES FROM ${CATALOG}.${SCHEMA}`,
  });

  // Trino returns a single col "Table" (case varies). Grab first element of each row.
  let tableNames = (tablesRes.data || []).map(r =>
    String(Array.isArray(r) ? r[0] : r.Table || r.table || r.table_name)
  ).filter(Boolean);

  // fallback via information_schema if somehow empty
  if (!tableNames.length) {
    const info = await trino(
      `SELECT table_name FROM ${CATALOG}.information_schema.tables WHERE table_schema='${SCHEMA}' ORDER BY 1`,
      { label: "information_schema.tables" }
    );
    tableNames = (info.data || []).map(r => String(Array.isArray(r) ? r[0] : r.table_name)).filter(Boolean);
  }

  if (!tableNames.length) {
    console.log("No tables to drop.");
    return;
  }

  console.log(`Found ${tableNames.length} table(s) to drop.`);
  for (const t of tableNames) {
    // quote the identifier to be safe
    const dropSQL = `DROP TABLE IF EXISTS ${CATALOG}.${SCHEMA}."${t}"`;
    await trino(dropSQL, { label: `drop ${t}`, catalog: CATALOG, schema: SCHEMA });
  }

  // verify empty
  await trino(`SHOW TABLES FROM ${CATALOG}.${SCHEMA}`, {
    label: `post-drop SHOW TABLES FROM ${CATALOG}.${SCHEMA}`,
  });

  console.log("✓ Drops complete.");
}

main().catch(e => {
  console.error("Drop failed:", e?.stack || e?.message || String(e));
  process.exit(1);
});
