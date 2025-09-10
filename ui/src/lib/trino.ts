// File: src/lib/trino.ts
// Minimal Trino client using the /v1/statement REST API.
// Works in Next.js API routes (uses global fetch).

type TrinoColumn = { name: string };
type TrinoPage = {
  id?: string;
  nextUri?: string;
  columns?: TrinoColumn[];
  data?: any[][];
  error?: { message?: string; errorCode?: number; errorName?: string };
};

const TRINO_URL = process.env.TRINO_URL ?? "http://localhost:8080";
const TRINO_USER = process.env.TRINO_USER ?? "web";
const TRINO_SOURCE = process.env.TRINO_SOURCE ?? "nextjs-ui";
const TRINO_CATALOG = process.env.TRINO_CATALOG; // optional
const TRINO_SCHEMA = process.env.TRINO_SCHEMA;   // optional

function headers(catalog?: string, schema?: string): Record<string, string> {
  const h: Record<string, string> = {
    "Content-Type": "application/sql",
    "Accept": "application/json",
    "X-Trino-User": TRINO_USER,
    "X-Trino-Source": TRINO_SOURCE,
  };
  const cat = catalog ?? TRINO_CATALOG;
  const sch = schema ?? TRINO_SCHEMA;
  if (cat) h["X-Trino-Catalog"] = cat;
  if (sch) h["X-Trino-Schema"] = sch;
  return h;
}

function rowsFrom(columns: TrinoColumn[] | undefined, data: any[][] | undefined): any[] {
  if (!columns || !data || data.length === 0) return [];
  const names = columns.map(c => c.name);
  return data.map(arr => {
    const obj: any = {};
    for (let i = 0; i < names.length; i++) obj[names[i]] = arr[i];
    return obj;
  });
}

/**
 * Run a SQL query on Trino and return rows as array of objects.
 * If you fully-qualify tables (e.g., iceberg.catalog.catalog_datasets), you can omit catalog/schema.
 */
export async function q<T = any>(
  sql: string,
  opts: { catalog?: string; schema?: string } = {}
): Promise<T[]> {
  // POST the statement
  const start = await fetch(`${TRINO_URL}/v1/statement`, {
    method: "POST",
    headers: headers(opts.catalog, opts.schema),
    body: sql,
  });

  if (!start.ok) {
    const text = await start.text().catch(() => "");
    throw new Error(`Trino POST failed ${start.status}: ${text || start.statusText}`);
  }

  let page: TrinoPage = await start.json();
  if (page.error) {
    throw new Error(`${page.error.errorName || "Trino error"}: ${page.error.message || ""}`);
  }

  let cols = page.columns;
  let out: any[] = rowsFrom(cols, page.data);

  // Follow nextUri chain
  while (page.nextUri) {
    const resp = await fetch(page.nextUri, { headers: { Accept: "application/json" } });
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      throw new Error(`Trino nextUri failed ${resp.status}: ${text || resp.statusText}`);
    }
    page = await resp.json();
    if (page.error) {
      throw new Error(`${page.error.errorName || "Trino error"}: ${page.error.message || ""}`);
    }
    // columns usually only on first page; keep the originals
    if (!cols && page.columns) cols = page.columns;
    if (page.data && page.data.length) out = out.concat(rowsFrom(cols, page.data));
  }

  return out as T[];
}
