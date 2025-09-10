// /lib/publishEmbeddings.ts
import crypto from "crypto";
import { Pool } from "pg";

/** REQUIRED ENV:
 *   TRINO_URL=http://localhost:8080
 *   TRINO_USER=web
 *   TRINO_SOURCE=u                      // optional, we send if present
 *   PG_URL=postgres://postgres:postgres@localhost:5432/postgres
 *   PGSSL=disable                        // or "require"
 *   OPENAI_API_KEY=sk-...
 *
 * OPTIONAL (reasonable defaults):
 *   OPENAI_EMBED_MODEL=text-embedding-3-small
 *   EMBEDDINGS_TABLE=public.embeddings
 *   TRINO_CATALOG=iceberg
 *   TRINO_SCHEMA=datasets
 */
const {
  TRINO_URL,
  TRINO_USER = "web",
  TRINO_SOURCE,
  PG_URL,
  PGSSL,
  OPENAI_API_KEY,
  OPENAI_EMBED_MODEL = "text-embedding-3-small",
  EMBEDDINGS_TABLE = "public.embeddings",
  TRINO_CATALOG = "iceberg",
  TRINO_SCHEMA = "datasets",
} = process.env;

if (!TRINO_URL) throw new Error("TRINO_URL env is required");
if (!PG_URL) throw new Error("PG_URL env is required");
if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY env is required");

const pgPool = new Pool({
  connectionString: PG_URL,
  ssl:
    PGSSL && PGSSL.toLowerCase() !== "disable"
      ? { rejectUnauthorized: false }
      : undefined,
});

type RowObject = Record<string, any>;

type PublishOptions = {
  limit?: number; // default 200
  truncate?: boolean; // default true
  tableOverride?: string;
  datasetToIceberg?: (datasetId: string) => string;
  includeColumns?: string[];
  excludeColumns?: string[];
};

export async function publishEmbeddings(
  datasetId: string,
  opts: PublishOptions = {}
): Promise<{ totalRows: number; inserted: number; targetTable: string }> {
  const {
    limit = 200,
    truncate = true,
    tableOverride,
    datasetToIceberg = (id) => `${TRINO_CATALOG}.${TRINO_SCHEMA}.${id}`,
    includeColumns,
    excludeColumns,
  } = opts;

  const targetTable = tableOverride || EMBEDDINGS_TABLE;

  // 1) Pull rows from Trino/Iceberg
  const fqTable = datasetToIceberg(datasetId);
  const rows = await trinoQueryRows(
    `SELECT * FROM ${fqTable} LIMIT ${Number(limit)}`
  );

  // 2) Build chunks (one row -> one chunk string + pk)
  const chunks = rows.map((row) => {
    const filtered = pickColumns(row, includeColumns, excludeColumns);
    const text = rowToChunk(filtered);
    const pk = pickPrimaryKey(row) ?? hashRow(filtered);
    return { pk, text };
  });

  if (chunks.length === 0) {
    if (truncate) await deleteDatasetEmbeddings(targetTable, datasetId);
    return { totalRows: 0, inserted: 0, targetTable };
  }

  // 3) Embeddings (OpenAI)
  const vectors = await embedChunksOpenAI(chunks.map((c) => c.text));
  if (vectors.length !== chunks.length) {
    throw new Error(
      `Embedding length mismatch: expected ${chunks.length}, got ${vectors.length}`
    );
  }

  // 4) Insert into Postgres (truncate-per-dataset first, if set)
  const inserted = await insertEmbeddingsPg(
    targetTable,
    datasetId,
    chunks.map((c, i) => ({
      pk: c.pk,
      text: c.text,
      embedding: vectors[i],
    })),
    { truncate }
  );

  return { totalRows: chunks.length, inserted, targetTable };
}

/* ---------- Trino REST (no extra deps) ---------- */

async function trinoQueryRows(sql: string): Promise<RowObject[]> {
  const headers: Record<string, string> = {
    "Content-Type": "text/plain",
    Accept: "application/json",
    "X-Trino-User": TRINO_USER,
    "X-Trino-Catalog": TRINO_CATALOG,
    "X-Trino-Schema": TRINO_SCHEMA,
  };
  if (TRINO_SOURCE) headers["X-Trino-Source"] = TRINO_SOURCE;

  let resp = await fetch(`${TRINO_URL}/v1/statement`, {
    method: "POST",
    headers,
    body: sql,
  });
  if (!resp.ok) throw new Error(`Trino POST failed: ${resp.status} ${resp.statusText}`);

  let payload: any = await resp.json();
  let data: any[] = [];
  let columns: { name: string }[] | null = payload.columns || null;

  if (payload.data) data.push(...payload.data);

  while (payload.nextUri) {
    resp = await fetch(payload.nextUri, { headers: { Accept: "application/json" } as any });
    if (!resp.ok) throw new Error(`Trino page fetch failed: ${resp.status} ${resp.statusText}`);
    payload = await resp.json();
    if (payload.columns && !columns) columns = payload.columns;
    if (payload.data) data.push(...payload.data);
  }

  if (!columns) return [];
  const names = columns.map((c: any) => c.name);
  return data.map((arr: any[]) => {
    const o: RowObject = {};
    for (let i = 0; i < names.length; i++) o[names[i]] = arr[i];
    return o;
  });
}

/* ---------- Chunk builder ---------- */

function pickColumns(row: RowObject, include?: string[], exclude?: string[]): RowObject {
  let keys = Object.keys(row);

  if (include && include.length) {
    const want = new Set(include.map((k) => k.toLowerCase()));
    keys = keys.filter((k) => want.has(k.toLowerCase()));
  }
  if (exclude && exclude.length) {
    const drop = new Set(exclude.map((k) => k.toLowerCase()));
    keys = keys.filter((k) => !drop.has(k.toLowerCase()));
  }

  // basic PII guard; expand as needed
  const pii = /(ssn|social|dob|birth|email|phone|mobile|address|tax|tin|ein|pan)/i;
  keys = keys.filter((k) => !pii.test(k));

  const out: RowObject = {};
  for (const k of keys) out[k] = row[k];
  return out;
}

function rowToChunk(row: RowObject): string {
  const keys = Object.keys(row).sort((a, b) => a.localeCompare(b));
  const parts: string[] = [];
  for (const k of keys) {
    const v = row[k];
    if (v === null || v === undefined || v === "") continue;
    parts.push(`${k}: ${formatVal(v)}`);
  }
  return parts.join("\n");
}

function formatVal(v: any): string {
  if (v instanceof Date) return v.toISOString();
  if (typeof v === "object") {
    try { return JSON.stringify(v); } catch { return String(v); }
  }
  return String(v);
}

function pickPrimaryKey(row: RowObject): string | null {
  for (const k of ["acct_number", "id", "pk", "uuid", "account_id", "customer_id"]) {
    if (row[k] != null && row[k] !== "") return String(row[k]);
  }
  return null;
}

function hashRow(row: RowObject): string {
  const s = JSON.stringify(row, Object.keys(row).sort());
  return crypto.createHash("sha256").update(s).digest("hex");
}

/* ---------- OpenAI embeddings ---------- */

async function embedChunksOpenAI(texts: string[]): Promise<number[][]> {
  const BATCH = 100;
  const out: number[][] = [];
  for (let i = 0; i < texts.length; i += BATCH) {
    const batch = texts.slice(i, i + BATCH);
    const resp = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: OPENAI_EMBED_MODEL, // 1536 dims with text-embedding-3-small
        input: batch,
      }),
    });
    if (!resp.ok) {
      const errTxt = await resp.text().catch(() => "");
      throw new Error(`OpenAI embeddings failed: ${resp.status} ${resp.statusText} ${errTxt}`);
    }
    const json: any = await resp.json();
    for (const item of json.data) {
      const emb: number[] = item.embedding;
      if (!Array.isArray(emb) || emb.length !== 1536) {
        throw new Error(`Unexpected embedding dimensions: ${emb?.length ?? "none"} (expected 1536)`);
      }
      out.push(emb);
    }
  }
  return out;
}

/* ---------- Postgres insert (pgvector) ---------- */

async function deleteDatasetEmbeddings(table: string, datasetId: string) {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    await client.query(`DELETE FROM ${table} WHERE dataset_id = $1`, [datasetId]);
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function insertEmbeddingsPg(
  table: string,
  datasetId: string,
  rows: { pk: string; text: string; embedding: number[] }[],
  opts: { truncate: boolean }
): Promise<number> {
  const client = await pgPool.connect();
  try {
    await client.query("BEGIN");
    if (opts.truncate) {
      await client.query(`DELETE FROM ${table} WHERE dataset_id = $1`, [datasetId]);
    }

    const BATCH = 500;
    let inserted = 0;

    for (let i = 0; i < rows.length; i += BATCH) {
      const slice = rows.slice(i, i + BATCH);

      const valuesSql: string[] = [];
      const params: any[] = [];
      let p = 1;
      for (const r of slice) {
        valuesSql.push(`($${p++}, $${p++}, $${p++}, $${p++}::vector)`);
        params.push(datasetId, r.pk, r.text, vectorLiteral(r.embedding));
      }

      const sql = `
        INSERT INTO ${table} (dataset_id, pk, text_chunk, embedding)
        VALUES ${valuesSql.join(",")}
      `;
      await client.query(sql, params);
      inserted += slice.length;
    }

    await client.query("COMMIT");
    return inserted;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

function vectorLiteral(vec: number[]): string {
  // Trim float precision to keep statement size down
  const trimmed = vec.map((x) => (Number.isFinite(x) ? Number(x.toFixed(6)) : 0));
  return `[${trimmed.join(",")}]`;
}
