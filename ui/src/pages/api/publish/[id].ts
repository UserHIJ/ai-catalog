// pages/api/publish/[id].ts
//
// Publish pipeline (withDuck edition):
// - Uses DuckDB + httpfs + iceberg inside a scoped session (with MinIO/S3 config)
// - Reads from Iceberg via iceberg_scan('<WAREHOUSE>/datasets/<table>') with LIMIT
// - Embeds text via OpenAI text-embedding-3-small (1536 dims)
// - Writes to Postgres into `embeddings` with a surrogate key (embedding_id BIGSERIAL)
// - Minimal, actionable logs to server terminal (stdout)
//
// REQUIRED ENV:
//   WAREHOUSE=s3://iceberg-warehouse             // your Iceberg warehouse root
//   OPENAI_API_KEY=sk-...                        // for embeddings
//   PG_URL=postgres://user:pass@host:5432/db     // Postgres DSN
//
// MinIO/S3 session config for DuckDB httpfs (no scheme in endpoint!):
//   MINIO_ENDPOINT=localhost:9000
//   MINIO_USE_SSL=false                          // "true" | "false"
//   MINIO_REGION=us-east-1
//   MINIO_ACCESS_KEY=minioadmin
//   MINIO_SECRET_KEY=minioadmin
//
import type { NextApiRequest, NextApiResponse } from "next";
import duckdb from "duckdb";
import { Client as PgClient } from "pg";
import OpenAI from "openai";

/* ============================
 * ENV + CONSTANTS
 * ============================ */
const WAREHOUSE = process.env.WAREHOUSE || "s3://iceberg-warehouse";
const PG_URL = process.env.PG_URL || "postgres://postgres:postgres@localhost:5432/postgres";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "sk-cccccc";

// MinIO/S3 for httpfs - HARDCODED VALUES
const MINIO_ENDPOINT = "localhost:9000"; // e.g. "localhost:9000" (NO http://)
const MINIO_USE_SSL = false;
const MINIO_REGION = "us-east-1";
const MINIO_ACCESS_KEY = "minioadmin";
const MINIO_SECRET_KEY = "minioadmin";

// Embedding model (1536 dims)
const EMBEDDING_MODEL = "text-embedding-3-small";
const EMBEDDING_DIMS = 1536;

// Batch sizes
const BATCH = 100;

/* ============================
 * SMALL UTILS
 * ============================ */
function jsonSafe<T>(obj: T): T {
  return JSON.parse(
    JSON.stringify(obj, (_k, v) => (typeof v === "bigint" ? v.toString() : v))
  );
}

// "ds_salesforce_accounts" -> "salesforce_accounts"
function stripPrefixDs(datasetId: string): string {
  return datasetId.startsWith("ds_") ? datasetId.slice(3) : datasetId;
}

// Serialize a row safely (preserve bigints as strings)
function rowToText(row: any): string {
  return JSON.stringify(
    row,
    (_k, v) => (typeof v === "bigint" ? v.toString() : v)
  );
}

// pgvector literal: [1,2,3]
function toPgVectorLiteral(vals: number[]): string {
  return `[${vals.join(",")}]`;
}

/* ============================
 * OPENAI
 * ============================ */
async function embedBatch(
  openai: OpenAI,
  inputs: string[]
): Promise<number[][]> {
  const resp = await openai.embeddings.create({
    model: EMBEDDING_MODEL,
    input: inputs,
  });
  return resp.data.map((d) => d.embedding as number[]);
}

/* ============================
 * DUCKDB (withDuck)
 * ============================ */
// Promisified `conn.all(sql)` helper
function allAsync(conn: duckdb.Connection, sql: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err: any, rows: any[]) => (err ? reject(err) : resolve(rows)));
  });
}

async function withDuck<T>(
  fn: (conn: duckdb.Connection) => Promise<T>
): Promise<T> {
  const db = new duckdb.Database(":memory:");
  const conn = await db.connect();
  try {
    // Extensions
    await conn.exec("INSTALL httpfs; LOAD httpfs;");
    await conn.exec("INSTALL iceberg; LOAD iceberg;");

    // Verbose logs to server terminal (use 'debug' if trace is too chatty)
    await conn.exec("SET logging_level='trace'");

    // MinIO/S3 session config (endpoint WITHOUT scheme; SSL controlled separately)
    if (MINIO_ENDPOINT) {
      await conn.exec(`SET s3_endpoint='${MINIO_ENDPOINT.replace(/'/g, "''")}'`);
      await conn.exec(`SET s3_use_ssl=${MINIO_USE_SSL ? "true" : "false"}`);
      await conn.exec(`SET s3_url_style='path'`); // MinIO prefers path-style
    }
    if (MINIO_REGION) {
      await conn.exec(`SET s3_region='${MINIO_REGION.replace(/'/g, "''")}'`);
    }
    if (MINIO_ACCESS_KEY) {
      await conn.exec(
        `SET s3_access_key_id='${MINIO_ACCESS_KEY.replace(/'/g, "''")}'`
      );
    }
    if (MINIO_SECRET_KEY) {
      await conn.exec(
        `SET s3_secret_access_key='${MINIO_SECRET_KEY.replace(/'/g, "''")}'`
      );
    }

    // Dev convenience: allow guessing latest Iceberg version if version-hint is missing
    await conn.exec("SET unsafe_enable_version_guessing=true");

    return await fn(conn);
  } finally {
    await conn.close();
  }
}

/* ============================
 * POSTGRES
 * ============================ */


/* ============================
 * TYPES
 * ============================ */
type Ok =
  | {
      request_id: string;
      status: "published";
      datasetId: string;
      totalRows: number;
      inserted: number;
      latency_ms: number;
    }
  | {
      request_id: string;
      status: "no-new-rows";
      datasetId: string;
      totalRows: 0;
      inserted: 0;
      latency_ms: number;
    };

type Err = { request_id: string; error: string; latency_ms: number };

/* ============================
 * HANDLER
 * ============================ */
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<Ok | Err>
) {
  if (req.method !== "POST") {
    return res
      .status(405)
      .json(jsonSafe({ request_id: "0", error: "POST only", latency_ms: 0 }));
  }

  const started = Date.now();
  const request_id = `${started}-${Math.floor(Math.random() * 1e6)}`;

  try {
    // Env + inputs
    if (!WAREHOUSE) {
      return res.status(500).json(
        jsonSafe({
          request_id,
          error: "WAREHOUSE env var missing",
          latency_ms: Date.now() - started,
        })
      );
    }
    if (!OPENAI_API_KEY) {
      return res.status(500).json(
        jsonSafe({
          request_id,
          error: "OPENAI_API_KEY missing",
          latency_ms: Date.now() - started,
        })
      );
    }

    const rawId = Array.isArray(req.query.id) ? req.query.id[0] : req.query.id;
    const datasetId = String(rawId || "");
    if (!datasetId) {
      return res.status(400).json(
        jsonSafe({
          request_id,
          error: "missing dataset id",
          latency_ms: Date.now() - started,
        })
      );
    }

    // Limit (body) — default 200, clamp 1..10000
    const limit = Math.max(
      1,
      Math.min(10_000, Number(req.body?.limit ?? 200))
    );

    console.log("[publish] start", {
      request_id,
      datasetId,
      limit,
      ts: new Date().toISOString(),
    });

    // PG + OpenAI
    const pg = new PgClient({ connectionString: PG_URL });
    await pg.connect();
    //await ensureEmbeddingsTable(pg);
    const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

    // Iceberg path
    const tableName = stripPrefixDs(datasetId); // e.g., "salesforce_accounts"
    const path = `${WAREHOUSE.replace(/\/+$/, "")}/datasets/${tableName}`;

    // Read from Iceberg (no dedupe), bounded by LIMIT
    type Row = Record<string, any>;
    const rows: Row[] = await withDuck(async (conn) => {
      const esc = path.replace(/'/g, "''");
      const sql = `
        SELECT *
        FROM iceberg_scan('${esc}')
        LIMIT ${limit}
      `;

      console.log("[publish] rows-sql-exec", { sql });

      // Use promisified all() to actually get an array
      const out = (await allAsync(conn, sql)) as Row[];

      return out;
    });

    console.log("[publish] rows-read", {
      request_id,
      datasetId,
      path,
      count: Array.isArray(rows) ? rows.length : -1,
      limit,
    });

    if (!Array.isArray(rows) || rows.length === 0) {
      console.log("[publish] no-new-rows", {
        request_id,
        datasetId,
        latency_ms: Date.now() - started,
      });
      await pg.end().catch(() => {});
      return res.status(200).json(
        jsonSafe({
          request_id,
          status: "no-new-rows",
          datasetId,
          totalRows: 0,
          inserted: 0,
          latency_ms: Date.now() - started,
        })
      );
    }

    // Prepare for embedding
    const texts = rows.map(rowToText);

    // Embed + insert in batches
    let inserted = 0;
    for (let i = 0; i < rows.length; i += BATCH) {
      const chunkRows = rows.slice(i, i + BATCH);
      const chunkTexts = texts.slice(i, i + BATCH);

      const embs = await embedBatch(openai, chunkTexts);

      const insertSQL = `
        INSERT INTO embeddings (dataset_id, pk, text_chunk, embedding)
        VALUES ($1, $2, $3, $4::vector)
      `;

      for (let j = 0; j < chunkRows.length; j++) {
        const row = chunkRows[j];

        // Keep a human-reference pk (not unique, history preserved via BIGSERIAL)
        const sourcePk =
          row?.email != null
            ? `${String(row.id)}:${String(row.email)}`
            : String(row.id);

        if (i === 0 && j < 3) {
          console.log("[pg insert probe]", {
            request_id,
            datasetId,
            pk: sourcePk,
            textPreview: chunkTexts[j]?.slice(0, 100),
            embeddingDim: embs[j]?.length,
          });
        }

        await pg.query(insertSQL, [
          datasetId,
          sourcePk,
          chunkTexts[j],
          toPgVectorLiteral(embs[j]),
        ]);
        inserted += 1;
      }
    }

    console.log("[publish] done", {
      request_id,
      datasetId,
      path,
      totalRows: rows.length,
      inserted,
      latency_ms: Date.now() - started,
    });

    await pg.end().catch(() => {});

    return res.status(200).json(
      jsonSafe({
        request_id,
        status: "published",
        datasetId,
        totalRows: rows.length,
        inserted,
        latency_ms: Date.now() - started,
      })
    );
  } catch (err: any) {
    console.error("[publish] error", {
      request_id,
      error: err?.message || String(err),
      stack: err?.stack,
    });
    return res.status(500).json(
      jsonSafe({
        request_id,
        error: err?.message || "Internal error",
        latency_ms: Date.now() - started,
      })
    );
  }
}
