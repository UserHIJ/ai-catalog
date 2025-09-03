import type { NextApiRequest, NextApiResponse } from "next";
import { q } from "@/lib/duckdb";
import OpenAI from "openai";
import { Client as PgClient } from "pg";

// ------- ENV -------
const W = process.env.WAREHOUSE;
const PG_URL =
  process.env.PG_URL || "postgres://postgres:postgres@localhost:5432/postgres";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// ------- HELPERS -------
function idToEntityName(id: string): string | null {
  const m = id.match(/^ds_(entity_\d{3})$/);
  return m ? m[1] : null;
}

async function probe(path: string): Promise<boolean> {
  try {
    await q(`SELECT * FROM iceberg_scan('${path}') LIMIT 1`);
    return true;
  } catch {
    return false;
  }
}

function rowToText(row: any) {
  // basic text serialization; tweak if you want nicer summaries
  return JSON.stringify(row);
}

function toVec(arr: number[]) {
  // pgvector array literal (no quotes); we'll cast with ::vector in SQL
  return `[${arr.join(",")}]`;
}

async function embedBatch(openai: OpenAI, inputs: string[]): Promise<number[][]> {
  const resp = await openai.embeddings.create({
    model: "text-embedding-3-small",
    input: inputs,
  });
  return resp.data.map((d) => d.embedding as number[]);
}

// ------- HANDLER -------
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  const datasetId = String(req.query.id || "");
  const limit = Math.max(1, Math.min(10_000, Number(req.body?.limit ?? 200)));

  if (!datasetId) return res.status(400).json({ error: "missing dataset id" });
  if (!W) return res.status(500).json({ error: "WAREHOUSE env var missing" });
  if (!OPENAI_API_KEY) return res.status(500).json({ error: "OPENAI_API_KEY missing" });
  if (!PG_URL) return res.status(500).json({ error: "PG_URL missing" });

  const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
  const pg = new PgClient({ connectionString: PG_URL });

  // telemetry-ish request id
  const rid = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  const started = Date.now();

  try {
    await pg.connect();

    // 1) Look up logical name for dataset
    const escId = datasetId.replace(/'/g, "''");
    const catSql = `
      SELECT dataset_id, name
      FROM iceberg_scan('${W}/catalog/catalog_datasets')
      WHERE dataset_id='${escId}'
      LIMIT 1
    `;
    const [meta] = await q(catSql);
    if (!meta) {
      return res.status(404).json({ error: `dataset not found: ${datasetId}` });
    }
    const logicalName = (meta as any).name as string;
    const entityName = idToEntityName(datasetId);

    // 2) Resolve Iceberg table path
    const candidates: string[] = [];
    candidates.push(`${W}/datasets/${logicalName}`);
    if (entityName) candidates.push(`${W}/datasets/${entityName}`);
    candidates.push(`${W}/demo/${logicalName}`);
    candidates.push(`${W}/${logicalName}`);

    let chosen: string | null = null;
    for (const c of candidates) {
      // eslint-disable-next-line no-await-in-loop
      if (await probe(c)) { chosen = c; break; }
    }
    if (!chosen) {
      return res.status(404).json({
        status: "not-found",
        error: "could not resolve Iceberg table path",
        tried: candidates,
      });
    }

    // 3) Read up to `limit` rows from Iceberg
    const rows = await q(`SELECT * FROM iceberg_scan('${chosen}') LIMIT ${limit}`);
    if (!rows?.length) {
      return res.status(200).json({
        request_id: rid,
        status: "no-new-rows",
        totalRows: 0,
        inserted: 0,
        latency_ms: Date.now() - started,
      });
    }

    // 4) Create embeddings in batches
    const BATCH = 100;
    const texts = rows.map(rowToText);
    let inserted = 0;

    const insertSQL = `
      INSERT INTO embeddings (dataset_id, pk, text_chunk, embedding)
      VALUES ($1, $2, $3, $4::vector)
      ON CONFLICT (dataset_id, pk) DO NOTHING
    `;

    for (let i = 0; i < rows.length; i += BATCH) {
      const batchTexts = texts.slice(i, i + BATCH);
      const embs = await embedBatch(openai, batchTexts);

      for (let j = 0; j < batchTexts.length; j++) {
        const row = rows[i + j] as any;
        const pk = String(row.id ?? i + j); // choose your PK; adjust to your schema
        const vec = toVec(embs[j]);
        await pg.query(insertSQL, [datasetId, pk, batchTexts[j], vec]);
        inserted++;
      }
    }

    return res.status(200).json({
      request_id: rid,
      status: "published",
      datasetId,
      totalRows: rows.length,
      inserted,
      latency_ms: Date.now() - started,
    });
  } catch (e: any) {
    console.error("[publish:id]", rid, e?.message || e);
    return res.status(500).json({ request_id: rid, error: e?.message || String(e) });
  } finally {
    try { await pg.end(); } catch {}
  }
}