// lib/publish.ts
import { q } from "@/lib/duckdb";
import { Pool } from "pg";
import OpenAI from "openai";

const W = process.env.WAREHOUSE!;
const PG_URL = process.env.PG_URL || "postgres://postgres:postgres@localhost:5432/postgres";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY!;

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
const pool = new Pool({ connectionString: PG_URL });

type PublishResult = {
  datasetId: string;
  chosenPath: string | null;
  triedPaths: string[];
  totalRows: number;
  inserted: number;
};

function rowToText(row: any) {
  return JSON.stringify(row);
}

async function embedBatch(texts: string[]) {
  const res = await openai.embeddings.create({
    model: "text-embedding-3-small",
    input: texts,
  });
  return res.data.map((d) => d.embedding as number[]);
}

function toVec(arr: number[]) {
  return `[${arr.join(",")}]`;
}

async function probe(path: string): Promise<boolean> {
  try {
    await q(`SELECT * FROM iceberg_scan('${path}') LIMIT 1`);
    return true;
  } catch {
    return false;
  }
}

function idToEntityName(id: string): string | null {
  const m = id.match(/^ds_(entity_\d{3})$/);
  return m ? m[1] : null;
}

export async function publishDataset(datasetId: string, limit = 200): Promise<PublishResult> {
  if (!W) throw new Error("WAREHOUSE env var missing");
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY not set");
  if (!datasetId) throw new Error("datasetId required");

  // 1. Look up logical name from catalog
  const escId = datasetId.replace(/'/g, "''");
  const catSql = `SELECT dataset_id, name FROM iceberg_scan('${W}/catalog/catalog_datasets') WHERE dataset_id='${escId}'`;
  const [meta] = await q(catSql);
  if (!meta) throw new Error(`dataset not found in catalog: ${datasetId}`);

  const logicalName = (meta as any).name as string;
  const entityName = idToEntityName(datasetId);

  // 2. Candidate Iceberg paths
  const candidates: string[] = [];
  candidates.push(`${W}/datasets/${logicalName}`);
  if (entityName) candidates.push(`${W}/datasets/${entityName}`);
  candidates.push(`${W}/demo/${logicalName}`);
  candidates.push(`${W}/${logicalName}`);

  let chosen: string | null = null;
  const tried: string[] = [];
  for (const c of candidates) {
    tried.push(c);
    // eslint-disable-next-line no-await-in-loop
    if (await probe(c)) { chosen = c; break; }
  }
  if (!chosen) {
    return { datasetId, chosenPath: null, triedPaths: tried, totalRows: 0, inserted: 0 };
  }

  // 3. Pull rows
  const rows = await q(`SELECT * FROM iceberg_scan('${chosen}') LIMIT ${Number(limit)}`);
  const total = rows.length;
  if (total === 0) {
    return { datasetId, chosenPath: chosen, triedPaths: tried, totalRows: 0, inserted: 0 };
  }

  // 4. Insert embeddings
  const client = await pool.connect();
  try {
    const insertSQL =
      "INSERT INTO embeddings (dataset_id, pk, text_chunk, embedding) VALUES ($1,$2,$3,$4::vector) ON CONFLICT (dataset_id, pk) DO NOTHING";

    const BATCH = 100;
    let inserted = 0;

    for (let i = 0; i < rows.length; i += BATCH) {
      const batch = rows.slice(i, i + BATCH);
      const texts = batch.map(rowToText);
      // eslint-disable-next-line no-await-in-loop
      const embs = await embedBatch(texts);

      await client.query("BEGIN");
      try {
        for (let j = 0; j < batch.length; j++) {
          const row: any = batch[j];
          const pk = String(row.id ?? `${i + j}`);
          // eslint-disable-next-line no-await-in-loop
          await client.query(insertSQL, [datasetId, pk, texts[j], toVec(embs[j])]);
          inserted++;
        }
        await client.query("COMMIT");
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      }
    }

    return { datasetId, chosenPath: chosen, triedPaths: tried, totalRows: total, inserted };
  } finally {
    client.release();
  }
}
