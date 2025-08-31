// scripts/embed_dataset.ts (repo root)
import { q } from "../ui/src/lib/duckdb";
import { Client } from "pg";
import OpenAI from "openai";

const W = process.env.WAREHOUSE!;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY!;
const PG_URL = process.env.PG_URL || "postgres://postgres:postgres@localhost:5432/postgres";

const DATASET_ID = process.argv[2];            // e.g. ds_entity_001
const LIMIT = Number(process.argv[3] || 200);  // rows to embed

if (!DATASET_ID) {
  console.error("usage: npx tsx ../scripts/embed_dataset.ts <dataset_id> [limit]");
  process.exit(1);
}

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
const pg = new Client({ connectionString: PG_URL });

function rowToText(row: any) {
  // basic text serialization; tweak if you want nicer summaries
  return JSON.stringify(row);
}

async function embedBatch(texts: string[]) {
  const res = await openai.embeddings.create({
    model: "text-embedding-3-small",
    input: texts,
  });
  return res.data.map((d) => d.embedding as number[]);
}

// build a pgvector literal like "[0.1,0.2,...]"
function toVec(arr: number[]) {
  return `[${arr.join(",")}]`;
}

// try iceberg_scan on a path; returns true if path is a valid Iceberg root
async function probe(path: string): Promise<boolean> {
  try {
    await q(`SELECT * FROM iceberg_scan('${path}') LIMIT 1`);
    return true;
  } catch {
    return false;
  }
}

// ds_entity_001 -> entity_001
function idToEntityName(id: string): string | null {
  const m = id.match(/^ds_(entity_\d{3})$/);
  return m ? m[1] : null;
}

(async () => {
  if (!W) throw new Error("WAREHOUSE env var missing");
  await pg.connect();

  // look up logical name from catalog
  const escId = DATASET_ID.replace(/'/g, "''");
  const catSql = `SELECT dataset_id, name FROM iceberg_scan('${W}/catalog/catalog_datasets') WHERE dataset_id='${escId}'`;
  const [meta] = await q(catSql);
  if (!meta) throw new Error(`dataset not found in catalog: ${DATASET_ID}`);

  const logicalName = (meta as any).name as string;
  const entityName = idToEntityName(DATASET_ID);

  // candidate Iceberg roots to try (in order)
  const candidates: string[] = [];
  candidates.push(`${W}/datasets/${logicalName}`);         // renamed logical path
  if (entityName) candidates.push(`${W}/datasets/${entityName}`); // original physical path
  candidates.push(`${W}/demo/${logicalName}`);             // demo schema fallback
  candidates.push(`${W}/${logicalName}`);                  // absolute fallback

  let chosen: string | null = null;
  const tried: string[] = [];

  for (const c of candidates) {
    tried.push(c);
    if (await probe(c)) {
      chosen = c;
      break;
    }
  }

  if (!chosen) {
    console.error("[embed] could not resolve Iceberg table root");
    console.error("[embed] tried:", tried);
    process.exit(2);
  }

  console.log(`[embed] reading rows from ${chosen} (limit ${LIMIT})`);
  const rows = await q(`SELECT * FROM iceberg_scan('${chosen}') LIMIT ${LIMIT}`);
  if (rows.length === 0) {
    console.log("[embed] no rows to embed, done.");
    await pg.end();
    return;
  }

  const insertSQL =
    "INSERT INTO embeddings (dataset_id, pk, text_chunk, embedding) VALUES ($1,$2,$3,$4::vector) ON CONFLICT (dataset_id, pk) DO NOTHING";

  const BATCH = 100;
  let inserted = 0;

  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const texts = batch.map(rowToText);
    const embs = await embedBatch(texts);

    for (let j = 0; j < batch.length; j++) {
      const row: any = batch[j];
      const pk = String(row.id ?? `${i + j}`);
      await pg.query(insertSQL, [DATASET_ID, pk, texts[j], toVec(embs[j])]);
      inserted++;
    }
    console.log(`[embed] inserted ${inserted}/${rows.length}`);
  }

  await pg.end();
  console.log(`[embed] done: ${inserted} vectors for ${DATASET_ID}`);
})().catch((e) => {
  console.error(e?.response?.data ?? e);
  process.exit(1);
});
