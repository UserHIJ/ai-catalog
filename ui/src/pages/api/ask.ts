import type { NextApiRequest, NextApiResponse } from "next";
import OpenAI from "openai";
import { Client } from "pg";
import { performance } from "perf_hooks";

const PG_URL = process.env.PG_URL || "postgres://postgres:postgres@localhost:5432/postgres";
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });

// Flip to "<=>" if your index uses cosine ops
const DIST_OP = "<->";

function round4(n: number) {
  return Math.round(n * 1e4) / 1e4;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  const rid = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`; // simple request id

  try {
    const { question, datasetId, k } = req.body || {};
    const topK = Number.isFinite(Number(k)) ? Number(k) : 5;

    if (!question || typeof question !== "string") {
      return res.status(400).json({ error: "missing question" });
    }

    // 1) Embed the question
    const emb = await openai.embeddings.create({
      model: "text-embedding-3-small",
      input: question,
    });
    const qvec = emb.data[0].embedding as number[];

    // 2) Query pgvector (parameterized)
    const pg = new Client({ connectionString: PG_URL });
    await pg.connect();

    const vecParam = `[${qvec.join(",")}]`;
    const params: any[] = [vecParam];
    let p = 2;

    let where = "";
    if (datasetId && typeof datasetId === "string" && datasetId.trim() !== "") {
      where = `WHERE dataset_id = $${p++}`;
      params.push(datasetId.trim());
    }

    const sql = `
      SELECT dataset_id, pk, text_chunk,
             embedding ${DIST_OP} $1::vector AS distance
      FROM embeddings
      ${where}
      ORDER BY embedding ${DIST_OP} $1::vector
      LIMIT $${p}
    `;
    params.push(topK);

    const t0 = performance.now();
    const result = await pg.query(sql, params);
    const t1 = performance.now();
    await pg.end();

    const latencyMs = Math.round(t1 - t0);

    // Compute top-3 distances (rounded)
    const top3 = result.rows
      .slice(0, 3)
      .map(r => round4(Number(r.distance)));

    // ---- Telemetry log (one line, JSON) ----
    // If you want less sensitive logs, replace `question` with a hash.
    console.log("ask_telemetry", JSON.stringify({
      rid,
      question,
      datasetId: datasetId ?? null,
      k: topK,
      latency_ms: latencyMs,
      top3_distances: top3,
      rowcount: result.rows.length,
    }));

    return res.status(200).json({
      question,
      datasetId: datasetId ?? null,
      k: topK,
      results: result.rows,
      latency_ms: latencyMs,
      top3_distances: top3,
      request_id: rid,
    });
  } catch (e: any) {
    console.error("ask_error", JSON.stringify({ rid, error: String(e?.message || e) }));
    return res.status(500).json({ error: e.message || String(e), request_id: rid });
  }
}
