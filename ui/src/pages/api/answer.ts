// pages/api/answer.ts
import type { NextApiRequest, NextApiResponse } from "next";
import { Pool } from "pg";
import OpenAI from "openai";

/**
 * Required env:
 *  - OPENAI_API_KEY="sk-..."
 *  - (one of) DATABASE_URL or PG_URL, e.g.:
 *      postgres://user:password@host:5432/db
 *    If your password has special chars (@:/?#&), URL-encode them.
 *
 * Schema expectations:
 *  - Table: embeddings
 *    - dataset_id TEXT NULL
 *    - pk TEXT NOT NULL
 *    - text_chunk TEXT NOT NULL
 *    - embedding VECTOR(1536) NOT NULL  -- pgvector
 *
 * Recommended index:
 *  CREATE INDEX ON embeddings USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
 *  ANALYZE embeddings;
 */

// ---------- Env + Pool (robust, supports PG_URL) ----------
const DATABASE_URL = process.env.DATABASE_URL ?? process.env.PG_URL;
if (!DATABASE_URL) {
  throw new Error(
    "Missing Postgres URL. Set DATABASE_URL or PG_URL. Example:\n" +
      "  DATABASE_URL=postgres://user:password@host:5432/db\n" +
      "Tip: URL-encode special characters in the password."
  );
}

// Many hosted Postgres require SSL. Disable locally with PGSSL=disable
const ssl =
  process.env.PGSSL === "disable" ? false : ({ rejectUnauthorized: false } as const);

function maskUrl(url: string) {
  try {
    const u = new URL(url);
    if (u.password) u.password = "*****";
    return u.toString();
  } catch {
    return "(invalid DATABASE_URL/PG_URL)";
  }
}

let pool: Pool;
try {
  pool = new Pool({ connectionString: DATABASE_URL, ssl });
} catch (e: any) {
  throw new Error(`Failed to init Postgres pool for ${maskUrl(DATABASE_URL)}: ${e?.message || e}`);
}

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Tunables
const EMBEDDING_MODEL = process.env.EMBEDDING_MODEL ?? "text-embedding-3-small"; // 1536 dims
const CHAT_MODEL = process.env.CHAT_MODEL ?? "gpt-4o-mini";
const MAX_CTX_CHARS = 12000; // keep prompt sane
const DEFAULT_K = 5;
const MAX_K = 50;

// cosine distance -> similarity
function toSimilarity(distance: number | null | undefined) {
  if (distance == null) return 0;
  const sim = 1 - distance; // pgvector cosine distance = 1 - cosine_similarity
  return Math.max(0, Math.min(1, sim));
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const t0 = Date.now();

  if (req.method !== "POST") {
    return res.status(405).json({ error: "POST only" });
  }

  try {
    const { question, datasetId, k } = req.body ?? {};
    const trimmedQ = (question ?? "").toString().trim();
    const K = Math.max(1, Math.min(Number.isFinite(+k) ? +k : DEFAULT_K, MAX_K));
    const ds = (datasetId ?? null) && String(datasetId).trim() ? String(datasetId).trim() : null;

    if (!trimmedQ) {
      return res.status(400).json({ error: "Missing 'question'." });
    }

    // 1) Embed the query (must match stored vector dims)
    const e0 = Date.now();
    const embResp = await openai.embeddings.create({
      model: EMBEDDING_MODEL,
      input: trimmedQ,
    });
    const qvec = embResp.data[0]?.embedding;
    if (!qvec || !Array.isArray(qvec)) {
      throw new Error("Failed to get embedding for question.");
    }
    const embed_ms = Date.now() - e0;

    // 2) Retrieve neighbors from Postgres/pgvector — identical to /api/ask
    const client = await pool.connect();
    let rows: {
      dataset_id: string | null;
      pk: string;
      text_chunk: string;
      distance: number;
    }[] = [];
    try {
      const sql = ds
        ? `
          SELECT dataset_id, pk, text_chunk, (embedding <=> $1::vector) AS distance
          FROM embeddings
          WHERE dataset_id = $2
          ORDER BY embedding <=> $1::vector
          LIMIT $3;
        `
        : `
          SELECT dataset_id, pk, text_chunk, (embedding <=> $1::vector) AS distance
          FROM embeddings
          ORDER BY embedding <=> $1::vector
          LIMIT $2;
        `;
      const params = ds ? [qvec, ds, K] : [qvec, K];
      const r = await client.query(sql, params);
      rows = r.rows;
    } finally {
      client.release();
    }

    // 3) Relevance gate
    const top = rows[0];
    const topSim = toSimilarity(top?.distance);
    const relevant = topSim >= 0.6; // tweak to taste

    // Citations + context
    const citations = rows.map((r) => ({ dataset_id: r.dataset_id, pk: r.pk }));

    let accumulated = 0;
    const contextBlocks: string[] = [];
    for (const r of rows) {
      const chunk = r.text_chunk || "";
      if (accumulated + chunk.length > MAX_CTX_CHARS) break;
      contextBlocks.push(
        [
          `# Source`,
          `dataset_id: ${r.dataset_id ?? "NULL"}`,
          `pk: ${r.pk}`,
          `similarity: ${toSimilarity(r.distance).toFixed(4)}`,
          ``,
          chunk,
        ].join("\n")
      );
      accumulated += chunk.length;
    }

    let answer = "I don't know.";

    // 4) Generate final answer grounded in retrieved context
    if (relevant && contextBlocks.length > 0) {
      const sys = [
        "You are a precise data catalog assistant.",
        "Answer ONLY using the provided context. If the context does not contain the answer, say you don't know.",
        "Be concise. When helpful, cite dataset_id and pk from the sources.",
      ].join(" ");

      const userPrompt = [
        `Question: ${trimmedQ}`,
        ``,
        `Context (top ${contextBlocks.length}):`,
        contextBlocks.join("\n\n---\n\n"),
        ``,
        `Instructions:`,
        `- If a beacon phrase (e.g., purple-elephant-42) appears in a source, surface it and cite it.`,
        `- If you don't have enough info, say "I don't know."`,
      ].join("\n");

      const chat = await openai.chat.completions.create({
        model: CHAT_MODEL,
        temperature: 0.2,
        messages: [
          { role: "system", content: sys },
          { role: "user", content: userPrompt },
        ],
      });

      const content = chat.choices?.[0]?.message?.content?.trim();
      if (content) answer = content;
    }

    const latency_ms = Date.now() - t0;

    return res.status(200).json({
      request_id: `${t0}-${Math.random().toString(36).slice(2, 10)}`,
      question: trimmedQ,
      datasetId: ds,
      k: K,
      latency_ms,
      answer,
      citations,
      relevant,
      top1_similarity: Number(topSim.toFixed(4)),
      top3_distances: rows.slice(0, 3).map((r) => Number(r.distance.toFixed(6))),
      embed_ms,
      retriever_model: EMBEDDING_MODEL,
      chat_model: CHAT_MODEL,
      db: maskUrl(DATABASE_URL), // handy for sanity checks
    });
  } catch (err: any) {
    const raw = (req as any)?._startTime;
    const started =
      typeof raw === "number" ? raw : raw instanceof Date ? raw.getTime() : Date.now();
    const latency_ms = Date.now() - started;
    return res.status(500).json({ error: err?.message ?? "Unknown error", latency_ms });
  }
}
