// pages/api/answer.ts
import type { NextApiRequest, NextApiResponse } from "next";
import OpenAI from "openai";
import { Client } from "pg";
import { performance } from "perf_hooks";

const PG_URL = process.env.PG_URL || "postgres://postgres:postgres@localhost:5432/postgres";
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });

// You created a cosine index (vector_cosine_ops) → use cosine operator
const DIST_OP = "<=>";

type Row = { dataset_id: string; pk: string; text_chunk: string; distance: number; };

function clamp(s: string, max: number) { return !s ? "" : (s.length <= max ? s : s.slice(0, max) + " …"); }
function round4(n: number) { return Math.round(Number(n) * 1e4) / 1e4; }

// pgvector cosine distance d ≈ 1 - cos_sim → sim ≈ 1 - d
function cosineSimFromDistance(d: number) {
  const s = 1 - Number(d);
  return Math.max(0, Math.min(1, s));
}

// very light heuristic: only treat queries with numbers/$ as numeric intent
function isNumericIntent(q: string) { return /\$?\d/.test(q); }

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  const rid = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

  try {
    const { question, datasetId, k = 6, perChunkChars = 1200 } = req.body || {};
    const topK = Number.isFinite(Number(k)) ? Number(k) : 6;
    if (!question || typeof question !== "string") return res.status(400).json({ error: "missing question" });

    // 1) Embed
    const emb = await openai.embeddings.create({ model: "text-embedding-3-small", input: question });
    const qvec = emb.data[0].embedding as number[];

    // 2) Retrieve (parameterized)
    const pg = new Client({ connectionString: PG_URL });
    await pg.connect();

    const vecParam = `[${qvec.join(",")}]`;
    const params: any[] = [vecParam];
    let p = 2, where = "";
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
    const { rows } = await pg.query<Row>(sql, params);
    await pg.end();
    const latency_ms = Math.round(performance.now() - t0);

    // 3) Build context + relevance signals
    const contexts = rows.map((r, i) => ({
      idx: i + 1,
      dataset_id: r.dataset_id,
      pk: r.pk,
      distance: Number(r.distance),
      content: clamp(r.text_chunk || "", Number(perChunkChars)),
    }));

    const contextText = contexts
      .map(c => `#${c.idx} [dataset:${c.dataset_id} pk:${c.pk} dist:${c.distance.toFixed(4)}]\n${c.content}`)
      .join("\n\n---\n\n");

    const top1 = rows[0];
    const top1Dist = top1 ? Number(top1.distance) : Infinity;
    const top1Sim = Number.isFinite(top1Dist) ? cosineSimFromDistance(top1Dist) : 0;

    // Tunables: be conservative. Below this sim → treat as irrelevant.
    const RELEVANT_SIM_THRESHOLD = 0.6;
    const relevant = rows.length > 0 && top1Sim >= RELEVANT_SIM_THRESHOLD;
    const numericIntent = isNumericIntent(question);

    // 4) Ask the model only if relevant
    let answer = "";
    if (relevant) {
      const system =
        "You are a careful data catalog assistant. Prefer concise, structured answers. " +
        "Use ONLY the supplied context. If the context is insufficient or irrelevant, say “I don't know.” " +
        "If you make a claim based on the context, cite it using [dataset_id:pk]. Never fabricate citations or data.";

      const userMsg =
        `Question: ${question}\n\n` +
        `Context:\n${contextText}\n\n` +
        `Instructions:\n` +
        `- If the context clearly answers, give a short answer with citations.\n` +
        `- If the question is vague (e.g., 'around $X'), you may list the closest matches found in context (amounts, rows, ids) with citations.\n` +
        `- If nothing relevant exists, say "I don't know."`;

      const completion = await openai.chat.completions.create({
        model: process.env.CHAT_MODEL || "gpt-4o-mini",
        messages: [{ role: "system", content: system }, { role: "user", content: userMsg }],
        temperature: 0,
        max_tokens: 500,
      });
      answer = completion.choices[0]?.message?.content?.trim() ?? "";
    } else {
      answer = "I don't know.";
    }

    // 5) Fallback ONLY if (a) relevant AND (b) numeric intent AND (c) model punted
    if ((!answer || /i don['’]t know/i.test(answer)) && relevant && numericIntent) {
      const bullets = rows.map((r) => {
        let amt: string | null = null;
        try {
          const obj = JSON.parse(r.text_chunk);
          if (typeof obj.amount === "number") amt = obj.amount.toFixed(2);
        } catch {}
        const amtPart = amt ? ` amount=$${amt}` : "";
        return `• ${r.dataset_id}:${r.pk}${amtPart} [${r.dataset_id}:${r.pk}]`;
      });
      answer = "Closest matches from context:\n" + bullets.join("\n") + "\n\n(Ask a more specific question to get a direct answer.)";
    }

    return res.status(200).json({
      request_id: rid,
      question,
      datasetId: datasetId ?? null,
      k: topK,
      latency_ms,
      top3_distances: contexts.slice(0, 3).map(c => round4(c.distance)),
      top1_similarity: round4(top1Sim),
      citations: contexts.map(c => ({ dataset_id: c.dataset_id, pk: c.pk })),
      answer,
    });
  } catch (e: any) {
    console.error("answer_error", JSON.stringify({ rid, error: String(e?.message || e) }));
    return res.status(500).json({ error: e.message || String(e), request_id: rid });
  }
}
