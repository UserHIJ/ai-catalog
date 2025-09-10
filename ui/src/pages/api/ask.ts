// /pages/api/ask.ts
import type { NextApiRequest, NextApiResponse } from "next";
import { Pool } from "pg";

/**
 * ENV required:
 *   PG_URL=postgres://user:pass@host:5432/db
 *   PGSSL=disable | require
 *   OPENAI_API_KEY=sk-...
 *
 * Optional:
 *   OPENAI_EMBED_MODEL=text-embedding-3-small
 *   OPENAI_CHAT_MODEL=gpt-4o-mini
 */

const {
  PG_URL,
  PGSSL,
  OPENAI_API_KEY,
  OPENAI_EMBED_MODEL = "text-embedding-3-small", // 1536 dims
  OPENAI_CHAT_MODEL = "gpt-4o-mini",
} = process.env;

if (!PG_URL) throw new Error("PG_URL env is required");
if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY env is required");

const pg = new Pool({
  connectionString: PG_URL,
  ssl:
    PGSSL && PGSSL.toLowerCase() !== "disable"
      ? { rejectUnauthorized: false }
      : undefined,
});

/* -------------------- types -------------------- */

type RowDB = {
  dataset_id: string;
  pk: string;
  text_chunk: string;
  distance?: number | null;
};

type Hit = {
  datasetId: string;
  pk: string;
  full: string;     // fed to LLM
  preview: string;  // UI only
  distance: number | null;
};

/* -------------------- logging utils -------------------- */

function now() { return Date.now(); }
function dur(msStart: number) { return `${(Date.now() - msStart).toFixed(0)}ms`; }

function logSection(title: string, obj?: any) {
  const ts = new Date().toISOString();
  if (obj !== undefined) {
    console.log(`[ASK] ${ts} :: ${title}`, safeLog(obj));
  } else {
    console.log(`[ASK] ${ts} :: ${title}`);
  }
}

// Avoid logging massive arrays (like vectors) verbatim
function safeLog(x: any): any {
  try {
    if (Array.isArray(x) && x.length > 50) {
      return `[array len=${x.length}]`;
    }
    if (typeof x === "object" && x !== null) {
      const out: any = {};
      for (const k of Object.keys(x)) {
        const v = (x as any)[k];
        if (Array.isArray(v) && v.length > 50) out[k] = `[array len=${v.length}]`;
        else if (typeof v === "string" && v.length > 1000) out[k] = `${v.slice(0, 1000)}… (${v.length} chars)`;
        else out[k] = v;
      }
      return out;
    }
    if (typeof x === "string" && x.length > 1000) return `${x.slice(0, 1000)}… (${x.length} chars)`;
    return x;
  } catch {
    return x;
  }
}

/* -------------------- handler -------------------- */

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const tAll = now();
  try {
    if (req.method !== "POST" && req.method !== "GET") {
      res.setHeader("Allow", "POST, GET");
      return res.status(405).json({ error: "Method Not Allowed" });
    }

    // Accept q from body OR query, with common aliases
    const body = (req.method === "POST" ? req.body : req.query) as any;
    const qRaw =
      body?.q ?? body?.question ?? body?.query ?? body?.prompt ?? body?.text ?? body?.message;
    const q = typeof qRaw === "string" ? qRaw.trim() : "";
    const datasetRaw = body?.datasetId ?? body?.dataset ?? body?.ds;
    const datasetId =
      typeof datasetRaw === "string" && datasetRaw.trim() && datasetRaw !== "all"
        ? datasetRaw.trim()
        : null;

    const K_EXACT = clampInt(Number(body?.exactLimit ?? 3), 0, 10);
    const K_SEM = clampInt(Number(body?.k ?? 8), 1, 25);

    logSection("REQUEST", { method: req.method, q, datasetId, K_EXACT, K_SEM });

    if (!q) {
      logSection("ERROR Missing question (q)");
      return res.status(400).json({ error: "Missing question (q)" });
    }

    /* ---------- EXACT ILIKE ---------- */
    const tExact = now();
    const exactPhrase = simplifyForIlike(q);
    const exactRows = K_EXACT > 0 ? await pgExact(exactPhrase, datasetId, K_EXACT) : [];
    logSection("PG exact ILIKE", {
      phrase: exactPhrase,
      rows: exactRows.length,
      took: dur(tExact),
      sample: exactRows.slice(0, 1).map(r => ({
        dataset_id: r.dataset_id,
        pk: r.pk,
        preview: r.text_chunk.slice(0, 120) + (r.text_chunk.length > 120 ? "…" : "")
      }))
    });

    /* ---------- EMBED QUERY ---------- */
    const tEmbed = now();
    const qvec = await embedQuery(q);
    logSection("OpenAI embeddings", { model: OPENAI_EMBED_MODEL, dims: qvec.length, took: dur(tEmbed) });

    /* ---------- SEMANTIC KNN ---------- */
    const tSem = now();
    const semRows = await pgSemantic(qvec, datasetId, K_SEM);
    logSection("PG semantic KNN", {
      rows: semRows.length,
      took: dur(tSem),
      top5: semRows.slice(0, 5).map(r => ({
        dataset_id: r.dataset_id,
        pk: r.pk,
        dist: typeof r.distance === "number" ? Number(r.distance).toFixed(4) : null,
        preview: r.text_chunk.slice(0, 120) + (r.text_chunk.length > 120 ? "…" : "")
      }))
    });

    /* ---------- SHAPE + MERGE ---------- */
    const exactHits = shape(exactRows);
    const semHits = shape(semRows);
    const merged = dedupe([...semHits, ...exactHits], 10);
    logSection("MERGED hits", {
      mergedCount: merged.length,
      exactCount: exactHits.length,
      semCount: semHits.length,
      mergedSample: merged.slice(0, 5).map(h => ({
        datasetId: h.datasetId, pk: h.pk,
        dist: typeof h.distance === "number" ? h.distance.toFixed(4) : null
      }))
    });

    // --- Debug: token presence in merged chunks ---
    const dbgTokens = significantTokens(q);
    const dbgTokenPresence = merged.map(h => {
      const lower = h.full.toLowerCase();
      const hits = dbgTokens.map(t => {
        const idx = lower.indexOf(t);
        return {
          token: t,
          present: idx >= 0,
          idx,
          snippet: idx >= 0
            ? lower.slice(Math.max(0, idx - 60), Math.min(lower.length, idx + 60))
            : null
        };
      });
      return {
        datasetId: h.datasetId,
        pk: h.pk,
        distance: typeof h.distance === "number" ? Number(h.distance).toFixed(4) : null,
        hits
      };
    });
    logSection("TOKEN PRESENCE", { tokens: dbgTokens, chunks: dbgTokenPresence });

    // Nothing retrieved at all → no LLM call
    if (merged.length === 0) {
      logSection("NO RETRIEVED CONTEXT → answer 'I don't know'");
      return res.status(200).json({
        answer: "I don't know.",
        used: { exact: previewOnly(exactHits), semantic: previewOnly(semHits), merged: [] },
      });
    }

    /* ---------- PROMPT BUILD ---------- */
    const contextForLlm = merged
      .map(
        (h, i) =>
          `#${i + 1} [dataset=${h.datasetId} pk=${h.pk}]\n${
            typeof h.full === "string" ? h.full : JSON.stringify(h.full)
          }`
      )
      .join("\n\n");

    const system = [
      "You are a precise data catalog assistant.",
      "Use the provided context for concrete facts (values, IDs, dates, numbers).",
      "You MAY apply universally known taxonomy/alias relationships to interpret the context (e.g., gorilla → primate → mammal; Visa → credit card; EUR → currency). If the context contradicts your inference, prefer the context.",
      "If the answer still isn’t supported by the context plus those simple inferences, reply exactly: I don't know.",
      "When you give an answer, include the dataset id and pk(s) that support it.",
    ].join(" ");

    logSection("PROMPT", {
      systemPreview: system.slice(0, 200) + (system.length > 200 ? "…" : ""),
      userHeader: `Question: ${q}`,
      contextChars: contextForLlm.length,
      chunks: merged.length
    });

    /* ---------- CHAT COMPLETION ---------- */
    const tChat = now();
    const answer = await chatComplete(system, `Question: ${q}\n\nContext:\n${contextForLlm}`);
    logSection("OpenAI chat", { model: OPENAI_CHAT_MODEL, took: dur(tChat), answerPreview: answer.slice(0, 280) });

    logSection("DONE", { total: dur(tAll) });

    return res.status(200).json({
      answer,
      used: {
        exact: previewOnly(exactHits),
        semantic: previewOnly(semHits),
        merged: previewOnly(merged),
      },
    });
  } catch (e: any) {
    logSection("ERROR", { message: e?.message, stack: e?.stack });
    return res.status(500).json({ error: e?.message || "ask failed" });
  }
}

/* -------------------- helpers -------------------- */

function clampInt(n: number, min: number, max: number) {
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}

function shape(rows: RowDB[]): Hit[] {
  return rows.map((r) => ({
    datasetId: r.dataset_id,
    pk: r.pk,
    full: r.text_chunk,
    preview: r.text_chunk.length > 200 ? r.text_chunk.slice(0, 200) + "…" : r.text_chunk,
    distance: Number.isFinite(r.distance as any) ? Number(r.distance) : null,
  }));
}

function previewOnly(hits: Hit[]) {
  // Client payload shouldn’t include the massive full chunk; previews only
  return hits.map(({ datasetId, pk, preview, distance }) => ({
    datasetId,
    pk,
    preview,
    distance,
  }));
}

function dedupe(hits: Hit[], maxOut: number): Hit[] {
  const seen = new Set<string>();
  const out: Hit[] = [];
  for (const h of hits) {
    const key = `${h.datasetId}::${h.pk}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(h);
    if (out.length >= maxOut) break;
  }
  return out;
}

function simplifyForIlike(s: string): string {
  // lower, strip punctuation and collapse whitespace
  return s.toLowerCase().replace(/[^\p{L}\p{N}\s]/gu, " ").replace(/\s+/g, " ").trim();
}

function significantTokens(q: string): string[] {
  // crude stoplist; tune as needed
  const stop = new Set([
    "a","an","the","and","or","of","for","to","is","are","any","there","be","in","on","at","with","without","do","does",
    "have","has","had","that","this","these","those","it","its","by","from","as","about","into","over","under","than",
    "then","so","such","if","whether","not","no","yes","can","could","would","should","please","find","question","ask"
  ]);
  return q
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .split(/\s+/)
    .filter((w) => w && !stop.has(w) && w.length >= 3);
}

async function pgExact(phrase: string, datasetId: string | null, limit: number): Promise<RowDB[]> {
  const t = now();
  const sql = `
    SELECT dataset_id, pk, text_chunk, 0::float8 AS distance
    FROM public.embeddings
    WHERE ($1::text IS NULL OR dataset_id = $1)
      AND text_chunk ILIKE '%' || $2 || '%'
    ORDER BY pk
    LIMIT $3
  `;
  const { rows } = await pg.query<RowDB>(sql, [datasetId, phrase, limit]);
  logSection("PG exact timings", { took: dur(t), limit, returned: rows.length });
  return rows;
}

async function pgSemantic(qvec: number[], datasetId: string | null, limit: number): Promise<RowDB[]> {
  const t = now();
  const vecLiteral = vectorLiteral(qvec); // pass as param, cast to ::vector
  const sql = `
    WITH query AS (SELECT $1::vector AS qvec)
    SELECT e.dataset_id, e.pk, e.text_chunk, (e.embedding <=> q.qvec) AS distance
    FROM public.embeddings e
    CROSS JOIN query q
    WHERE ($2::text IS NULL OR e.dataset_id = $2)
    ORDER BY e.embedding <=> q.qvec
    LIMIT $3
  `;
  const { rows } = await pg.query<RowDB>(sql, [vecLiteral, datasetId, limit]);
  logSection("PG semantic timings", { took: dur(t), limit, returned: rows.length });
  return rows;
}

async function embedQuery(q: string): Promise<number[]> {
  const t = now();
  const resp = await fetch("https://api.openai.com/v1/embeddings", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: OPENAI_EMBED_MODEL,
      input: q,
    }),
  });
  const took = dur(t);
  if (!resp.ok) {
    const ttxt = await resp.text().catch(() => "");
    logSection("OpenAI embeddings ERROR", { status: resp.status, statusText: resp.statusText, body: ttxt, took });
    throw new Error(`OpenAI embeddings failed: ${resp.status} ${resp.statusText}`);
  }
  const json: any = await resp.json();
  const emb: number[] = json?.data?.[0]?.embedding;
  if (!Array.isArray(emb) || emb.length !== 1536) {
    logSection("OpenAI embeddings BAD_DIMS", { dims: Array.isArray(emb) ? emb.length : "none", took });
    throw new Error(`Unexpected embedding dims: ${emb?.length ?? "none"} (expected 1536)`);
  }
  logSection("OpenAI embeddings OK", { took });
  return emb;
}

async function chatComplete(system: string, user: string): Promise<string> {
  const t = now();
  const resp = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${OPENAI_API_KEY}`, // <-- fixed
    },
    body: JSON.stringify({
      model: OPENAI_CHAT_MODEL,
      messages: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
      temperature: 0,
      max_tokens: 250,
    }),
  });
  const took = dur(t);
  if (!resp.ok) {
    const ttxt = await resp.text().catch(() => "");
    logSection("OpenAI chat ERROR", { status: resp.status, statusText: resp.statusText, body: ttxt, took });
    throw new Error(`OpenAI chat failed: ${resp.status} ${resp.statusText}`);
  }
  const json: any = await resp.json();
  const answer = json?.choices?.[0]?.message?.content?.trim() || "I don't know.";
  const usage = json?.usage ? { prompt: json.usage.prompt_tokens, completion: json.usage.completion_tokens, total: json.usage.total_tokens } : undefined;
  logSection("OpenAI chat OK", { took, usage });
  return answer;
}

// JS number[] -> pgvector text literal: "[v1,v2,...]"
function vectorLiteral(vec: number[]): string {
  const trimmed = vec.map((x) => (Number.isFinite(x) ? Number(x.toFixed(6)) : 0));
  return `[${trimmed.join(",")}]`;
}
