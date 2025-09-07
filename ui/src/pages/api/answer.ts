/* /pages/api/ask.ts */
import type { NextApiRequest, NextApiResponse } from "next";
import { Pool } from "pg";
import { openai, embedText, toSqlVector } from "@/lib/ai";

const DB_URL = process.env.DATABASE_URL || process.env.PG_URL;
if (!DB_URL) throw new Error("Missing DATABASE_URL/PG_URL for Postgres connection");
const pool = new Pool({ connectionString: DB_URL });

/** LOGGING ONLY — do not execute what this prints */
function escapeLiteralForLog(val: unknown): string {
  if (val === null || val === undefined) return "NULL";
  if (typeof val === "number" && Number.isFinite(val)) return String(val);
  if (typeof val === "boolean") return val ? "TRUE" : "FALSE";
  if (typeof val === "string") {
    // If looks like a pgvector literal, keep unquoted
    if (/^\s*\[\s*-?\d+(?:\.\d+)?(?:\s*,\s*-?\d+(?:\.\d+)?)*\s*\]\s*$/.test(val)) return val;
    return `'${val.replace(/'/g, "''")}'`;
  }
  try {
    const s = JSON.stringify(val);
    return `'${s.replace(/'/g, "''")}'`;
  } catch {
    return `'${String(val).replace(/'/g, "''")}'`;
  }
}
function interpolate(sql: string, params: unknown[]): string {
  return sql.replace(/\$(\d+)\b/g, (_, i) => escapeLiteralForLog(params[Number(i) - 1]));
}

type RowDB = {
  dataset_id: string;
  pk: string;
  text_chunk: string;
  distance: number;
};
type Hit = { datasetId: string; pk: string; preview: string; distance: number };

type AskSuccess = {
  ok: true;
  datasetId: string | null;
  question: string;
  k: number;
  results: { exact: Hit[]; semantic: Hit[] };
  answer?: string;
  llm_model?: string;
  embedding_model?: string;
  token_usage?: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
  latency_ms?: number;
};
type AskError = { error: string };

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<AskSuccess | AskError>
) {
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  const started = Date.now();
  const body = typeof req.body === "string" ? safeParse(req.body) : req.body ?? {};
  const datasetId: string | null =
    typeof body?.datasetId === "string" && body.datasetId.trim() ? body.datasetId.trim() : null;
  const question: string =
    typeof body?.question === "string" && body.question.trim() ? body.question.trim() : "";
  const k = Number.isFinite(body?.k) && body.k > 0 && body.k <= 50 ? Number(body.k) : 5;
  const useLlm: boolean = !!body?.useLlm;

  if (!question) return res.status(400).json({ error: "Missing 'question' text" });

  const client = await pool.connect();
  try {
    // ---------- EXACT (substring) ----------
    const exactSql = `
      SELECT dataset_id, pk, text_chunk, 0 AS distance
      FROM public.embeddings
      WHERE ($1::text IS NULL OR dataset_id = $1)
        AND text_chunk ILIKE '%' || $2 || '%'
      ORDER BY pk
      LIMIT $3
    `;
    const exactParams = [datasetId ?? null, question, k];
    console.log("\nASK exact:\n" + interpolate(exactSql, exactParams));
    const exactRes = await client.query<RowDB>(exactSql, exactParams);

    // ---------- SEMANTIC (pgvector) ----------
    // Build a query embedding from the user's question; skip if we can’t
    let semanticResRows: RowDB[] = [];
    let qvec: number[] | null = null;
    try {
      qvec = await embedText(question);
    } catch (e: any) {
      console.warn("embedText failed:", e?.message || e);
    }

    if (qvec) {
      const qv = toSqlVector(qvec);
      const semSql = `
        WITH query AS (SELECT $1::vector AS qvec)
        SELECT e.dataset_id, e.pk, e.text_chunk,
               (e.embedding <#> q.qvec) AS distance
        FROM public.embeddings e
        CROSS JOIN query q
        WHERE ($2::text IS NULL OR e.dataset_id = $2)
        ORDER BY e.embedding <#> q.qvec
        LIMIT $3
      `;
      const semParams = [qv, datasetId ?? null, k];
      console.log("\nASK semantic:\n" + interpolate(semSql, semParams));
      const semRes = await client.query<RowDB>(semSql, semParams);
      semanticResRows = semRes.rows;
    } else {
      console.warn("No query embedding; semantic retrieval skipped.");
    }

    // ---------- Shape output ----------
    const shape = (rows: RowDB[]): Hit[] =>
      rows.map((r) => ({
        datasetId: r.dataset_id,
        pk: r.pk,
        preview: r.text_chunk.length > 200 ? r.text_chunk.slice(0, 200) + "…" : r.text_chunk,
        distance: Number.isFinite(r.distance) ? r.distance : 0,
      }));

    const exact = shape(exactRes.rows);
    const semantic = shape(semanticResRows);

    // ---------- Optional: LLM answer grounded in both exact+semantic ----------
    let answer: string | undefined;
    let llm_model: string | undefined;
    let token_usage: AskSuccess["token_usage"] | undefined;

    if (useLlm && openai) {
      const context = [...exact, ...semantic]
        // de-dupe by dataset+pk while keeping order
        .filter(
          ((seen) => (h) => {
            const key = `${h.datasetId}|${h.pk}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
          })(new Set<string>())
        )
        .slice(0, Math.max(3, k)); // don’t overstuff the prompt

      const sys =
        "You are a precise data catalog assistant. Use only the provided context. If the answer isn't present, say you don't know. When helpful, cite pk values inline.";
      const user = [
        `Dataset ID: ${datasetId ?? "ALL"}`,
        `Question: ${question}`,
        "Context rows:",
        context
          .map(
            (h, i) =>
              `#${i + 1} [dataset=${h.datasetId} pk=${h.pk}] ${typeof h.preview === "string" ? h.preview : JSON.stringify(h.preview)}`
          )
          .join("\n\n") || "(none)",
      ].join("\n\n");

      try {
        const chat = await openai.chat.completions.create({
          model: process.env.OPENAI_MODEL || "gpt-4o-mini",
          messages: [
            { role: "system", content: sys },
            { role: "user", content: user },
          ],
          temperature: 0.2,
          max_tokens: 400,
        });
        answer = chat.choices?.[0]?.message?.content?.trim() || undefined;
        llm_model = process.env.OPENAI_MODEL || "gpt-4o-mini";
        const u: any = (chat as any)?.usage;
        if (u) {
          token_usage = {
            prompt_tokens: u.prompt_tokens ?? 0,
            completion_tokens: u.completion_tokens ?? 0,
            total_tokens:
              (u.prompt_tokens ?? 0) + (u.completion_tokens ?? 0),
          };
        }
      } catch (err: any) {
        console.warn("LLM call failed:", err?.message || err);
      }
    } else if (useLlm && !openai) {
      console.warn("UseLlm=true but OPENAI_API_KEY not set.");
    }

    const latency_ms = Date.now() - started;
    return res.status(200).json({
      ok: true,
      datasetId,
      question,
      k,
      results: { exact, semantic },
      ...(answer ? { answer } : {}),
      ...(llm_model ? { llm_model } : {}),
      embedding_model: "text-embedding-3-small",
      ...(token_usage ? { token_usage } : {}),
      latency_ms,
    });
  } catch (e: any) {
    console.error("ASK ERROR:", e?.message || e);
    return res.status(400).json({ error: e?.message || "ask failed" });
  } finally {
    client.release();
  }
}

function safeParse(s: string): any {
  try {
    return JSON.parse(s);
  } catch {
    return {};
  }
}
