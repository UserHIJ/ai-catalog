/* /lib/ai.ts */
import OpenAI from "openai";

const apiKey = process.env.OPENAI_API_KEY || "";
if (!apiKey) {
  console.warn("[ai] OPENAI_API_KEY is not set; LLM/embeddings will be disabled.");
}

export const openai = apiKey ? new OpenAI({ apiKey }) : null;

/** Create a 1536-dim embedding. Keep model consistent with your stored vectors. */
export async function embedText(text: string): Promise<number[] | null> {
  if (!openai) return null;
  const trimmed = text.length > 8192 ? text.slice(0, 8192) : text;
  const resp = await openai.embeddings.create({
    model: "text-embedding-3-small",
    input: trimmed,
  });
  // @ts-ignore types use 'data[0].embedding'
  return (resp.data?.[0]?.embedding as number[]) ?? null;
}

/** Format a JS vector into a Postgres 'vector' literal understood by pgvector. */
export function toSqlVector(vec: number[]): string {
  // pgvector accepts `[v1, v2, ...]` ::vector
  return `[${vec.map((v) => (Number.isFinite(v) ? v : 0)).join(",")}]`;
}
