// pages/api/ask.ts
import type { NextApiRequest, NextApiResponse } from "next";
import { Pool } from "pg";
import OpenAI from "openai";

// Environment variables
const PG_URL = process.env.PG_URL;
if (!PG_URL) {
  console.error("Missing PG_URL environment variable");
}

const DEMO_DATASET_ID = process.env.DEMO_DATASET_ID || "ds_salesforce_accounts";
const EMBEDDING_MODEL = process.env.EMBEDDING_MODEL || "text-embedding-3-small";
const LLM_MODEL = process.env.LLM_MODEL || "gpt-4o-mini";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

if (!OPENAI_API_KEY) {
  console.error("Missing OPENAI_API_KEY environment variable");
}

// Initialize clients
const pool = PG_URL ? new Pool({
  connectionString: PG_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
}) : null;

const openai = OPENAI_API_KEY ? new OpenAI({
  apiKey: OPENAI_API_KEY,
}) : null;

// Type definitions
type AskRequest = {
  question: string;
  datasetId?: string | null;
  k?: number;
  useLlm?: boolean;
  metric?: "cosine" | "l2" | "inner";
};

type Neighbor = {
  dataset_id: string;
  pk: string;
  text_chunk: string;
  distance: number;
  similarity: number;
  similarity01: number;
};

type AskResponse = {
  question: string;
  datasetId: string | null;
  k: number;
  results: Neighbor[];
  latency_ms: number;
  embedding_model: string;
  dbg: {
    db: string;
    host: string;
    port: number;
    schema: string;
    metric: string;
    routed_dataset: string | null;
    routed_rows: number;
    used_dataset: string | null;
    used_scope: "dataset" | "all";
    fallback_used: boolean;
    total_rows_all: number;
    search_query?: string;
    query_variations?: string[];
    search_method?: string;
  };
  answer?: string;
  citations?: Array<{
    dataset_id: string;
    pk: string;
    preview: string;
    distance: number;
  }>;
  llm_model?: string;
};

// Utility functions
function toVectorLiteral(vec: number[]): string {
  return `[${vec.join(",")}]`;
}

function distanceToSim01(distance: number): number {
  const d = Math.max(0, Math.min(2, distance));
  return 1 - d / 2;
}

function sanitizeInput(input: string): string {
  return input.trim().slice(0, 1000);
}

function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

function truncateContext(context: string, maxTokens: number = 8000): string {
  const estimatedTokens = estimateTokens(context);
  if (estimatedTokens <= maxTokens) {
    return context;
  }
  const charsToKeep = Math.floor((maxTokens / estimatedTokens) * context.length);
  return context.slice(0, charsToKeep) + "... [truncated]";
}

// Generate search variations for a term
function generateSearchVariations(term: string): string[] {
  const variations: string[] = [term];
  
  // Add common variations for hyphenated terms
  if (term.includes('-')) {
    variations.push(term.replace(/-/g, '_'));
    variations.push(term.replace(/-/g, ' '));
    variations.push(term.replace(/-/g, ''));
  }
  
  // Add partial matches for longer terms
  if (term.length > 10) {
    const parts = term.split('-');
    if (parts.length > 1) {
      variations.push(parts[0]); // just "purple"
      variations.push(parts[1]); // just "elephant"
      if (parts.length > 2) {
        variations.push(parts[2]); // just "42"
      }
    }
  }
  
  return [...new Set(variations)]; // Remove duplicates
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<AskResponse | { error: string; latency_ms: number }>
) {
  const t0 = Date.now();
  
  if (!pool || !openai) {
    const latency_ms = Date.now() - t0;
    return res.status(500).json({ 
      error: "Server configuration error", 
      latency_ms 
    });
  }

  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Use POST", latency_ms: 0 });
    }

    const { question, datasetId, k, useLlm }: AskRequest = req.body || {};
    
    if (!question || typeof question !== "string") {
      return res.status(400).json({ error: "Missing 'question'", latency_ms: 0 });
    }
    
    const sanitizedQuestion = sanitizeInput(question);
    const topK = Math.max(1, Math.min(k || 5, 50));
    const clientDatasetId = datasetId ? String(datasetId) : null;
    const wantsAll = !clientDatasetId || clientDatasetId.toUpperCase() === "ALL" || clientDatasetId === "*";
    const routedDatasetId = wantsAll ? DEMO_DATASET_ID : clientDatasetId;

    const client = await pool.connect();
    
    try {
      // Get database statistics
      const totalRowsAllResult = await client.query<{ n: string }>(
        "SELECT COUNT(*)::bigint AS n FROM embeddings"
      );
      const totalRowsAll = totalRowsAllResult.rows[0]?.n ?? "0";

      const routedRowsResult = await client.query<{ n: string }>(
        "SELECT COUNT(*)::bigint AS n FROM embeddings WHERE dataset_id = $1",
        [routedDatasetId]
      );
      const routedRows = routedRowsResult.rows[0]?.n ?? "0";

      // STEP 1: Generate search variations
      let searchQuery = sanitizedQuestion;
      let queryVariations = generateSearchVariations(sanitizedQuestion);
      let searchMethod = "exact";
      
      // STEP 2: First try exact matching for all variations
      let allResults: any[] = [];
      
      for (const variation of queryVariations) {
        if (variation.length <= 100 && variation.length >= 2) {
          try {
            const exactMatchSql = `
              SELECT dataset_id, pk, text_chunk, 0 as distance
              FROM embeddings 
              WHERE ($1::text IS NULL OR dataset_id = $1)
              AND text_chunk ILIKE '%' || $2 || '%'
              LIMIT $3
            `;
            const exactMatchResult = await client.query(exactMatchSql, [
              routedDatasetId, 
              variation, 
              topK
            ]);
            allResults.push(...exactMatchResult.rows);
          } catch (error) {
            console.error("Exact match error:", error);
          }
        }
      }

      // Remove duplicates
      const uniqueExactResults = allResults.filter((result, index, self) =>
        index === self.findIndex(r => r.dataset_id === result.dataset_id && r.pk === result.pk)
      ).slice(0, topK);

      let results: Neighbor[] = [];
      
      // If we found exact matches, use them
      if (uniqueExactResults.length > 0) {
        results = uniqueExactResults.map((r: any) => ({
          dataset_id: r.dataset_id,
          pk: String(r.pk),
          text_chunk: r.text_chunk,
          distance: 0,
          similarity: 1,
          similarity01: 1,
        }));
      } else {
        // STEP 3: Fall back to semantic search
        searchMethod = "semantic";
        try {
          const emb = await openai.embeddings.create({
            model: EMBEDDING_MODEL,
            input: searchQuery,
          });
          const vec: number[] = emb.data[0].embedding;
          const vecLiteral = toVectorLiteral(vec);

          const semanticSql = `
            SELECT dataset_id, pk, text_chunk, (embedding <=> $1::vector) AS distance
            FROM embeddings
            WHERE ($2::text IS NULL OR dataset_id = $2)
            ORDER BY embedding <=> $1::vector
            LIMIT $3
          `;
          
          const semanticResult = await client.query(semanticSql, [
            vecLiteral, 
            routedDatasetId, 
            topK
          ]);
          
          results = semanticResult.rows.map((r: any) => ({
            dataset_id: r.dataset_id,
            pk: String(r.pk),
            text_chunk: r.text_chunk,
            distance: Number(r.distance),
            similarity: -Number(r.distance),
            similarity01: distanceToSim01(Number(r.distance)),
          }));
        } catch (error) {
          console.error("Semantic search error:", error);
        }
      }

      // STEP 4: Generate LLM response if requested
      let answer: string | undefined;
      let citations: AskResponse["citations"] = undefined;
      let llm_model: string | undefined;

      if (useLlm) {
        llm_model = LLM_MODEL;

        if (results.length > 0) {
          let contextSnippet = results
            .map((r, i) => `#${i + 1} (${r.dataset_id}#${r.pk})\n${r.text_chunk}`)
            .join("\n\n");
          
          contextSnippet = truncateContext(contextSnippet);

          try {
            const completion = await openai.chat.completions.create({
              model: llm_model,
              messages: [
                {
                  role: "system",
                  content: `Answer the user's question using the provided context. Be helpful and analytical. If the context contains the information, provide a detailed answer. If not, say you couldn't find specific information.`
                },
                {
                  role: "user",
                  content: `Question: ${sanitizedQuestion}\n\nContext:\n${contextSnippet}`
                }
              ],
              temperature: 0.2,
              max_tokens: 500,
            });

            answer = completion.choices[0]?.message?.content?.trim();
          } catch (llmError) {
            console.error("LLM error:", llmError);
            answer = "I encountered an error while generating the response.";
          }
        } else {
          answer = "I couldn't find any relevant information about this in the available data.";
        }

        citations = results.map(r => ({
          dataset_id: r.dataset_id,
          pk: r.pk,
          preview: r.text_chunk.slice(0, 160) + (r.text_chunk.length > 160 ? "..." : ""),
          distance: r.distance,
        }));
      }

      const latency_ms = Date.now() - t0;
      
      const responseData: AskResponse = {
        question: sanitizedQuestion,
        datasetId: clientDatasetId,
        k: topK,
        results,
        latency_ms,
        embedding_model: searchMethod === "semantic" ? EMBEDDING_MODEL : "exact-match",
        dbg: {
          db: "postgres",
          host: (client as any).connectionParameters?.host || "unknown",
          port: Number((client as any).connectionParameters?.port || 5432),
          schema: "public",
          metric: "cosine",
          routed_dataset: routedDatasetId,
          routed_rows: Number(routedRows),
          used_dataset: routedDatasetId,
          used_scope: "dataset",
          fallback_used: false,
          total_rows_all: Number(totalRowsAll),
          search_query: searchQuery,
          query_variations: queryVariations,
          search_method: searchMethod
        }
      };

      if (answer) responseData.answer = answer;
      if (citations) responseData.citations = citations;
      if (llm_model) responseData.llm_model = llm_model;

      return res.status(200).json(responseData);

    } finally {
      client.release();
    }
  } catch (err: any) {
    const latency_ms = Date.now() - t0;
    console.error("API Error:", err);
    return res.status(500).json({ 
      error: err.message || "Internal server error", 
      latency_ms 
    });
  }
}