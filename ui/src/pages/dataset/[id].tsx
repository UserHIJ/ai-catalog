/* /ui/src/pages/dataset/[id].tsx */
import { useEffect, useState } from "react";
import { useRouter } from "next/router";
import Layout from "@/components/Layout";
import { Card } from "@/components/Card";
import { LineageVisualization } from "@/components/LineageVisualization";

type Meta = {
  dataset_id: string;
  name: string;
  source?: unknown;
  row_count?: unknown;
  size_bytes?: unknown;
  last_profiled_at?: unknown;
};

type Column = {
  dataset_id: string;
  column_name: string;
  data_type?: unknown;
  pii_flag?: unknown;
  null_ratio?: unknown;
  distinct_ratio?: unknown;
  indexed: boolean;
};

type Edge = {
  src_dataset_id: string;
  dst_dataset_id: string;
  transform_type?: string | null;
  updated_at?: unknown;
};

/** Types shaped to what /api/ask returns (answer optional) */
type AskHit = { datasetId: string; pk: string; preview: string; distance: number };
type AskResults = { exact: AskHit[]; semantic: AskHit[] };
type AskResponse = {
  ok: boolean;
  datasetId: string | null;
  question: string;
  results: AskResults;
  answer?: string;
};

function asNumber(v: unknown): number | null {
  if (typeof v === "number") return v;
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  // @ts-ignore bigint fallback
  if (typeof v === "bigint") {
    const n = Number(v);
    return Number.isSafeInteger(n) ? n : null;
  }
  return null;
}
function asText(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return String(v);
  // @ts-ignore bigint fallback
  if (typeof v === "bigint") return v.toString();
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}
function prettyInt(v: unknown): string {
  const n = asNumber(v);
  return n == null ? asText(v) : n.toLocaleString();
}
function prettyBytes(v: unknown): string {
  const n = asNumber(v);
  if (n == null) return asText(v);
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  return `${(n / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

export default function DatasetDetail(): JSX.Element {
  const { query } = useRouter();
  const id =
    typeof query.id === "string" ? query.id : Array.isArray(query.id) ? query.id[0] : "";

  const [meta, setMeta] = useState<Meta | null>(null);
  const [columns, setColumns] = useState<Column[]>([]);
  const [lineage, setLineage] = useState<Edge[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // Publish state
  const [publishing, setPublishing] = useState(false);
  const [pubMsg, setPubMsg] = useState<string | null>(null);
  const [pubErr, setPubErr] = useState<string | null>(null);

  // Ask state (with checkbox to toggle LLM)
  const [asking, setAsking] = useState(false);
  const [askQuestion, setAskQuestion] = useState<string>("");
  const [useLlm, setUseLlm] = useState<boolean>(true);
  const [answer, setAnswer] = useState<string | null>(null);
  const [askErr, setAskErr] = useState<string | null>(null);
  const [hits, setHits] = useState<AskResults | null>(null);

  useEffect(() => {
    setMeta(null);
    setColumns([]);
    setLineage([]);
    setErr(null);
  }, [id]);

  useEffect(() => {
    if (!id) return;
    let alive = true;
    setLoading(true);
    fetch(`/api/datasets/${encodeURIComponent(id)}`)
      .then(async (r) => {
        if (!r.ok) throw new Error(await r.text());
        return r.json();
      })
      .then((data) => {
        if (!alive) return;
        setMeta(data.meta as Meta);
        setColumns((data.columns ?? []) as Column[]);
        setLineage((data.lineage ?? []) as Edge[]);
      })
      .catch((e: any) => {
        if (!alive) return;
        setErr(e?.message || "failed to load dataset");
      })
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [id]);

  async function onPublish(limit = 200) {
    if (!id) return;
    setPublishing(true);
    setPubMsg(null);
    setPubErr(null);
    try {
      const r = await fetch(`/api/publish/${encodeURIComponent(id)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ limit }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.error || r.statusText);
      setPubMsg(
        j?.status === "published"
          ? `Published ${j.inserted}/${j.totalRows} rows`
          : j?.status === "no-new-rows"
          ? "No new rows (already up to date)"
          : "Dataset path not found"
      );
    } catch (e: any) {
      setPubErr(e?.message || "publish failed");
    } finally {
      setPublishing(false);
    }
  }

  async function onAsk() {
    if (!meta?.dataset_id) {
      setAskErr("No dataset id found");
      return;
    }
    setAsking(true);
    setAnswer(null);
    setAskErr(null);
    setHits(null);
    try {
      const body: any = {
        datasetId: String(meta.dataset_id),
        question: askQuestion,
        useLlm, // pass checkbox value
        k: 5,
      };
      const r = await fetch(`/api/ask`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const j: AskResponse = await r.json();
      if (!r.ok || j?.ok === false) {
        throw new Error((j as any)?.error || r.statusText || "Ask failed");
      }
      setAnswer(j.answer ?? null);
      setHits(j.results ?? null);
    } catch (e: any) {
      setAskErr(e?.message || "ask failed");
    } finally {
      setAsking(false);
    }
  }

  function renderAnswerArea() {
    const hasAnswer = !!answer && answer.trim().length > 0;
    const exact = hits?.exact ?? [];
    const semantic = hits?.semantic ?? [];
    const fallback = [...exact, ...semantic];
    const total = fallback.length;

    return (
      <div style={{ marginBottom: 16 }}>
        <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>
          Answer
        </label>
        <div
          style={{
            border: "1px solid #e5e7eb",
            borderRadius: 6,
            padding: "8px 10px",
            background: "#f9fafb",
          }}
        >
          {askErr ? (
            <div style={{ color: "#dc2626", fontSize: 14 }}>Error: {askErr}</div>
          ) : hasAnswer ? (
            <div style={{ fontSize: 14, color: "#111827", whiteSpace: "pre-wrap" }}>{answer}</div>
          ) : total > 0 ? (
            <div style={{ display: "grid", gap: 6 }}>
              <div style={{ fontSize: 13, color: "#6b7280" }}>
                <strong>Top matches:</strong>
              </div>
              <ol style={{ margin: 0, paddingLeft: 18 }}>
                {fallback.map((h, idx) => (
                  <li key={`${h.pk}-${idx}`} style={{ fontSize: 14, color: "#374151" }}>
                    <span style={{ color: "#6b7280" }}>[{h.pk}]</span>{" "}
                    {typeof h.preview === "string"
                      ? h.preview.length > 180
                        ? h.preview.slice(0, 180) + "…"
                        : h.preview
                      : JSON.stringify(h.preview)}
                    {typeof (h as any).distance === "number" && (
                      <span style={{ color: "#9ca3af" }}> • d={(h as any).distance.toFixed(4)}</span>
                    )}
                  </li>
                ))}
              </ol>
            </div>
          ) : (
            <div style={{ fontSize: 14, color: "#6b7280" }}>No results yet.</div>
          )}
        </div>
      </div>
    );
  }

  return (
    <Layout>
      {!id && <div style={{ padding: 12, color: "#6b7280" }}>Waiting for dataset id…</div>}
      {err && <div style={{ padding: 12, background: "#fee2e2" }}>{err}</div>}
      {!err && loading && <div style={{ padding: 12, color: "#6b7280" }}>loading…</div>}

      {!err && !loading && meta && (
        <>
          {/* Header */}
          <div style={{ marginBottom: 8 }}>
            <h2 style={{ fontSize: 22, fontWeight: 700, margin: 0 }}>
              {asText(meta.name)}{" "}
              <span style={{ color: "#666", fontSize: 14 }}>({asText(meta.dataset_id)})</span>
            </h2>
            <div style={{ color: "#666", fontSize: 13 }}>source: {asText(meta.source)}</div>
          </div>

          {/* Unified toolbar: Ask (left) + Publish (right) + LLM toggle */}
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 12,
              margin: "8px 0",
            }}
          >
            {/* Ask controls (left) */}
            <div style={{ flex: 1, display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
              <button
                onClick={onAsk}
                disabled={asking}
                style={{
                  background: "#16a34a",
                  color: "white",
                  padding: "6px 12px",
                  borderRadius: 6,
                  fontSize: 14,
                  fontWeight: 600,
                  cursor: asking ? "not-allowed" : "pointer",
                  whiteSpace: "nowrap",
                }}
              >
                {asking ? "Asking…" : "Ask"}
              </button>

              <input
                type="text"
                placeholder="Type a question…"
                value={askQuestion}
                onChange={(e) => setAskQuestion(e.target.value)}
                style={{
                  flex: 1,
                  minWidth: 160,
                  maxWidth: "60%",
                  padding: "6px 10px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                  fontSize: 14,
                  outline: "none",
                }}
                disabled={asking}
              />

              {/* LLM toggle */}
              <label style={{ display: "flex", alignItems: "center", gap: 6, userSelect: "none" }}>
                <input
                  type="checkbox"
                  checked={useLlm}
                  onChange={(e) => setUseLlm(e.target.checked)}
                  disabled={asking}
                />
                <span style={{ fontSize: 13, color: "#111827" }}>Use AI Answer</span>
              </label>
            </div>

            {/* Publish (right) */}
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <button
                onClick={() => onPublish(200)}
                disabled={publishing}
                style={{
                  background: "#dc2626",
                  color: "white",
                  padding: "6px 12px",
                  borderRadius: 6,
                  fontSize: 14,
                  fontWeight: 600,
                  cursor: publishing ? "not-allowed" : "pointer",
                }}
              >
                {publishing ? "Publishing…" : "Publish"}
              </button>
              {(pubMsg || pubErr) && (
                <span style={{ fontSize: 13, color: pubErr ? "#dc2626" : "#374151" }}>
                  {pubErr ?? pubMsg}
                </span>
              )}
            </div>
          </div>

          {/* Answer area (now always visible after the toolbar) */}
          {renderAnswerArea()}

          {/* Two-column grid: Profile + Lineage */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: 12,
              marginBottom: 12,
            }}
          >
            <Card>
              <h3 style={{ fontWeight: 600, marginBottom: 8 }}>Profile</h3>
              <div style={{ fontSize: 14, lineHeight: 1.8 }}>
                <div>rows: <strong>{prettyInt(meta.row_count)}</strong></div>
                <div>size (bytes): {prettyBytes(meta.size_bytes)}</div>
                <div>last profiled: {asText(meta.last_profiled_at)}</div>
              </div>
            </Card>
            <Card>
              <h3 style={{ fontWeight: 600, marginBottom: 8 }}>Lineage</h3>
              <LineageVisualization datasetId={id} edges={lineage} />
            </Card>
          </div>

          {/* Columns */}
          <Card>
            <h3 style={{ fontWeight: 600, marginBottom: 8 }}>Columns</h3>
            {columns.length === 0 ? (
              <div style={{ color: "#6b7280" }}>No columns</div>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", fontSize: 14, borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ color: "#6b7280", textAlign: "left" }}>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>
                        column_name
                      </th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>
                        data_type
                      </th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>
                        pii_flag
                      </th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>
                        null_ratio
                      </th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>
                        distinct_ratio
                      </th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>
                        indexed
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {columns.map((c, i) => (
                      <tr key={`${c.column_name}-${i}`}>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
                          {asText(c.column_name)}
                        </td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
                          {asText(c.data_type)}
                        </td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
                          {asText(c.pii_flag)}
                        </td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
                          {asText(c.null_ratio)}
                        </td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
                          {asText(c.distinct_ratio)}
                        </td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
                          {asText(c.indexed)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        </>
      )}
    </Layout>
  );
}
