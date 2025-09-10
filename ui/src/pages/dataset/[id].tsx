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
  data_type?: unknown;        // DB2 source type (what your API currently returns)
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

/**
 * Map DB2 source data types → Iceberg data types (using your Fivetran rules).
 * Returns "i don't know" when ambiguous.
 * NOTE: All outputs are intentionally lowercased.
 */
function icebergTypeFor(db2Type: unknown): string {
  const emit = (s: string) => s.toLowerCase();

  const raw = asText(db2Type);
  if (raw === "—") return emit("I don't know");
  const s = raw.toLowerCase().trim();

  // normalize
  const compact = s.replace(/\s+/g, " ");          // collapse spaces
  const base = s.replace(/\s*\(.+\)/, "");         // strip (...) e.g., decimal(18,2) -> decimal

  // Temporal
  if (compact.includes("timestamp with time zone")) return emit("TIMESTAMPTZ");
  if (compact.includes("timestamp")) return emit("TIMESTAMP");
  if (compact === "date" || compact === "ansidate") return emit("DATE");
  if (compact.startsWith("time2")) return emit("I don't know"); // connector-dependent

  // Integers
  if (base === "smallint") return emit("INTEGER");
  if (base === "integer") return emit("INTEGER");
  if (base === "bigint") return emit("LONG");

  // Exact numeric (Fivetran rules: DECIMAL(38,10) | DOUBLE | STRING for huge PKs)
  if (s.startsWith("decimal")) {
    // If no precision/scale at all -> DOUBLE
    if (!/\d/.test(s)) return emit("DOUBLE");
    // Otherwise default to DECIMAL(38,10). (We can’t know PK oversize here.)
    return emit("DECIMAL(38,10)");
  }

  // Floating / decfloat
  if (base === "decfloat") return emit("I don't know"); // often coerced to DOUBLE, but unspecified here
  if (base === "real") return emit("FLOAT");
  if (base === "double") return emit("DOUBLE");

  // Strings
  if (["char", "varchar", "clob", "dbclob", "graphic", "vargraphic", "xml"].includes(base))
    return emit("STRING");

  // Binary
  if (["binary", "varbinary", "blob"].includes(base)) return emit("BINARY");

  return emit("I don't know");
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

  const profiled = Boolean(meta?.last_profiled_at && asText(meta.last_profiled_at) !== "—");

  return (
    <Layout>
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

          {/* Toolbar: Publish */}
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "flex-end",
              gap: 12,
              margin: "8px 0",
            }}
          >
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
                <div>
                  rows: <strong>{prettyInt(meta.row_count)}</strong>
                </div>
                <div>size (bytes): {prettyBytes(meta.size_bytes)}</div>
                <div>last profiled: {asText(meta.last_profiled_at)}</div>
              </div>
            </Card>
            <Card>
              <h3 style={{ fontWeight: 600, marginBottom: 8 }}>Lineage</h3>
              <LineageVisualization datasetId={String(meta.dataset_id)} />
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
                        column name
                      </th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>
                        source data type
                      </th>
                      <th
                        style={{
                          padding: "6px 8px", paddingRight: "1in",    
                          borderBottom: "1px solid #e5e7eb",
                          width: 24,
                          textAlign: "center",
                        }}
                        //aria-label="mapping arrow"
                        title="source → Iceberg"
                      >
                        →
                      </th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>
                        Iceberg data type
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
                        <td style={{ padding: "6px 8px",  paddingRight: "1in", borderBottom: "1px solid #f3f4f6" }}>
                          {asText(c.column_name)}
                        </td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
                          {asText(c.data_type)}
                        </td>
                        <td
                          style={{
                            padding: "6px 8px",
                            borderBottom: "1px solid #f3f4f6",
                            textAlign: "center",
                            color: "#6b7280",
                          }}
                        >
                          →
                        </td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
                          {icebergTypeFor(c.data_type)}
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

          {/* Footer: source badge + profiled/unprofiled */}
          <div
            style={{
              marginTop: 14,
              paddingTop: 10,
              borderTop: "1px solid #e5e7eb",
              display: "flex",
              alignItems: "center",
              gap: 8,
              color: "#6b7280",
            }}
          >
            <span
              style={{
                display: "inline-block",
                fontSize: 12,
                padding: "2px 8px",
                borderRadius: 999,
                background: "#eef2ff",
                color: "#4f46e5",
                border: "1px solid #4f46e533",
              }}
            >
              {asText(meta.source)}
            </span>
            <span style={{ fontSize: 12 }}>{profiled ? "profiled" : "unprofiled"}</span>
          </div>
        </>
      )}
    </Layout>
  );
}
