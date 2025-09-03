/* /ui/src/pages/dataset/[id].tsx */
import { useEffect, useState } from "react";
import { useRouter } from "next/router";
import Layout from "@/components/Layout";
import { Card } from "@/components/Card";

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
};

// ---- SAFE RENDER HELPERS ----
function isObj(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === "object" && !Array.isArray(v);
}
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
  if (isObj(v) || Array.isArray(v)) return JSON.stringify(v);
  // @ts-ignore bigint fallback
  if (typeof v === "bigint") {
    const n = Number(v);
    return Number.isSafeInteger(n) ? String(n) : v.toString();
  }
  return String(v);
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
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // PUBLISH UI
  const [publishing, setPublishing] = useState(false);
  const [pubMsg, setPubMsg] = useState<string | null>(null);
  const [pubErr, setPubErr] = useState<string | null>(null);

  useEffect(() => {
    setMeta(null);
    setColumns([]);
    setErr(null);
  }, [id]);

  // Load meta + columns (lineage removed)
  useEffect(() => {
    if (!id) return;
    let alive = true;
    setLoading(true);
    fetch(`/api/datasets/${encodeURIComponent(id)}`)
      .then(async (r) => {
        if (!r.ok) {
          const t = await r.text().catch(() => "");
          throw new Error(t || r.statusText || `HTTP ${r.status}`);
        }
        return r.json();
      })
      .then((data) => {
        if (!alive) return;
        if (!data?.meta) throw new Error("dataset not found");
        setMeta(data.meta);
        setColumns(Array.isArray(data.columns) ? data.columns : []);
        // lineage intentionally removed
      })
      .catch((e: any) => {
        if (!alive) return;
        setErr(e?.message || "failed to load dataset");
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
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

  return (
    <Layout>
      {!id && <div style={{ padding: 12, color: "#6b7280" }}>Waiting for dataset id…</div>}

      {err && (
        <div
          style={{
            padding: 12,
            background: "#fee2e2",
            border: "1px solid #fecaca",
            borderRadius: 8,
            color: "#991b1b",
            marginBottom: 12,
          }}
        >
          {String(err)}
        </div>
      )}

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

          {/* Publish toolbar OUTSIDE the Lineage card */}
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              margin: "8px 0 16px 0",
              justifyContent: "flex-end",
            }}
          >
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
            {pubMsg && (
              <span style={{ color: "#374151", fontSize: 13 }}>{pubMsg}</span>
            )}
            {pubErr && (
              <span style={{ color: "#dc2626", fontSize: 13 }}>
                Error: {pubErr}
              </span>
            )}
          </div>

          {/* Two-column grid: Profile + Lineage (blank) */}
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
              {/* Lineage view intentionally blank for now */}
              <div style={{ minHeight: 80 }} />
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
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>column_name</th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>data_type</th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>pii_flag</th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>null_ratio</th>
                      <th style={{ padding: "6px 8px", borderBottom: "1px solid #e5e7eb" }}>distinct_ratio</th>
                    </tr>
                  </thead>
                  <tbody>
                    {columns.map((c, i) => (
                      <tr key={`${c.column_name}-${i}`}>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>{asText(c.column_name)}</td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>{asText(c.data_type)}</td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>{asText(c.pii_flag)}</td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>{asText(c.null_ratio)}</td>
                        <td style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>{asText(c.distinct_ratio)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>

          {/* Samples intentionally skipped for now */}
        </>
      )}
    </Layout>
  );
}
