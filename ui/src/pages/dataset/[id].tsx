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
type LineageEdge = {
  src_dataset_id: string;
  dst_dataset_id: string;
  transform_type?: unknown;
  updated_at?: unknown;
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
  // bigint cannot appear here in the UI if the API sanitized; just in case:
  // @ts-ignore
  if (typeof v === "bigint") {
    const n = Number(v);
    return Number.isSafeInteger(n) ? n : null;
  }
  return null;
}
function asText(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return String(v);
  // UI must never try to render a raw object; stringify compactly
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
  const [lineage, setLineage] = useState<LineageEdge[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // PUBLISH UI
  const [publishing, setPublishing] = useState(false);
  const [pubMsg, setPubMsg] = useState<string | null>(null);
  const [pubErr, setPubErr] = useState<string | null>(null);

  useEffect(() => {
    setMeta(null);
    setColumns([]);
    setLineage([]);
    setErr(null);
  }, [id]);

  // Load meta/columns/lineage
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
        setLineage(Array.isArray(data.lineage) ? data.lineage : []);
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
          <div style={{ marginBottom: 16 }}>
            <h2 style={{ fontSize: 22, fontWeight: 700, margin: 0 }}>
              {asText(meta.name)}{" "}
              <span style={{ color: "#666", fontSize: 14 }}>({asText(meta.dataset_id)})</span>
            </h2>
            <div style={{ color: "#666", fontSize: 13 }}>source: {asText(meta.source)}</div>
          </div>

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

              {/* Publish button */}
              <div style={{ marginBottom: 8 }}>
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
                  <span style={{ marginLeft: 8, color: "#374151", fontSize: 13 }}>
                    {pubMsg}
                  </span>
                )}
                {pubErr && (
                  <span style={{ marginLeft: 8, color: "#dc2626", fontSize: 13 }}>
                    Error: {pubErr}
                  </span>
                )}
              </div>

              <ul style={{ margin: 0, paddingLeft: 18 }}>
                {lineage.length ? (
                  lineage.map((e, i) => (
                    <li key={i} style={{ fontSize: 14 }}>
                      {asText(e.src_dataset_id)} → {asText(e.dst_dataset_id)}{" "}
                      <span style={{ color: "#666" }}>({asText(e.transform_type)})</span>
                    </li>
                  ))
                ) : (
                  <li style={{ color: "#666" }}>No lineage recorded</li>
                )}
              </ul>
            </Card>
          </div>

          <Card>
            <h3 style={{ fontWeight: 600, marginBottom: 8 }}>Columns</h3>
            <div style={{ overflowX: "auto" }}>
              {/* If you render a table, make sure to wrap each cell with asText(...) */}
              {/* Example: */}
              {/* <table> ... <td>{asText(col.data_type)}</td> ... </table> */}
            </div>
          </Card>

          {/* Samples intentionally skipped for now */}
        </>
      )}
    </Layout>
  );
}
