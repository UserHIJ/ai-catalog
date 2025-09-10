// /ui/src/pages/index.tsx
import { useEffect, useState } from "react";
import Link from "next/link";
import Layout from "@/components/Layout";
import { Card } from "@/components/Card";



// 14 sample points (oldest → newest). Tweak as you like.
const SAMPLE_TREND = [8, 9, 10, 11, 12, 12, 13, 14, 15, 16, 17, 18, 19, 18, 19, 17, 19, 19, 18, 15, 12, 13, 14, 15];
const SPARK_ONLY_ID = "db2_zos_corebank_accounts_1";


type DatasetRow = {
  dataset_id: string;
  name: string;
  source: string;
  row_count: number;
  size_bytes: number;
  last_profiled_at?: string | null;
  // OPTIONAL: if your API sends it, we’ll draw it
  row_trend?: number[]; // e.g., last 14 points of ingest counts (oldest→newest)
};

function prettyBytes(n: number) {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(n) / Math.log(1024));
  return `${(n / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

const sourceColors: Record<string, string> = {
  "SAP ECC": "#0ea5e9",
  "Oracle EBS": "#f97316",
  "SQL Server": "#ef4444",
  "PostgreSQL": "#2563eb",
  "Anaplan": "#8b5cf6",
  "Shopify": "#16a34a",
  "Salesforce": "#38bdf8",
  "Workday HCM": "#0891b2",
  "NetSuite": "#64748b",
  "Zendesk": "#10b981",
  "Jira Cloud": "#3b82f6",
  "GitHub": "#111827",
  "ServiceNow": "#22c55e",
  "Google Analytics": "#f59e0b",
};

function badge(label: string, color?: string) {
  const bg = color ? `${color}22` : "#eef2ff";
  const fg = color || "#4f46e5";
  return (
    <span
      style={{
        display: "inline-block",
        fontSize: 12,
        padding: "2px 8px",
        borderRadius: 999,
        background: bg,
        color: fg,
        border: `1px solid ${fg}33`,
      }}
    >
      {label}
    </span>
  );
}

/** Tiny, dependency-free sparkline (pure SVG) */
function Sparkline({
  data,
  width = 120,
  height = 26,
  stroke = "#64748b",
  strokeWidth = 1.5,
  placeholder = true, // if no data, show a subtle flat line
  showLastLabel = true,                   // <— NEW
  formatLabel = (n: number) => `${n}`,    // <— NEW
}: {
  data?: number[] | null;
  width?: number;
  height?: number;
  stroke?: string;
  strokeWidth?: number;
  placeholder?: boolean;
}) {
  const w = Math.max(10, width);
  const h = Math.max(10, height);
  const pad = 2;

  const xs = Array.isArray(data) ? data.filter((v) => Number.isFinite(v)) : [];
  const hasData = xs.length >= 2;
  const min = hasData ? Math.min(...xs) : 0;
  const max = hasData ? Math.max(...xs) : 1;
  const span = max - min || 1;

  const points: [number, number][] = hasData
    ? xs.map((v, i) => {
        const x = pad + (i * (w - pad * 2)) / (xs.length - 1);
        const y = h - pad - ((v - min) / span) * (h - pad * 2);
        return [x, y];
      })
    : placeholder
    ? [
        [pad, h / 2],
        [w - pad, h / 2],
      ]
    : [];

  const path =
    points.length > 0
      ? "M " + points.map(([x, y]) => `${x.toFixed(2)} ${y.toFixed(2)}`).join(" L ")
      : "";

  const last = points[points.length - 1];

  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} role="img" aria-label="sparkline" style={{ width: "100%" }}   preserveAspectRatio="none" >
      <path d={path} fill="none" stroke={"blue"} strokeWidth={strokeWidth} />
      {hasData && last && <circle cx={last[0]} cy={last[1]} r={2} fill={stroke} />}
    </svg>
  );
}

export default function Home() {
  const [data, setData] = useState<DatasetRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [selectedFilter, setSelectedFilter] = useState("");

  useEffect(() => {
    let alive = true;
    fetch("/api/datasets")
      .then((r) => (r.ok ? r.json() : r.text().then((t) => Promise.reject(t))))
      .then((rows) => alive && setData(rows))
      .catch((e) => alive && setError(typeof e === "string" ? e : e?.message || "error"));
    return () => {
      alive = false;
    };
  }, []);

  const rows = (data ?? []).filter((d) => {
    const matchesSearch = q.trim()
      ? `${d.dataset_id} ${d.name} ${d.source}`.toLowerCase().includes(q.trim().toLowerCase())
      : true;
    const matchesFilter = selectedFilter ? d.source === selectedFilter : true;
    return matchesSearch && matchesFilter;
  });

  return (
    <Layout>
      <div style={{ display: "flex", justifyContent: "flex-start", marginBottom: 16, gap: 12 }}>
        <h2 style={{ fontSize: 22, fontWeight: 700, margin: 0 }}></h2>

        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
          <input
            placeholder="Search name/id/source"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            style={{
              padding: 8,
              border: "1px solid #ddd",
              borderRadius: 8,
              minWidth: 260,
              outline: "none",
            }}
          />

          <select
            value={selectedFilter}
            onChange={(e) => setSelectedFilter(e.target.value)}
            style={{
              padding: 8,
              border: "1px solid #ddd",
              borderRadius: 8,
              outline: "none",
              minWidth: 140,
              backgroundColor: "white",
            }}
          >
            <option value="">All Sources</option>
            {Object.keys(sourceColors).map((source) => (
              <option key={source} value={source}>
                {source}
              </option>
            ))}
          </select>
        </div>
      </div>

      {error && (
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
          {error}
        </div>
      )}
      {!data && !error && <div style={{ padding: 12, color: "#6b7280" }}>loading…</div>}
      {data && rows.length === 0 && <div style={{ padding: 12, color: "#6b7280" }}>no matches</div>}

      <div
        className="grid-datasets px-6"
        style={{ display: "grid", gridTemplateColumns: "repeat(8, minmax(220px, 1fr))", gap: 12 }}
      >
        {rows.map((d) => {
          const color = sourceColors[d.source] || "#94a3b8";
          const href = `/dataset/${encodeURIComponent(d.dataset_id)}`;
          return (
            <Card key={d.dataset_id} accent={color}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                <div style={{ minWidth: 0 }}>
                  <Link
                    href={href}
                    style={{ color: color, textDecoration: "none", fontWeight: 700, fontSize: 24 }}
                  >
                    {d.name}
                  </Link>
                  <div
                    style={{
                      color: "#64748b",
                      fontSize: 12,
                      marginTop: 4,
                      whiteSpace: "nowrap",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                    }}
                  >
                    {d.dataset_id}
                  </div>
                </div>
                <div style={{ textAlign: "right", color: "#111827", fontSize: 13 }}>
                  <div>
                    <strong>{(d.row_count ?? 0).toLocaleString()}</strong>
                  </div>
                  <div style={{ color: "#6b7280" }}>{prettyBytes(d.size_bytes ?? 0)}</div>
                </div>
              </div>

              {/* Source + profiled line */}
              <div
                style={{
                  display: "flex",
                  gap: 8,
                  alignItems: "center",
                  marginTop: 8,
                  flexWrap: "wrap",
                }}
              >
                {badge(d.source, color)}
                <span style={{ fontSize: 12, color: "#6b7280" }}>
                  {d.last_profiled_at ? "profiled" : "unprofiled"}
                </span>
              </div>

              {/* Sparkline sits BELOW the source line */}
            {d.dataset_id === SPARK_ONLY_ID && (
              <div style={{ marginTop: 6 }}>
                <Sparkline
                data={Array.isArray(d.row_trend) && d.row_trend.length >= 2 ? d.row_trend : SAMPLE_TREND}
              />
              </div>
              )}
        
            </Card>
          );
        })}
      </div>
    </Layout>
  );
}
