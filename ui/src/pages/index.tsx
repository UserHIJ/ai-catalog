import { useEffect, useState } from "react";
import Link from "next/link";
import Layout from "@/components/Layout";
import { Card } from "@/components/Card";


type DatasetRow = {
  dataset_id: string;
  name: string;
  source: string;
  row_count: number;
  size_bytes: number;
  last_profiled_at?: string | null;
};

function prettyBytes(n: number) {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(n) / Math.log(1024));
  return `${(n / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

// color per source system
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

export default function Home() {
  const [data, setData] = useState<DatasetRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [q, setQ] = useState("");

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

  const rows = (data ?? []).filter((d) =>
    q.trim()
      ? `${d.dataset_id} ${d.name} ${d.source}`.toLowerCase().includes(q.trim().toLowerCase())
      : true
  );

  return (
    <Layout>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16, gap: 12 }}>
        <h2 style={{ fontSize: 22, fontWeight: 700, margin: 0 }}>Datasets</h2>
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
      </div>

      {error && (
        <div style={{ padding: 12, background: "#fee2e2", border: "1px solid #fecaca", borderRadius: 8, color: "#991b1b", marginBottom: 12 }}>
          {error}
        </div>
      )}
      {!data && !error && <div style={{ padding: 12, color: "#6b7280" }}>loading…</div>}
      {data && rows.length === 0 && <div style={{ padding: 12, color: "#6b7280" }}>no matches</div>}

      <div className="grid-datasets">
        {rows.map((d) => {
          const color = sourceColors[d.source] || "#94a3b8";
          const href = `/dataset/${encodeURIComponent(d.dataset_id)}`;
          return (
            <Card key={d.dataset_id} accent={color}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                <div style={{ minWidth: 0 }}>
                  <Link
                    href={href}
                    style={{ color: color, textDecoration: "none", fontWeight: 700, fontSize: 16 }}
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

              <div style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 8, flexWrap: "wrap" }}>
                {badge(d.source, color)}
                <span style={{ fontSize: 12, color: "#6b7280" }}>
                  {d.last_profiled_at ? "profiled" : "unprofiled"}
                </span>
              </div>
            </Card>
          );
        })}
      </div>
    </Layout>
  );
}
