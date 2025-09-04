import React from 'react';
import { LineageDAG } from '@/utils/lineageDAG';

interface LineageVisualizationProps {
  datasetId: string;
}

const TRANSFORM_COLORS: Record<string, { bg: string; border: string; text: string }> = {
  replication: { bg: "rgba(37,99,235,0.12)", border: "#2563eb", text: "#1e40af" },      // blue
  "raw data":  { bg: "rgba(8,145,178,0.12)", border: "#0891b2", text: "#0e7490" },     // cyan
  transform:   { bg: "rgba(245,158,11,0.12)", border: "#f59e0b", text: "#b45309" },     // amber
  enriched:    { bg: "rgba(22,163,74,0.12)",  border: "#16a34a", text: "#166534" },     // green
};
const styleForTransform = (name?: string | null) => {
  const k = (name || "").toLowerCase();
  return TRANSFORM_COLORS[k] || { bg: "rgba(107,114,128,0.12)", border: "#9ca3af", text: "#374151" }; // gray fallback
};


export const LineageVisualization: React.FC<LineageVisualizationProps> = ({ datasetId }) => {
  const { nodes, edges } = LineageDAG.createSalesforceLineage();

  return (
  <div style={{ padding: 8 }}>
    {/* ROW OF NODES */}
    <div
      style={{
        display: "flex",
        alignItems: "center",
        flexWrap: "wrap",
        gap: 8,
         marginBottom: 25,   
      }}
    >
      {nodes.map((node, index) => (
        <React.Fragment key={node.id}>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              padding: "4px 10px",
              border: "1px solid #e5e7eb",
              borderRadius: 8,
              background: "#fffbe6" /* subtle yellow so you can SEE it */,
        
            }}
          >
            <span
              style={{
                fontSize: 14,         // 👈 smaller icon
                lineHeight: 1,        // 👈 keeps icon vertically centered
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                width: 15,
                height: 15,
              }}
            >
              {node.icon}
            </span>
            <span style={{ fontSize: 13 }}>{node.name}</span>
          </div>

          {index < nodes.length - 1 && (
            <span
              style={{
                margin: "0 6px",
                alignSelf: "center",   // 👈 arrow centers between nodes
                fontSize: 14,
              }}
            >
              →
            </span>
          )}
        </React.Fragment>
      ))}
    </div>

    {/* EDGE LABELS */}
{edges.length > 0 && (
  <div style={{ marginTop: 6, display: "flex", gap: 6, flexWrap: "wrap" }}>
    {edges.map((edge, i) => (
      <span
        key={i}
        style={{
          fontSize: 12,
          color: "#6b7280",
          background: "#f3f4f6",
          border: "1px solid #e5e7eb",
          borderRadius: 6,
          padding: "12px 12px",
        }}
      >
        {edge.transform}
      </span>
    ))}
  </div>
)}
  </div>
);
};