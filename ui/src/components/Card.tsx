import React, { PropsWithChildren } from "react";

type CardProps = PropsWithChildren<{
  accent?: string;    // e.g., "#2563eb"
  onClick?: () => void;
}>;

export function Card({ children, accent = "#e5e7eb", onClick }: CardProps) {
  return (
    <div
      role={onClick ? "button" : undefined}
      onClick={onClick}
      style={{
        border: "1px solid #e5e7eb",
        borderRadius: 4, // Reduced from 14
        padding: 4, // Reduced from 14
        boxShadow: "0 1px 2px rgba(0,0,0,0.06)",
        cursor: onClick ? "pointer" : "default",
        background: "white",
        display: "flex",
        flexDirection: "column",
        gap: 2, // Reduced from 8
        transition: "transform .08s ease, box-shadow .12s ease",
        borderTop: `3px solid ${accent}`, // Reduced from 4px
      }}
      onMouseEnter={(e) => {
        (e.currentTarget as HTMLDivElement).style.boxShadow =
          "0 6px 18px rgba(0,0,0,0.08)";
        (e.currentTarget as HTMLDivElement).style.transform = "translateY(-2px)";
      }}
      onMouseLeave={(e) => {
        (e.currentTarget as HTMLDivElement).style.boxShadow =
          "0 1px 2px rgba(0,0,0,0.06)";
        (e.currentTarget as HTMLDivElement).style.transform = "none";
      }}
    >
      {children}
    </div>
  );
}