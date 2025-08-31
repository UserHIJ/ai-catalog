import React, { PropsWithChildren } from "react";

export default function Layout({ children }: PropsWithChildren<{}>) {
  return (
    <div style={{ maxWidth: 980, margin: "0 auto", padding: "24px" }}>
      <header
        style={{
          marginBottom: 24,
          display: "flex",
          alignItems: "baseline",
          gap: 12,
        }}
      >
        <h1 style={{ fontSize: 24, fontWeight: 700 }}>AI Catalog</h1>
        <span style={{ color: "#666" }}>prototype</span>
      </header>
      {children}
      <footer
        style={{
          marginTop: 40,
          color: "#888",
          fontSize: 12,
        }}
      >
        v0 — UI only; data is mocked. Hook up Iceberg later.
      </footer>
    </div>
  );
}
