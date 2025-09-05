import React, { PropsWithChildren } from "react";

export default function Layout({ children }: PropsWithChildren<{}>) {
  return (
    <div style={{ maxWidth: 980, margin: "12 auto", padding: "12px 66px" }}>
      <header
        style={{
          marginBottom: 12,
          display: "flex",
          alignItems: "baseline",
          gap: 12,
        }}
      >
        <img 
          src="/Fivetran.png" // Update this path to your actual image
          alt="Fivetran Logo" 
          style={{
            height: 200, // Adjust size as needed
            width: "auto",
            //filter: "contrast(122) brightness(122) invert(0)" 
          }}
        />
        <span style={{ color: "#666" }}></span>
      </header>
      {children}
      <footer
        style={{
          marginTop: 40,
          color: "#888",
          fontSize: 12,
        }}
      >
        v1 - Fivetran Iceberg Data Catalog 
      </footer>
    </div>
  );
}
