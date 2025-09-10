// /ui/src/components/Layout.tsx
import React, { PropsWithChildren } from "react";
import { Inter } from "next/font/google";

const inter = Inter({
  subsets: ["latin"],
  display: "swap",
  weight: ["400", "600", "700"],
});

export default function Layout({ children }: PropsWithChildren<{}>) {
  return (
    <div
      className={inter.className}
      style={{
        width: "100%",
        margin: 0,                      // no centering; left-justified
        padding: "12px 1.25in",        // ~“an inch or two” on both sides
        boxSizing: "border-box",       // include padding in width
        textAlign: "left",
      }}
    >
      <header
        style={{
          marginBottom: 12,
          display: "flex",
          alignItems: "baseline",
          gap: 12,
        }}
      >
        <img
          src="/Fivetran.png"
          alt="Fivetran Logo"
          style={{
            height: 200,
            width: "auto",
            // filter: "contrast(122) brightness(122) invert(0)"
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
