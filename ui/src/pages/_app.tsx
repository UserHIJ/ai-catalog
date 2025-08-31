// src/pages/_app.tsx
import type { AppProps } from "next/app";
import "@/styles/globals.css";   // ✅ add this

export default function App({ Component, pageProps }: AppProps) {
  return (
    <div style={{ background: "#f8fafc", minHeight: "100vh" }}>
      <Component {...pageProps} />
    </div>
  );
}

