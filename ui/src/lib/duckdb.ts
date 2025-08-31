// src/lib/duckdb.ts
import duckdb from "duckdb";

let db: duckdb.Database | null = null;

function getConnection() {
  if (!db) db = new duckdb.Database(":memory:");
  return new duckdb.Connection(db);
}

export function q<T = any>(sql: string, params: any[] = []): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const c = getConnection();
    try {
      // 🔎 Log the exact SQL + params every time
      console.log("[DuckDB] SQL:", sql);
      if (params && params.length) {
        console.log("[DuckDB] Params:", params);
      }

      // one-time-ish setup; safe to rerun
      c.run("INSTALL httpfs; LOAD httpfs;");
      c.run("INSTALL iceberg; LOAD iceberg;");
      c.run(`SET s3_endpoint='${process.env.S3_ENDPOINT!.replace(/^https?:\/\//, "")}'`);
      c.run(`SET s3_use_ssl=${process.env.S3_USE_SSL === "true" ? "true" : "false"}`);
      c.run(`SET s3_url_style='${process.env.S3_URL_STYLE ?? "path"}'`);
      c.run(`SET s3_access_key_id='${process.env.S3_ACCESS_KEY}'`);
      c.run(`SET s3_secret_access_key='${process.env.S3_SECRET_KEY}'`);
      c.run("SET unsafe_enable_version_guessing = true;");

      const cb = (err: any, rows: unknown[]) => {
        c.close();
        if (err) reject(err);
        else resolve(rows as T[]);
      };

      if (params && params.length > 0) {
        (c as any).all(sql, params, cb);
      } else {
        (c as any).all(sql, cb);
      }
    } catch (e) {
      c.close();
      reject(e);
    }
  });
}
