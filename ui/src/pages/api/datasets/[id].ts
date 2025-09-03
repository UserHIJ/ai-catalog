// /ui/src/pages/api/datasets/[id].ts
import type { NextApiRequest, NextApiResponse } from "next";
import { q } from "@/lib/duckdb";

const WAREHOUSE = process.env.WAREHOUSE || ""; // e.g., s3://iceberg-warehouse

function jsonSafe<T>(obj: T): T {
  return JSON.parse(
    JSON.stringify(obj, (_k, v) => (typeof v === "bigint" ? v.toString() : v))
  );
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  // Allow GET; block others.
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const raw = Array.isArray(req.query.id) ? req.query.id[0] : req.query.id;
    const datasetId = String(raw || "").trim();
    if (!datasetId) return res.status(400).json({ error: "missing id" });
    if (!WAREHOUSE) return res.status(500).json({ error: "WAREHOUSE missing" });

    const wh = WAREHOUSE.replace(/\/+$/, "");
    const escId = datasetId.replace(/'/g, "''");

    // --- META --------------------------------------------------------------
    // Read a single row from the catalog_datasets table for this dataset_id
    const metaRows = await q(`
      SELECT *
      FROM iceberg_scan('${wh}/catalog/catalog_datasets')
      WHERE dataset_id='${escId}'
      LIMIT 1
    `);
    if (!Array.isArray(metaRows) || metaRows.length === 0) {
      return res.status(404).json({ error: "dataset not found" });
    }
    const meta = metaRows[0];

    // --- COLUMNS -----------------------------------------------------------
    // Try columns; if the table isn't present, just return an empty array.
    let columns: any[] = [];
    try {
      // Use ordinal_position when available; otherwise order by column_name
      const cols = await q(`
        SELECT *
        FROM iceberg_scan('${wh}/catalog/catalog_columns')
        WHERE dataset_id='${escId}'
        ORDER BY
          CASE WHEN try_cast(NULLIF(ordinal_position, '') AS INTEGER) IS NOT NULL
               THEN try_cast(NULLIF(ordinal_position, '') AS INTEGER)
               ELSE 2147483647 END,
          column_name
      `);
      if (Array.isArray(cols)) columns = cols;
    } catch {
      columns = [];
    }

    // --- LINEAGE -----------------------------------------------------------
    // Try lineage; include both inbound and outbound edges.
    let lineage: any[] = [];
    try {
      const lin = await q(`
        SELECT *
        FROM iceberg_scan('${wh}/catalog/catalog_lineage')
        WHERE src_dataset_id='${escId}' OR dst_dataset_id='${escId}'
      `);
      if (Array.isArray(lin)) lineage = lin;
    } catch {
      lineage = [];
    }

    // Done.
    return res.status(200).json(jsonSafe({ meta, columns, lineage }));
  } catch (e: any) {
    console.error("[api/datasets/:id] error:", e?.message || e);
    return res.status(500).json({ error: e?.message || "internal error" });
  }
}
