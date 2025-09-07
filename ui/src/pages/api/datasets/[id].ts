// /ui/src/pages/api/datasets/[id].ts
import type { NextApiRequest, NextApiResponse } from "next";
import { q } from "@/lib/duckdb";

// BigInt-safe JSON stringify
function jsonSafe<T>(obj: T): T {
  return JSON.parse(
    JSON.stringify(obj, (_k, v) => (typeof v === "bigint" ? v.toString() : v))
  );
}

const WAREHOUSE = process.env.WAREHOUSE || ""; // e.g. s3://iceberg-warehouse

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  // Allow GET; block others
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json(jsonSafe({ error: "Method not allowed" }));
  }

  try {
    const raw = Array.isArray(req.query.id) ? req.query.id[0] : req.query.id;
    const datasetId = String(raw || "").trim();
    if (!datasetId) return res.status(400).json(jsonSafe({ error: "missing id" }));
    if (!WAREHOUSE) return res.status(500).json(jsonSafe({ error: "WAREHOUSE missing" }));

    const wh = WAREHOUSE.replace(/\/+$/, "");
    const escId = datasetId.replace(/'/g, "''");

    // --- META --------------------------------------------------------------
    const metaRows = await q(`
      SELECT dataset_id, name, source, row_count, size_bytes, last_profiled_at
      FROM iceberg_scan('${wh}/catalog/catalog_datasets')
      WHERE dataset_id='${escId}'
      LIMIT 1
    `);
    const meta = Array.isArray(metaRows) && metaRows.length ? metaRows[0] : null;

    if (!meta) {
      // If we truly found nothing, return 404 (not a hard 500)
      return res.status(404).json(jsonSafe({ error: `dataset not found: ${datasetId}` }));
    }

    // --- COLUMNS -----------------------------------------------------------
    // Your catalog_columns uses ds_* ids (you verified), so match on the ds id.
    const colsRows = await q(`
      SELECT dataset_id, column_name, data_type, pii_flag, null_ratio, distinct_ratio, indexed
      FROM iceberg_scan('s3://iceberg-warehouse/catalog/catalog_columns-f42a2d52bb264ff4a628a4a9e77a1e12/metadata/v00003.metadata.json')
      WHERE dataset_id='${escId}'
      ORDER BY dataset_id, column_name, data_type
    `);
    const columns = Array.isArray(colsRows) ? colsRows : [];
    //const lineage: any[] = [];

    // --- LINEAGE -----------------------------------------------------------


    let lineage: any[] = [];
      try {
        const linRows = await q(`
          SELECT src_dataset_id, dst_dataset_id, transform_type, updated_at
          FROM iceberg_scan('${wh}/catalog/catalog_lineage_edges')
          WHERE src_dataset_id='${escId}' OR dst_dataset_id='${escId}'
          ORDER BY updated_at DESC NULLS LAST
          LIMIT 200
        `);
        lineage = Array.isArray(linRows) ? linRows : [];
      } catch {
        lineage = [];
}
    // Done
    return res.status(200).json(jsonSafe({ meta, columns, lineage }));
  } catch (e: any) {
    console.error("[api/datasets/:id] error:", e?.message || e);
    return res.status(500).json(jsonSafe({ error: e?.message || "internal error" }));
  }
}
