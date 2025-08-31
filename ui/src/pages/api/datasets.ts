import type { NextApiRequest, NextApiResponse } from "next";
import { q } from "@/lib/duckdb";
import { jsonSafe } from "@/lib/jsonsafe";

const W = process.env.WAREHOUSE!;

export default async function handler(_: NextApiRequest, res: NextApiResponse) {
  try {
    const rows = await q(`
      SELECT *
      FROM iceberg_scan('${W}/catalog/catalog_datasets')
      ORDER BY COALESCE(last_profiled_at, TIMESTAMP '1970-01-01') DESC
      LIMIT 500
    `);
    res.status(200).json(jsonSafe(rows));
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
}
