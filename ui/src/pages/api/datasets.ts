// File: src/pages/api/datasets.ts
// Minimal: list datasets via Trino and return a plain array (no wrapper)

import type { NextApiRequest, NextApiResponse } from "next";
import { q } from "@/lib/trino";

type DatasetRow = {
  dataset_id: string;
  name: string | null;
  source: string | null;
  created_at: string | null;
  row_count: number | null;
  size_bytes: number | null;
  last_profiled_at: string | null;
};

export default async function handler(_req: NextApiRequest, res: NextApiResponse) {
  try {
    const sql = `
      SELECT dataset_id, name, source, created_at, row_count, size_bytes, last_profiled_at
      FROM iceberg.catalog.catalog_datasets
      ORDER BY COALESCE(name, dataset_id)
    `;
    const rows = await q<DatasetRow>(sql);
    // return a plain array to match existing UI expectations
    res.status(200).json(rows);
  } catch (err: any) {
    res.status(500).json({ error: err?.message || "failed to list datasets from trino" });
  }
}
