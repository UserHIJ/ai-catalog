// File: ui/src/pages/api/datasets/[id].ts

import type { NextApiRequest, NextApiResponse } from "next";
import { q } from "@/lib/trino"; // <— swap to your Trino query helper

type MetaRow = {
  dataset_id: string;
  name: string | null;
  source: string | null;
  created_at: string | null;
  row_count: number | null;
  size_bytes: number | null;
  last_profiled_at: string | null;
};

type ColumnRow = {
  dataset_id: string;
  column_name: string;
  data_type: string | null;
  pii_flag: boolean | null;
  null_ratio: number | null;
  distinct_ratio: number | null;
  indexed: boolean | null; // your new column
};

type EdgeRow = {
  src_dataset_id: string;
  dst_dataset_id: string;
  transform_type: string | null;
  updated_at: string | null;
};

// ——— helpers ———

function escLit(s: string): string {
  // escape a string literal for SQL (' -> '')
  return s.replace(/'/g, "''");
}

function jsonSafe<T = unknown>(x: T): T {
  // ensure BigInt or weird values won't explode JSON.stringify downstream
  return JSON.parse(
    JSON.stringify(x, (_k, v) => (typeof v === "bigint" ? Number(v) : v))
  );
}

// ——— handler ———

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    const datasetIdRaw = (req.query.id ?? "").toString();
    if (!datasetIdRaw) {
      return res.status(400).json({ error: "missing dataset id" });
    }
    const datasetId = datasetIdRaw.trim();
    const did = escLit(datasetId);

    // 1) meta
    const metaSql = `
      SELECT dataset_id, name, source, created_at, row_count, size_bytes, last_profiled_at
      FROM iceberg.catalog.catalog_datasets
      WHERE dataset_id = '${did}'
      LIMIT 1
    `;
    const metaRows = (await q<MetaRow>(metaSql)) ?? [];
    const meta = metaRows[0] ?? null;

    if (!meta) {
      return res.status(404).json({ error: `dataset not found: ${datasetId}` });
    }

    // 2) columns (include your new "indexed" field)
    const colsSql = `
      SELECT dataset_id, column_name, data_type, pii_flag, null_ratio, distinct_ratio, indexed
      FROM iceberg.catalog.catalog_columns
      WHERE dataset_id = '${did}'
      ORDER BY column_name
    `;
    const columns = (await q<ColumnRow>(colsSql)) ?? [];

    // 3) lineage (edges touching this dataset)
    const linSql = `
      SELECT src_dataset_id, dst_dataset_id, transform_type, updated_at
      FROM iceberg.catalog.catalog_lineage_edges
      WHERE src_dataset_id = '${did}' OR dst_dataset_id = '${did}'
      ORDER BY updated_at DESC NULLS LAST
    `;
    const lineage = (await q<EdgeRow>(linSql)) ?? [];

    // Done
    res.status(200).json(
      jsonSafe({
        meta,
        columns,
        lineage,
      })
    );
  } catch (err: any) {
    const msg =
      err?.message ||
      (typeof err === "string" ? err : "failed to load dataset from trino");
    // Keep error blunt—helps you see Trino/SQL issues fast
    res.status(500).json({ error: msg });
  }
}
