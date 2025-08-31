// src/pages/api/samples/[id].ts
import type { NextApiRequest, NextApiResponse } from "next";
import { q } from "@/lib/duckdb";
import { jsonSafe } from "@/lib/jsonsafe";

const W = process.env.WAREHOUSE!;
const esc = (s: string) => s.replace(/'/g, "''");

// Try a quick scan to see if an Iceberg table exists at `root`
async function probe(root: string): Promise<boolean> {
  try {
    // cheap probe: limit 1 — if it errors, the path is wrong (or not Iceberg)
    await q(`SELECT * FROM iceberg_scan('${root}') LIMIT 1`);
    return true;
  } catch {
    return false;
  }
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const debug = req.query.debug === "1";
  const raw = req.query.id;
  const id = Array.isArray(raw) ? raw[0] : raw;

  if (!id || typeof id !== "string" || id === "undefined" || id.trim() === "") {
    return res.status(400).json({ error: "missing dataset id" });
  }

  // 1) Lookup dataset by id (grab name and table_path if your catalog has it)
  const sqlLookup = `SELECT * FROM iceberg_scan('${W}/catalog/catalog_datasets') WHERE dataset_id = '${esc(id)}'`;
  try {
    const [ds] = await q(sqlLookup);
    if (!ds) {
      const payload = { error: "not found" };
      return debug ? res.status(404).json({ _debug: { sqlLookup }, ...payload }) : res.status(404).json(payload);
    }

    // Accept either a stored table_path (recommended), or fallback to heuristics
    // @ts-ignore — catalog may or may not have this column yet
    const explicitPath = (ds.table_path as string | undefined) || (ds.data_path as string | undefined);

    // 2) Build candidates to try, in order
    const candidates: string[] = [];
    if (explicitPath) {
      candidates.push(explicitPath);
    }
    // Default “datasets/NAME”
    candidates.push(`${W}/datasets/${esc(ds.name)}`);
    // Common alt: “demo/NAME” (how you created your smoke table)
    candidates.push(`${W}/demo/${esc(ds.name)}`);
    // Last-ditch: direct at warehouse root
    candidates.push(`${W}/${esc(ds.name)}`);

    const tried: string[] = [];
    let chosen: string | null = null;

    // 3) Probe each candidate until one works
    for (const cand of candidates) {
      tried.push(cand);
      // Note: esc() already handled quotes; cand is not re-escaped here
      if (await probe(cand)) {
        chosen = cand;
        break;
      }
    }

    if (!chosen) {
      const payload = {
        error: "table not found at any candidate root",
        tried
      };
      return debug
        ? res.status(404).json({ _debug: { sqlLookup }, ...payload })
        : res.status(404).json(payload);
    }

    // 4) Fetch real sample rows now that we know the right root
    const sqlSample = `SELECT * FROM iceberg_scan('${chosen}') LIMIT 50`;
    const rows = await q(sqlSample);
    const data = jsonSafe(rows);

    if (debug) {
      return res.status(200).json({ _debug: { sqlLookup, sqlSample, chosen, tried }, data });
    }
    res.status(200).json(data);
  } catch (e: any) {
    if (debug) return res.status(500).json({ _debug: { sqlLookup }, error: e.message });
    res.status(500).json({ error: e.message });
  }
}
