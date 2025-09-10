// /pages/api/publish/[id].ts
import type { NextApiRequest, NextApiResponse } from "next";
import { publishEmbeddings } from "@/lib/publishEmbeddings";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  const id = Array.isArray(req.query.id) ? req.query.id[0] : req.query.id;
  if (!id) return res.status(400).json({ error: "Missing dataset id" });

  const limit = Number(req.body?.limit ?? 200);
  if (!Number.isFinite(limit) || limit <= 0) {
    return res.status(400).json({ error: "Invalid limit" });
  }

  try {
    const result = await publishEmbeddings(id, {
      limit,
      truncate: true, // your chosen semantics
      // includeColumns: ["acct_number","acct_type","status_code","ledger_balance","available_balance","currency_code","open_date","last_txn_ts"],
      // excludeColumns: ["email_primary","phone_home","phone_mobile","addr_line1","addr_line2"],
    });

    const status = result.inserted === 0 ? "no-new-rows" : "published";
    return res.status(200).json({
      status,
      targetTable: result.targetTable,
      totalRows: result.totalRows,
      inserted: result.inserted,
    });
  } catch (e: any) {
    console.error("publish error", e);
    return res.status(500).json({ error: e?.message || "publish failed" });
  }
}
