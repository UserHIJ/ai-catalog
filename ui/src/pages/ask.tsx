// /ui/src/pages/ask.tsx
import { useState } from "react";
import Layout from "@/components/Layout";
import { Card } from "@/components/Card";

type ApiHit = {
  datasetId: string;
  pk: string;
  preview: string;
  distance?: number | null;
};

type AskResponse = {
  answer?: string;
  // New shape from /api/ask
  used?: {
    exact?: ApiHit[];
    semantic?: ApiHit[];
    merged?: ApiHit[];
  };
  // Old shape fallback
  results?: ApiHit[];
  error?: string;
};

export default function AskPage() {
  const [q, setQ] = useState("");
  const [datasetId, setDatasetId] = useState<string>(""); // blank = all datasets
  const [loading, setLoading] = useState(false);
  const [resp, setResp] = useState<AskResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [k, setK] = useState<number>(8); // semantic neighbors
  const [exactLimit, setExactLimit] = useState<number>(3);

  const results: ApiHit[] =
    resp?.used?.merged ??
    resp?.results ??
    []; // safe fallback so .length and .map never explode

  async function onAsk(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setResp(null);
    setErr(null);
    try {
      const r = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          q,
          datasetId: datasetId && datasetId.toLowerCase() !== "all" ? datasetId : undefined,
          k,
          exactLimit,
        }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.error || r.statusText);
      setResp(j as AskResponse);
    } catch (e: any) {
      setErr(e?.message || "ask failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <Layout>
      <div className="mb-4">
        <h2 className="text-2xl font-semibold">Ask the Catalog</h2>
      </div>

      <Card>
        <form onSubmit={onAsk} className="flex flex-col gap-3">
          <div className="flex flex-col gap-2">
            <label htmlFor="q" className="text-sm text-slate-600">
              Question
            </label>
            <textarea
              id="q"
              placeholder='e.g. "is there a blue gorilla?"'
              value={q}
              onChange={(e) => setQ(e.target.value)}
              rows={3}
              className="w-full rounded-lg border border-slate-300 p-3 outline-none focus:ring focus:ring-indigo-200"
            />
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <div className="flex flex-col gap-2">
              <label htmlFor="dataset" className="text-sm text-slate-600">
                Dataset (optional)
              </label>
              <input
                id="dataset"
                placeholder='leave blank for "All Datasets" (e.g., db2_zos_corebank_accounts_1)'
                value={datasetId}
                onChange={(e) => setDatasetId(e.target.value)}
                className="w-full rounded-lg border border-slate-300 p-2 outline-none focus:ring focus:ring-indigo-200"
              />
            </div>

            <div className="flex flex-col gap-2">
              <label htmlFor="k" className="text-sm text-slate-600">
                Semantic neighbors (k)
              </label>
              <input
                id="k"
                type="number"
                min={1}
                max={25}
                value={k}
                onChange={(e) => setK(Number(e.target.value))}
                className="w-full rounded-lg border border-slate-300 p-2 outline-none focus:ring focus:ring-indigo-200"
              />
            </div>

            <div className="flex flex-col gap-2">
              <label htmlFor="exact" className="text-sm text-slate-600">
                Exact match limit
              </label>
              <input
                id="exact"
                type="number"
                min={0}
                max={10}
                value={exactLimit}
                onChange={(e) => setExactLimit(Number(e.target.value))}
                className="w-full rounded-lg border border-slate-300 p-2 outline-none focus:ring focus:ring-indigo-200"
              />
            </div>
          </div>

          <div className="flex items-center gap-3 pt-1">
            <button
              type="submit"
              disabled={loading || !q.trim()}
              className="inline-flex items-center rounded-lg bg-indigo-600 px-4 py-2 text-white font-semibold disabled:opacity-50"
            >
              {loading ? "Asking…" : "Ask"}
            </button>
            {err && <span className="text-sm text-rose-600">{err}</span>}
          </div>
        </form>
      </Card>

      {/* Answer */}
      {resp?.answer && (
        <div className="mt-4">
          <Card>
            <h3 className="text-lg font-semibold mb-2">Answer</h3>
            <div className="whitespace-pre-wrap text-slate-900">{resp.answer}</div>
          </Card>
        </div>
      )}

      {/* No Results Message */}
      {(!resp?.answer || results.length === 0) && !loading && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-xl p-6 text-center mt-4">
          <span className="text-2xl mb-2 block">🔍</span>
          <h3 className="text-lg font-semibold text-yellow-800 mb-2">No Results Found</h3>
          <p className="text-yellow-700">
            Try rephrasing your question or searching for different terms.
          </p>
        </div>
      )}

      {/* Results */}
      {results.length > 0 && (
        <div className="mt-4">
          <Card>
            <h3 className="text-lg font-semibold mb-3">Context Used</h3>
            <ul className="flex flex-col gap-3">
              {results.map((h, i) => (
                <li key={`${h.datasetId}::${h.pk}`} className="border border-slate-200 rounded-lg p-3">
                  <div className="text-xs text-slate-500 mb-1">
                    #{i + 1} • dataset=<span className="font-mono">{h.datasetId}</span> • pk=
                    <span className="font-mono">{h.pk}</span>
                    {typeof h.distance === "number" && (
                      <> • distance=<span className="font-mono">{h.distance.toFixed(4)}</span></>
                    )}
                  </div>
                  <div className="text-slate-900 whitespace-pre-wrap">
                    {h.preview || ""}
                  </div>
                </li>
              ))}
            </ul>
          </Card>
        </div>
      )}

      {/* Debug panes (optional): exact and semantic separately */}
      {resp?.used && (resp.used.exact?.length || resp.used.semantic?.length) ? (
        <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card>
            <h4 className="font-semibold mb-2">Exact matches</h4>
            {resp.used.exact && resp.used.exact.length > 0 ? (
              <ul className="flex flex-col gap-2">
                {resp.used.exact.map((h, i) => (
                  <li key={`ex-${h.datasetId}::${h.pk}`} className="text-sm">
                    <span className="text-slate-500 mr-2">#{i + 1}</span>
                    <span className="font-mono">{h.datasetId}</span> /{" "}
                    <span className="font-mono">{h.pk}</span> — {h.preview}
                  </li>
                ))}
              </ul>
            ) : (
              <div className="text-slate-500 text-sm">none</div>
            )}
          </Card>
          <Card>
            <h4 className="font-semibold mb-2">Semantic neighbors</h4>
            {resp.used.semantic && resp.used.semantic.length > 0 ? (
              <ul className="flex flex-col gap-2">
                {resp.used.semantic.map((h, i) => (
                  <li key={`sem-${h.datasetId}::${h.pk}`} className="text-sm">
                    <span className="text-slate-500 mr-2">#{i + 1}</span>
                    <span className="font-mono">{h.datasetId}</span> /{" "}
                    <span className="font-mono">{h.pk}</span> — {h.preview}
                    {typeof h.distance === "number" && (
                      <span className="text-slate-500"> (dist {h.distance.toFixed(4)})</span>
                    )}
                  </li>
                ))}
              </ul>
            ) : (
              <div className="text-slate-500 text-sm">none</div>
            )}
          </Card>
        </div>
      ) : null}
    </Layout>
  );
}
