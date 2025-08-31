import { useState } from "react";

interface AskResultRow {
  dataset_id: string | null;
  pk: string | number;
  text_chunk: string;
  distance: number;
}

interface AskResponse {
  question: string;
  datasetId: string | null;
  k: number;
  results: AskResultRow[];
}

export default function Ask() {
  const [question, setQuestion] = useState("");
  const [datasetId, setDatasetId] = useState<string>("ds_entity_001"); // leave blank for ALL
  const [topK, setTopK] = useState<number>(5);
  const [loading, setLoading] = useState(false);
  const [resp, setResp] = useState<AskResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmedQ = question.trim();
    if (!trimmedQ) return;

    setLoading(true);
    setErr(null);
    setResp(null);

    try {
      const payload = {
        question: trimmedQ,
        datasetId: datasetId.trim() ? datasetId.trim() : null,
        k: Number.isFinite(Number(topK)) ? Number(topK) : 5,
      };

      const r = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const j = (await r.json()) as AskResponse & { error?: string };
      if (!r.ok) throw new Error(j.error || r.statusText);
      setResp(j);
    } catch (e: any) {
      setErr(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="p-6 max-w-3xl mx-auto space-y-4">
      <h1 className="text-2xl font-semibold">Ask your data</h1>

      <form onSubmit={onSubmit} className="space-y-3">
        <input
          className="w-full border rounded p-2"
          placeholder='e.g. "around 100 dollars"'
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
        />

        <div className="flex gap-2">
          <input
            className="flex-1 border rounded p-2"
            placeholder="datasetId (leave blank for ALL)"
            value={datasetId}
            onChange={(e) => setDatasetId(e.target.value)}
          />
          <input
            type="number"
            min={1}
            max={50}
            className="w-24 border rounded p-2"
            value={topK}
            onChange={(e) => setTopK(Number(e.target.value))}
            title="Top K"
          />
          <button
            className="px-4 py-2 rounded bg-black text-white disabled:opacity-50"
            disabled={loading || !question.trim()}
            type="submit"
          >
            {loading ? "Searching…" : "Ask"}
          </button>
        </div>
      </form>

      {err && <div className="text-red-600">{err}</div>}

      {resp && (
        <div className="space-y-2">
          <div className="text-sm text-gray-600">
            Top <span className="font-semibold">{resp.k}</span> for “
            <span className="font-mono">{resp.question}</span>” in{" "}
            <span className="font-mono">{resp.datasetId ?? "ALL"}</span>
          </div>

          <ul className="space-y-3">
            {resp.results.map((r, i) => (
              <li
                key={`${r.dataset_id ?? "ALL"}-${r.pk}-${i}`}
                className="border rounded p-3"
              >
                <div className="text-xs text-gray-500">
                  {r.dataset_id ?? "ALL"} · pk={r.pk} · dist=
                  {Number(r.distance).toFixed(4)}
                </div>
                <pre className="whitespace-pre-wrap break-words text-sm mt-1">
                  {r.text_chunk}
                </pre>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
