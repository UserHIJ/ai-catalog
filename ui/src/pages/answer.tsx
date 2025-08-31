// pages/answers.tsx
import { useState } from "react";

type Message = { role: "user" | "assistant"; content: string; meta?: any };

export default function AnswersPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [datasetId, setDatasetId] = useState("ds_entity_001");
  const [topK, setTopK] = useState(5);

  async function onSend(e: React.FormEvent) {
    e.preventDefault();
    if (!input.trim()) return;

    const question = input.trim();
    setMessages((m) => [...m, { role: "user", content: question }]);
    setInput("");
    setLoading(true);

    try {
      const r = await fetch("/api/answer", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, datasetId: datasetId || null, k: topK }),
      });
      const j = await r.json();
      const bot: Message = {
        role: "assistant",
        content: j.answer || "(no answer)",
        meta: { citations: j.citations, latency: j.latency_ms, relevant: j.relevant },
      };
      setMessages((m) => [...m, bot]);
    } catch (err: any) {
      setMessages((m) => [
        ...m,
        { role: "assistant", content: `Error: ${err?.message || String(err)}` },
      ]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div
      className="min-h-screen text-gray-900"
      style={{
        // One real background with two layers: grid + watermark image.
        backgroundImage:
          "radial-gradient(circle at 1px 1px, rgba(0,0,0,0.06) 1px, transparent 0), url('/fivetran.png')",
        backgroundSize: "22px 22px, 50vw",
        backgroundPosition: "0 0, center",
        backgroundRepeat: "repeat, no-repeat",
        backgroundAttachment: "fixed, fixed",
        // Faint watermark:
        filter: "none",
      }}
    >
      {/* Top bar */}
      <header className="border-b bg-white/80 backdrop-blur supports-[backdrop-filter]:bg-white/60">
        <div className="mx-auto flex max-w-5xl items-center gap-3 p-3">
          <img src="/fivetran.png" alt="Fivetran" className="h-6 w-auto" />
          <div className="text-sm text-gray-600">AI Data Catalog</div>
          <div className="ml-auto flex items-center gap-2">
            <input
              className="w-48 rounded border px-2 py-1 text-sm bg-white/90"
              placeholder="datasetId (blank = ALL)"
              value={datasetId}
              onChange={(e) => setDatasetId(e.target.value)}
            />
            <input
              type="number"
              min={1}
              max={50}
              className="w-20 rounded border px-2 py-1 text-sm bg-white/90"
              value={topK}
              onChange={(e) => setTopK(Number(e.target.value))}
              title="Top K"
            />
          </div>
        </div>
      </header>

      {/* Chat */}
      <main className="mx-auto flex min-h-[calc(100vh-56px)] max-w-5xl flex-col">
        <div className="flex-1 space-y-4 overflow-y-auto p-4 sm:p-6">
          {messages.map((m, i) => (
            <div key={i} className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
              <div
                className={`max-w-[80%] rounded-2xl px-4 py-3 shadow-sm ring-1 ring-black/5 ${
                  m.role === "user"
                    ? "bg-blue-600 text-white rounded-br-none"
                    : "bg-white/95 text-gray-900 backdrop-blur rounded-bl-none"
                }`}
              >
                <div className="whitespace-pre-wrap">{m.content}</div>
                {m.role === "assistant" && m.meta && (
                  <div className="mt-2 flex items-center gap-2 text-xs text-gray-500">
                    {!!m.meta.relevant &&
                      m.meta.citations?.length > 0 &&
                      !/i don['’]t know/i.test(m.content) && (
                        <div className="flex flex-wrap gap-1">
                          {m.meta.citations.map((c: any, j: number) => (
                            <span key={j} className="rounded bg-gray-100 px-1.5 py-0.5">
                              [{c.dataset_id}:{c.pk}]
                            </span>
                          ))}
                        </div>
                      )}
                    <span className="ml-auto">{m.meta.latency}ms</span>
                  </div>
                )}
              </div>
            </div>
          ))}
          {loading && (
            <div className="flex justify-start">
              <div className="rounded-2xl rounded-bl-none bg-white/95 px-4 py-3 text-gray-400 ring-1 ring-black/5 shadow-sm">
                <span className="animate-pulse">…</span>
              </div>
            </div>
          )}
        </div>

        {/* Input bar */}
        <form onSubmit={onSend} className="sticky bottom-0 border-t bg-white/80 backdrop-blur">
          <div className="mx-auto flex max-w-5xl gap-2 p-3">
            <input
              className="flex-1 rounded-xl border px-4 py-3 shadow-sm outline-none ring-1 ring-black/5 focus:ring-blue-500/40 bg-white/95"
              placeholder="Ask your data…"
              value={input}
              onChange={(e) => setInput(e.target.value)}
            />
            <button
              type="submit"
              className="rounded-xl bg-blue-600 px-5 py-3 font-medium text-white shadow-sm transition active:scale-[0.99] disabled:opacity-50"
              disabled={loading || !input.trim()}
            >
              Send
            </button>
          </div>
        </form>
      </main>
    </div>
  );
}
