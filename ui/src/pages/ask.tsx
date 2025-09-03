// pages/ask.tsx
import React, { useState } from "react";

type AskResponse = {
  question: string;
  datasetId: string | null;
  k: number;
  results: any[];
  latency_ms: number;
  embedding_model: string;
  answer?: string;
  citations?: Array<{ dataset_id: string; pk: string; preview: string; distance: number }>;
  llm_model?: string;
};

export default function AskPage() {
  const [question, setQuestion] = useState("What is purple-elephant-42 and which user account is it associated with?");
  const [datasetId, setDatasetId] = useState<string>("ds_salesforce_accounts");
  const [k, setK] = useState<number>(3);
  const [useLlm, setUseLlm] = useState<boolean>(true);
  const [loading, setLoading] = useState(false);
  const [resp, setResp] = useState<AskResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function onAsk(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setResp(null);

    try {
      const body: any = {
        question,
        k,
        useLlm,
      };
      if (datasetId && datasetId !== "ALL") {
        body.datasetId = datasetId;
      }

      const r = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const txt = await r.text();
        throw new Error(`HTTP ${r.status}: ${txt}`);
      }
      const data: AskResponse = await r.json();
      setResp(data);
    } catch (err: any) {
      setError(String(err?.message || err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50">
      <div className="py-6 px-4">
        <div className="max-w-3xl mx-auto">
          
          {/* Header */}
          <div className="text-center mb-6">
            <h1 className="text-3xl font-bold text-gray-800 mb-2">AI Search</h1>
            <p className="text-gray-600">Ask questions about your data</p>
          </div>

          {/* Main Content Card */}
          <div className="bg-white rounded-2xl shadow-lg border border-gray-200">
            
            {/* Search Form */}
            <div className="p-6">
              <form onSubmit={onAsk} className="space-y-6">
                
                {/* Question Input Section */}
                <div className="space-y-3">
                  <label className="block text-lg font-semibold text-gray-800">
                  
                  </label>
                  <textarea
                    className="w-full rounded-xl border-2 border-gray-300 px-4 py-3 text-base focus:border-indigo-500 focus:ring-2 focus:ring-indigo-200 transition-all resize-none"
                    rows={3}
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="Ask something about your data..."
                    disabled={loading}
                  />
                </div>

                {/* Configuration Section */}
                <div className="bg-gray-50 rounded-xl p-4 border border-gray-200">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {/* Dataset Selection */}
                    <div className="space-y-2">
                      <label className="block text-sm font-semibold text-gray-700">
                        Dataset
                      </label>
                      <select
                        className="w-full rounded-xl border-2 border-gray-300 px-3 py-2 focus:border-indigo-500 focus:ring-2 focus:ring-indigo-200 transition-all bg-white"
                        value={datasetId}
                        onChange={(e) => setDatasetId(e.target.value)}
                        disabled={loading}
                      >
                        <option value="ALL">All Datasets</option>
                        <option value="ds_salesforce_accounts">Salesforce Accounts</option>
                      </select>
                    </div>

                    {/* Results Count */}
                    <div className="space-y-2">
                      <label className="block text-sm font-semibold text-gray-700">
                        Results Count
                      </label>
                      <input
                        type="number"
                        min={1}
                        max={20}
                        className="w-full rounded-xl border-2 border-gray-300 px-3 py-2 focus:border-indigo-500 focus:ring-2 focus:ring-indigo-200 transition-all bg-white"
                        value={k}
                        onChange={(e) => setK(Number(e.target.value))}
                        disabled={loading}
                      />
                    </div>
                  </div>

                  {/* AI Toggle */}
                  <div className="mt-4 pt-4 border-t border-gray-200">
                    <label className="flex items-center space-x-3">
                      <input
                        type="checkbox"
                        checked={useLlm}
                        onChange={(e) => setUseLlm(e.target.checked)}
                        className="w-5 h-5 text-indigo-600 focus:ring-indigo-500"
                        disabled={loading}
                      />
                      <span className="text-md font-semibold text-gray-800">Use AI Answer</span>
                    </label>
                  </div>
                </div>

                {/* Submit Button */}
                <button
                  type="submit"
                  disabled={loading}
                  className="w-full bg-indigo-600 text-white py-3 px-6 rounded-xl font-semibold hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all shadow-md"
                >
                  {loading ? (
                    <div className="flex items-center justify-center space-x-2">
                      <div className="animate-spin rounded-full h-5 w-5 border-2 border-white border-t-transparent"></div>
                      <span>Searching...</span>
                    </div>
                  ) : (
                    "Ask Question"
                  )}
                </button>

              </form>
            </div>
          </div>

          {/* Error Display */}
          {error && (
            <div className="mt-6 bg-red-50 border border-red-200 rounded-xl p-4">
              <div className="flex items-center space-x-3">
                <span className="text-xl">⚠️</span>
                <div>
                  <h3 className="font-semibold text-red-800 mb-1">Error</h3>
                  <p className="text-red-600">{error}</p>
                </div>
              </div>
            </div>
          )}

          {/* Results Display */}
          {resp && (
            <div className="mt-6 space-y-6">
              
              {/* AI Answer Card */}
              {resp.answer && (
                <div className="bg-white rounded-xl shadow-lg p-6 border border-gray-200">
                  <div className="flex items-center mb-4 space-x-3">
                    <span className="text-2xl">🤖</span>
                    <h2 className="text-xl font-semibold text-gray-800">AI Answer</h2>
                    {resp.llm_model && (
                      <span className="ml-auto text-sm text-gray-500 bg-gray-100 px-2 py-1 rounded">
                        {resp.llm_model}
                      </span>
                    )}
                  </div>
                  
                  <div className="bg-blue-50 rounded-lg p-4 border border-blue-200 mb-3">
                    <p className="text-gray-800 leading-relaxed">{resp.answer}</p>
                  </div>
                  
                  <div className="text-sm text-gray-500 text-right">
                    {resp.latency_ms}ms • {resp.embedding_model}
                  </div>
                </div>
              )}

              {/* No Results Message */}
              {(!resp.answer || resp.results.length === 0) && (
                <div className="bg-yellow-50 border border-yellow-200 rounded-xl p-6 text-center">
                  <span className="text-2xl mb-2 block">🔍</span>
                  <h3 className="text-lg font-semibold text-yellow-800 mb-2">No Results Found</h3>
                  <p className="text-yellow-700">Try rephrasing your question or searching for different terms.</p>
                </div>
              )}

            </div>
          )}

          {/* Footer spacing */}
          <div className="h-8"></div>

        </div>
      </div>
    </div>
  );
}