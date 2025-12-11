"use client";

import { useEffect, useState } from "react";

type PlayerResponse = {
  player_id: number;
  player_name: string;
  team_id: number | null;
  team_name: string | null;
  role: string | null;
  metrics: Record<string, number | null>;
  recent_unicorns: {
    run_date: string;
    pattern_id: string;
    description: string;
    metric_value: number;
    score: number;
  }[];
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "https://mlb-unicorn-engine.onrender.com";

export default function PlayerPage({ params }: { params: { id: string } }) {
  const playerId = params.id;
  const [data, setData] = useState<PlayerResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        setLoading(true);
        const res = await fetch(`${API_BASE}/players/${playerId}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        setData(json);
      } catch (err) {
        setError("Unable to load player data.");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [playerId]);

  const metricEntries = data ? Object.entries(data.metrics || {}) : [];

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <p className="text-sm text-neutral-500">
            MLB Unicorn Engine · {data?.team_name || "Unknown Team"}
          </p>
          <h1 className="text-3xl font-semibold text-neutral-900">
            {data?.player_name || "Loading..."}
          </h1>
          {data?.role && (
            <span className="text-sm text-neutral-600 uppercase tracking-wide">
              {data.role}
            </span>
          )}
        </div>
        <a
          href="/"
          className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition"
        >
          ← Back to Top 50
        </a>
      </div>

      <div className="glass rounded-3xl p-6 fade-in">
        <h2 className="text-xl font-semibold text-neutral-900 mb-4">Predictive Metrics</h2>
        {loading && <p className="text-neutral-600">Loading…</p>}
        {error && <p className="text-red-600">{error}</p>}
        {!loading && !error && metricEntries.length === 0 && (
          <p className="text-neutral-600">No metrics available.</p>
        )}
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {metricEntries.map(([key, value]) => (
            <div key={key} className="glass rounded-2xl p-4 shadow-sm">
              <p className="text-xs uppercase text-neutral-500">{key.replace(/_/g, " ")}</p>
              <p className="text-lg font-semibold text-neutral-900">
                {value !== null && value !== undefined ? Number(value).toFixed(3) : "—"}
              </p>
            </div>
          ))}
        </div>
      </div>

      <div className="glass rounded-3xl p-6 fade-in">
        <h2 className="text-xl font-semibold text-neutral-900 mb-4">Recent Unicorns</h2>
        {data?.recent_unicorns?.length ? (
          <div className="space-y-3">
            {data.recent_unicorns.map((u) => (
              <div key={`${u.run_date}-${u.pattern_id}`} className="rounded-2xl bg-white/70 px-4 py-3">
                <div className="flex items-center justify-between text-sm text-neutral-600">
                  <span>{u.run_date}</span>
                  <span className="font-mono">{u.pattern_id}</span>
                </div>
                <p className="mt-1 text-neutral-900">{u.description}</p>
                <p className="text-sm text-neutral-600 mt-1">
                  Score {u.score.toFixed(2)} · Metric {u.metric_value.toFixed(3)}
                </p>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-neutral-600">No recent unicorns.</p>
        )}
      </div>
    </main>
  );
}
