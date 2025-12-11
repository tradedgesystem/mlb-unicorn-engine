"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { fetchPlayerProfile, fetchPlayers } from "../../../lib/api";
import { getPlayerIdFromSlug, reverseSlugToName } from "../../../lib/slugs";

export default function PlayerPage({ params }: { params: { playerSlug: string } }) {
  const [playerId, setPlayerId] = useState<number | null>(null);
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const load = async () => {
      try {
        const players = await fetchPlayers();
        const id = getPlayerIdFromSlug(params.playerSlug, players);
        if (!id) throw new Error("Player not found");
        setPlayerId(id);
        const detail = await fetchPlayerProfile(id);
        setData(detail);
      } catch (err) {
        setError("Unable to load player data.");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [params.playerSlug]);

  const metricEntries = useMemo(() => (data ? Object.entries(data.metrics || {}) : []), [data]);

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-neutral-500">{data?.team_name || "Player"}</p>
          <h1 className="text-3xl font-semibold text-neutral-900">
            {data?.player_name || reverseSlugToName(params.playerSlug)}
          </h1>
          {data?.role && <span className="text-sm text-neutral-600 uppercase tracking-wide">{data.role}</span>}
        </div>
        <div className="flex gap-2">
          <Link href="/" className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition">
            Top 50
          </Link>
          <Link href="/teams" className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition">
            Teams
          </Link>
        </div>
      </div>

      <div className="glass rounded-3xl p-6 space-y-4">
        <h2 className="text-xl font-semibold text-neutral-900">Predictive Metrics</h2>
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

      <div className="glass rounded-3xl p-6 space-y-3">
        <h2 className="text-xl font-semibold text-neutral-900">Recent Unicorns</h2>
        {data?.recent_unicorns?.length ? (
          <div className="space-y-3">
            {data.recent_unicorns.map((u: any) => (
              <div key={`${u.run_date}-${u.pattern_id}`} className="rounded-2xl bg-white/70 px-4 py-3">
                <div className="flex items-center justify-between text-sm text-neutral-600">
                  <span>{u.run_date}</span>
                  <span className="font-mono">{u.pattern_id}</span>
                </div>
                <p className="mt-1 text-neutral-900">{u.description}</p>
                <p className="text-sm text-neutral-600 mt-1">
                  Score {Number(u.score).toFixed(2)} · Metric {Number(u.metric_value).toFixed(3)}
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
