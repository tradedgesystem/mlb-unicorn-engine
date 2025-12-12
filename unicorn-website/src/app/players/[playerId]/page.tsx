"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { fetchPlayerProfile } from "../../../lib/api";

type PlayerResponse = {
  player_id: number;
  player_name: string;
  team_id?: number;
  team_name?: string;
  role?: string;
  metrics?: Record<string, number | null>;
  recent_unicorns?: {
    run_date: string;
    pattern_id: string;
    description: string;
    metric_value: number;
    score: number;
  }[];
};

const METRIC_KEYS: Record<string, { key: string; label: string }[]> = {
  hitter: [
    { key: "barrel_pct_last_50", label: "Barrel %" },
    { key: "hard_hit_pct_last_50", label: "Hard-Hit %" },
    { key: "xwoba_last_50", label: "xwOBA" },
    { key: "contact_pct_last_50", label: "Contact %" },
    { key: "chase_pct_last_50", label: "Chase %" },
  ],
  starter: [
    { key: "xwoba_last_3_starts", label: "xwOBA Allowed" },
    { key: "whiff_pct_last_3_starts", label: "Whiff %" },
    { key: "k_pct_last_3_starts", label: "K %" },
    { key: "bb_pct_last_3_starts", label: "BB %" },
    { key: "hard_hit_pct_last_3_starts", label: "Hard-Hit % Allowed" },
  ],
  reliever: [
    { key: "xwoba_last_5_apps", label: "xwOBA Allowed" },
    { key: "whiff_pct_last_5_apps", label: "Whiff %" },
    { key: "k_pct_last_5_apps", label: "K %" },
    { key: "bb_pct_last_5_apps", label: "BB %" },
    { key: "hard_hit_pct_last_5_apps", label: "Hard-Hit % Allowed" },
  ],
};

export default function PlayerPage({ params }: { params: { playerId: string } }) {
  const playerId = Number(params.playerId);
  const [data, setData] = useState<PlayerResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (Number.isNaN(playerId)) {
      setError("Invalid player id.");
      setLoading(false);
      return;
    }
    const load = async () => {
      try {
        setLoading(true);
        setError(null);
        const detail = await fetchPlayerProfile(playerId);
        setData(detail);
      } catch {
        setError("Unable to load player data.");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [playerId]);

  const metrics = useMemo(() => {
    const roleKey = (data?.role || "hitter").toLowerCase();
    const configs = METRIC_KEYS[roleKey] || METRIC_KEYS.hitter;
    const metricsObj = data?.metrics || {};
    return configs.map((cfg) => ({
      ...cfg,
      value: metricsObj[cfg.key],
    }));
  }, [data]);

  const hasMetrics = metrics.some((m) => m.value !== null && m.value !== undefined);

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-neutral-500">{data?.team_name || "Player"}</p>
          <h1 className="text-3xl font-semibold text-neutral-900">
            {data?.player_name || `Player ${playerId}`}
          </h1>
          {data?.role && (
            <span className="text-sm text-neutral-600 uppercase tracking-wide">{data.role}</span>
          )}
        </div>
        <div className="flex gap-2">
          <Link
            href="/"
            className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition"
          >
            Top 50
          </Link>
          <Link
            href="/teams"
            className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition"
          >
            Teams
          </Link>
        </div>
      </div>

      <div className="glass rounded-3xl p-6 space-y-4">
        <h2 className="text-xl font-semibold text-neutral-900">Predictive Metrics</h2>
        {loading && <p className="text-neutral-600">Loading…</p>}
        {error && <p className="text-red-600">{error}</p>}
        {!loading && !error && !hasMetrics && (
          <p className="text-neutral-600">Metrics unavailable.</p>
        )}
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {metrics.map((m) => (
            <div key={m.key} className="glass rounded-2xl p-4 shadow-sm">
              <p className="text-xs uppercase text-neutral-500">{m.label}</p>
              <p className="text-lg font-semibold text-neutral-900">
                {m.value !== null && m.value !== undefined
                  ? Number(m.value).toFixed(3)
                  : "—"}
              </p>
            </div>
          ))}
        </div>
      </div>

      <div className="glass rounded-3xl p-6 space-y-3">
        <h2 className="text-xl font-semibold text-neutral-900">Recent Unicorns</h2>
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
