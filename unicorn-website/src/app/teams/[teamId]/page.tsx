"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { fetchTeam } from "../../../lib/api";

type Player = {
  player_id: number;
  player_name?: string;
  full_name?: string;
  position?: string;
  role?: string;
};

type TeamDetail = {
  team_id: number;
  team_name: string;
  abbrev: string;
  hitters: Player[];
  starters: Player[];
  relievers: Player[];
};

type TabKey = "hitters" | "starters" | "relievers";

export default function TeamPage({ params }: { params: { teamId: string } }) {
  const teamId = Number(params.teamId);
  const [team, setTeam] = useState<TeamDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabKey>("hitters");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (Number.isNaN(teamId)) {
      setError("Invalid team id");
      setLoading(false);
      return;
    }
    const load = async () => {
      try {
        setLoading(true);
        setError(null);
        const detail = await fetchTeam(teamId);
        setTeam(detail);
      } catch {
        setError("Unable to load team");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [teamId]);

  const roster = useMemo(() => {
    if (!team) return [];
    if (activeTab === "hitters") return team.hitters || [];
    if (activeTab === "starters") return team.starters || [];
    return team.relievers || [];
  }, [team, activeTab]);

  const hasRoster =
    (team?.hitters?.length || 0) > 0 ||
    (team?.starters?.length || 0) > 0 ||
    (team?.relievers?.length || 0) > 0;

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <p className="text-sm text-neutral-500">{team?.abbrev || "Team"}</p>
          <h1 className="text-3xl font-semibold text-neutral-900">
            {team?.team_name || "Loading..."}
          </h1>
        </div>
        <div className="flex gap-2">
          <Link
            href="/teams"
            className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition"
          >
            ← Teams
          </Link>
          <Link
            href="/"
            className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition"
          >
            Top 50
          </Link>
        </div>
      </div>

      {error && (
        <div className="rounded-2xl bg-red-50 text-red-700 px-4 py-3 text-sm">
          {error}
        </div>
      )}

      <div className="glass rounded-3xl p-6 space-y-4">
        <div className="flex flex-wrap gap-3">
          {(["hitters", "starters", "relievers"] as TabKey[]).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`rounded-full px-4 py-2 text-sm font-medium transition ${
                activeTab === tab
                  ? "glass shadow-sm text-neutral-900"
                  : "bg-white/70 text-neutral-600 hover:-translate-y-0.5"
              }`}
            >
              {tab === "hitters" ? "Hitters" : tab === "starters" ? "Starters" : "Relievers"}
            </button>
          ))}
        </div>

        {loading ? (
          <p className="text-neutral-600">Loading roster…</p>
        ) : !hasRoster ? (
          <p className="text-neutral-600">No roster data.</p>
        ) : roster.length === 0 ? (
          <p className="text-neutral-600">No players in this group.</p>
        ) : (
          <ul className="divide-y divide-white/40 rounded-2xl bg-white/60 shadow-sm">
            {roster.map((p) => {
              const pid = p.player_id;
              const valid = typeof pid === "number" && Number.isFinite(pid);
              return (
                <li
                  key={pid ?? p.player_name ?? p.full_name ?? `${activeTab}-unknown`}
                  className="px-4 py-3 flex items-center justify-between"
                >
                  {valid ? (
                    <Link
                      href={`/players/${pid}`}
                      className="text-neutral-900 font-medium hover:underline"
                    >
                      {p.player_name || p.full_name || `Player ${pid}`}
                    </Link>
                  ) : (
                    <span className="text-neutral-900 font-medium">
                      {p.player_name || p.full_name || "Player"}
                    </span>
                  )}
                  <p className="text-xs text-neutral-500">
                    {p.position ||
                      p.role ||
                      (activeTab === "hitters"
                        ? "Hitter"
                        : activeTab === "starters"
                        ? "Starter"
                        : "Reliever")}
                  </p>
                  <span className="text-xs uppercase text-neutral-500">
                    {activeTab === "hitters"
                      ? "Hitter"
                      : activeTab === "starters"
                      ? "Starter"
                      : "Reliever"}
                  </span>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </main>
  );
}
