"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { API_BASE } from "../../../lib/apiBase";
import { fetchJson } from "../../../lib/fetchJson";
import { TeamDetailSchema, TeamsListSchema } from "../../../lib/schemas";

type Player = {
  player_id: number;
  player_name?: string;
  full_name?: string;
  position?: string | null;
  role?: string | null;
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
  const rawTeamId = params.teamId;
  const [team, setTeam] = useState<TeamDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabKey>("hitters");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        setLoading(true);
        setError(null);
        setTeam(null);

        const isNumeric = /^[0-9]+$/.test(rawTeamId);
        let resolvedTeamId: number | null = isNumeric ? Number.parseInt(rawTeamId, 10) : null;

        if (resolvedTeamId === null) {
          const teamsUrl = `${API_BASE}/api/teams`;
          const teamsRes = await fetchJson<unknown>(teamsUrl, {
            timeoutMs: 4000,
            init: { next: { revalidate: 300 } },
          });
          if (!teamsRes.ok) {
            if (cancelled) return;
            setError(
              teamsRes.status
                ? `Unable to load team (status ${teamsRes.status})`
                : "Unable to load team"
            );
            return;
          }
          const parsedTeams = TeamsListSchema.safeParse(teamsRes.data);
          if (!parsedTeams.success) {
            if (cancelled) return;
            setError("Unable to load team (invalid teams list).");
            return;
          }
          const needle = rawTeamId.toUpperCase();
          const match = parsedTeams.data.find((t) => t.abbrev.toUpperCase() === needle);
          if (!match) {
            if (cancelled) return;
            setError("Team not found");
            return;
          }
          resolvedTeamId = match.team_id;
        }

        const url = `${API_BASE}/api/teams/${resolvedTeamId}`;
        const res = await fetchJson<unknown>(url, { timeoutMs: 4000 });
        if (!res.ok) {
          if (cancelled) return;
          setError(res.status ? `Unable to load team (status ${res.status})` : "Unable to load team");
          return;
        }
        const parsed = TeamDetailSchema.safeParse(res.data);
        if (!parsed.success) {
          if (cancelled) return;
          setError("Unable to load team (invalid data).");
          return;
        }
        if (cancelled) return;
        setTeam(parsed.data);
      } catch {
        if (cancelled) return;
        setError("Unable to load team");
      } finally {
        if (cancelled) return;
        setLoading(false);
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [rawTeamId]);

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
            className="glass px-4 py-2 text-sm text-neutral-900 hover:bg-neutral-200"
          >
            ← Teams
          </Link>
          <Link
            href="/"
            className="glass px-4 py-2 text-sm text-neutral-900 hover:bg-neutral-200"
          >
            Top 50
          </Link>
        </div>
      </div>

      {error && (
        <div className="border border-red-600 bg-red-50 text-red-700 px-4 py-3 text-sm">
          {error}
        </div>
      )}

      <div className="glass rounded-3xl p-6 space-y-4">
        <div className="flex flex-wrap gap-3">
          {(["hitters", "starters", "relievers"] as TabKey[]).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-2 text-sm font-medium border border-neutral-400 ${
                activeTab === tab
                  ? "glass text-neutral-900"
                  : "bg-transparent text-neutral-900 hover:bg-neutral-200"
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
          <ul className="glass divide-y divide-neutral-300">
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
