"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { API_BASE } from "../../../lib/apiBase";
import { fetchJson } from "../../../lib/fetchJson";
import { TeamDetailSchema, TeamsListSchema } from "../../../lib/schemas";
import * as Sentry from "@sentry/nextjs";

type Player = {
  player_id?: number;
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

function teamFetchErrorMessage(res: { status?: number; error?: string }): string {
  const err = (res.error || "").toLowerCase();
  if (err.includes("timeout")) return "Unable to load team (timeout)";
  if (err.startsWith("invalid json")) return "Unable to load team (invalid response)";
  if (typeof res.status === "number") return `Unable to load team (status ${res.status})`;
  return "Unable to load team";
}

function coerceRosterPlayers(value: unknown): Player[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item) => item && typeof item === "object")
    .map((item) => {
      const obj = item as Record<string, unknown>;
      const pid =
        typeof obj.player_id === "number"
          ? obj.player_id
          : typeof obj.player_id === "string"
            ? Number(obj.player_id)
            : typeof obj.id === "number"
              ? obj.id
              : typeof obj.id === "string"
                ? Number(obj.id)
                : undefined;
      return {
        player_id: Number.isFinite(pid as number) ? (pid as number) : undefined,
        player_name: typeof obj.player_name === "string" ? obj.player_name : undefined,
        full_name: typeof obj.full_name === "string" ? obj.full_name : undefined,
        position: typeof obj.position === "string" ? obj.position : null,
        role: typeof obj.role === "string" ? obj.role : null,
      };
    });
}

export default function TeamPage({ params }: { params: { teamId: string } }) {
  const rawTeamId = params.teamId;
  const [team, setTeam] = useState<TeamDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabKey>("hitters");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        setLoading(true);
        setError(null);
        setNotice(null);
        setTeam(null);

        const isNumeric = /^[0-9]+$/.test(rawTeamId);
        let resolvedTeamId: number | null = isNumeric ? Number.parseInt(rawTeamId, 10) : null;
        let resolvedMeta: { team_id: number; team_name: string; abbrev: string } | null = null;

        if (resolvedTeamId === null) {
          const teamsUrl = `${API_BASE}/api/teams`;
          const teamsRes = await fetchJson<unknown>(teamsUrl, {
            timeoutMs: 4000,
            init: { next: { revalidate: 300 } },
          });
          if (!teamsRes.ok) {
            if (cancelled) return;
            const message = teamFetchErrorMessage(teamsRes);
            console.error(message, teamsRes.error);
            Sentry.captureMessage(message, { level: "error", extra: { url: teamsUrl, ...teamsRes } });
            setError(message);
            return;
          }
          const parsedTeams = TeamsListSchema.safeParse(teamsRes.data);
          if (!parsedTeams.success) {
            if (cancelled) return;
            const message = "Unable to load team (invalid response)";
            console.error(message);
            Sentry.captureMessage(message, { level: "error", extra: { url: teamsUrl } });
            setError(message);
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
          resolvedMeta = match;
        }

        if (resolvedTeamId === null || !Number.isFinite(resolvedTeamId)) {
          if (cancelled) return;
          setError("Team not found");
          return;
        }

        const url = `${API_BASE}/api/teams/${resolvedTeamId}`;
        const res = await fetchJson<unknown>(url, { timeoutMs: 10000 });
        if (!res.ok) {
          if (cancelled) return;
          const message = teamFetchErrorMessage(res);
          console.error(message, res.error);
          Sentry.captureMessage(message, { level: "error", extra: { url, ...res } });
          setError(message);
          return;
        }
        const parsed = TeamDetailSchema.safeParse(res.data);
        if (!parsed.success) {
          const raw = res.data as Record<string, unknown> | null;
          const hitters = raw ? coerceRosterPlayers(raw.hitters) : [];
          const starters = raw ? coerceRosterPlayers(raw.starters) : [];
          const relievers = raw ? coerceRosterPlayers(raw.relievers) : [];

          const hasAnyRoster =
            hitters.length > 0 || starters.length > 0 || relievers.length > 0;

          if (cancelled) return;
          if (hasAnyRoster) {
            const bestEffort: TeamDetail = {
              team_id: resolvedMeta?.team_id ?? (typeof raw?.team_id === "number" ? raw.team_id : resolvedTeamId),
              team_name:
                resolvedMeta?.team_name ??
                (typeof raw?.team_name === "string" ? raw.team_name : `Team ${resolvedTeamId}`),
              abbrev:
                resolvedMeta?.abbrev ??
                (typeof raw?.abbrev === "string" ? raw.abbrev : rawTeamId.toUpperCase()),
              hitters,
              starters,
              relievers,
            };
            setTeam(bestEffort);
            setNotice("Unable to load team (invalid response)");
            console.error("Team detail schema invalid; rendering best-effort roster");
            Sentry.captureMessage("Team detail schema invalid; rendering best-effort roster", {
              level: "warning",
              extra: { url, teamId: resolvedTeamId },
            });
            return;
          }

          setNotice("Roster unavailable");
          console.error("Team detail schema invalid; roster unavailable");
          Sentry.captureMessage("Team detail schema invalid; roster unavailable", {
            level: "warning",
            extra: { url, teamId: resolvedTeamId },
          });
          return;
        }
        if (cancelled) return;
        setTeam(parsed.data);
      } catch (err) {
        if (cancelled) return;
        console.error("Unable to load team", err);
        Sentry.captureException(err);
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
      {notice && !error && (
        <div className="border border-neutral-400 bg-neutral-100 text-neutral-800 px-4 py-3 text-sm">
          {notice}
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
