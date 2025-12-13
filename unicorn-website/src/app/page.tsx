"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { TopTable } from "../components/TopTable";
import { API_BASE } from "../lib/apiBase";
import { fetchJson } from "../lib/fetchJson";
import { TeamDetailSchema, TeamsListSchema } from "../lib/schemas";

type Team = { team_id: number; team_name: string; abbrev: string };
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

export default function Home() {
  const [teams, setTeams] = useState<Team[]>([]);
  const [teamsError, setTeamsError] = useState<string | null>(null);
  const [teamsLoading, setTeamsLoading] = useState<boolean>(true);
  const [selectedTeam, setSelectedTeam] = useState<TeamDetail | null>(null);
  const [selectedError, setSelectedError] = useState<string | null>(null);
  const [loadingTeamId, setLoadingTeamId] = useState<number | null>(null);

  useEffect(() => {
    const url = `${API_BASE}/api/teams`;
    fetchJson<unknown>(url, { timeoutMs: 4000, init: { next: { revalidate: 300 } } })
      .then((res) => {
        if (!res.ok) {
          const status = res.status ? ` (status ${res.status})` : "";
          const host = (() => {
            try {
              return ` from ${new URL(url).host}`;
            } catch {
              return "";
            }
          })();
          setTeamsError(`Unable to load teams${status}${host}`);
          setTeams([]);
          return;
        }
        const parsed = TeamsListSchema.safeParse(res.data);
        if (!parsed.success) {
          setTeamsError("Unable to load teams (invalid data).");
          setTeams([]);
          return;
        }
        setTeams(parsed.data);
      })
      .catch((err) => {
        console.error("Teams fetch unexpected error", err);
        setTeamsError("Unable to load teams (unexpected error).");
        setTeams([]);
      })
      .finally(() => setTeamsLoading(false));
  }, []);

  const handleSelectTeam = async (teamId: number) => {
    const url = `${API_BASE}/api/teams/${teamId}`;
    console.log("Fetching team roster:", url);
    setLoadingTeamId(teamId);
    setSelectedError(null);
    try {
      const res = await fetchJson<unknown>(url, { timeoutMs: 4000 });
      if (!res.ok) {
        if (res.status) console.error(`Team fetch failed (${res.status}) for ${url}`);
        setSelectedError(
          res.status ? `Unable to load team (status ${res.status})` : "Unable to load team"
        );
        setSelectedTeam(null);
        return;
      }
      const parsed = TeamDetailSchema.safeParse(res.data);
      if (!parsed.success) {
        setSelectedError("Unable to load team (invalid data).");
        setSelectedTeam(null);
        return;
      }
      setSelectedTeam(parsed.data);
    } catch (err) {
      console.error("Team fetch error", err);
      setSelectedError("Unable to load team (network error)");
      setSelectedTeam(null);
    } finally {
      setLoadingTeamId(null);
    }
  };

  const renderRoster = (label: string, players: Player[], emptyText?: string) => {
    if (players?.length === 0 && label !== "Starters") {
      return null;
    }
    return (
      <div className="space-y-2">
        <h4 className="text-lg font-semibold text-neutral-900">{label}</h4>
        {players && players.length > 0 ? (
          <ul className="glass divide-y divide-neutral-300">
            {players.map((p: Player) => {
              const pid = p.player_id;
              const isValidId = typeof pid === "number" && Number.isFinite(pid);
              return (
                <li
                  key={pid ?? `${p.player_name}-${label}`}
                  className="px-4 py-3 flex items-center justify-between hover:bg-neutral-200"
                >
                  {isValidId ? (
                    <Link href={`/players/${pid}`} className="flex flex-col">
                      <span className="text-neutral-900 font-medium hover:underline">
                        {p.player_name || p.full_name || `Player ${pid}`}
                      </span>
                      <p className="text-xs text-neutral-500">
                        {p.position || p.role || label.slice(0, -1)}
                      </p>
                    </Link>
                  ) : (
                    <div className="flex flex-col">
                      <span className="text-neutral-900 font-medium">
                        {p.player_name || p.full_name || "Player"}
                      </span>
                      <p className="text-xs text-neutral-500">
                        {p.position || p.role || label.slice(0, -1)}
                      </p>
                    </div>
                  )}
                  <span className="text-xs uppercase text-neutral-500">{label.slice(0, -1)}</span>
                </li>
              );
            })}
          </ul>
        ) : (
          <p className="text-neutral-600">{emptyText || "No players found."}</p>
        )}
      </div>
    );
  };

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <p className="text-sm text-neutral-500">MLB Unicorn Engine</p>
          <h1 className="text-3xl font-semibold text-neutral-900">Unicorn Top 50</h1>
        </div>
        <div className="text-xs text-neutral-500">
          Live data from mlb-unicorn-engine.onrender.com
        </div>
      </div>
      <section className="glass rounded-3xl p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-semibold text-neutral-900">Teams</h2>
          <Link href="/teams" className="text-sm text-neutral-600 hover:underline">
            View all
          </Link>
        </div>
        {teamsError ? (
          <div className="border border-red-600 bg-red-50 text-red-700 px-4 py-3 text-sm">
            {teamsError}
          </div>
        ) : teamsLoading ? (
          <div className="border border-neutral-400 bg-neutral-100 text-neutral-800 px-4 py-3 text-sm">
            Loading teams…
          </div>
        ) : teams.length === 0 ? (
          <div className="border border-neutral-400 bg-neutral-100 text-neutral-800 px-4 py-3 text-sm">
            No teams available.
          </div>
        ) : (
          <div className="grid gap-3 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5">
            {teams.map((team) => (
              <button
                key={team.team_id}
                onClick={() => handleSelectTeam(team.team_id)}
                className="text-left glass p-4 hover:bg-neutral-200"
              >
                <p className="text-xs uppercase text-neutral-500">{team.abbrev}</p>
                <h3 className="text-lg font-semibold text-neutral-900">{team.team_name}</h3>
                {loadingTeamId === team.team_id && (
                  <p className="text-xs text-neutral-500 mt-1">Loading…</p>
                )}
              </button>
            ))}
          </div>
        )}
      </section>
      {selectedError && (
        <div className="border border-red-600 bg-red-50 text-red-700 px-4 py-3 text-sm">
          {selectedError}
        </div>
      )}
      {selectedTeam && (
        <section className="glass rounded-3xl p-6 space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-neutral-500">{selectedTeam.abbrev}</p>
              <h2 className="text-2xl font-semibold text-neutral-900">{selectedTeam.team_name}</h2>
            </div>
            <p className="text-xs text-neutral-500">
              Roster loaded from {API_BASE}/api/teams/{selectedTeam.team_id}
            </p>
          </div>
          <div className="grid gap-4 md:grid-cols-3">
            <div className="md:col-span-1">
              {renderRoster("Hitters", selectedTeam.hitters || [])}
            </div>
            <div className="md:col-span-1">
              {renderRoster("Starters", selectedTeam.starters || [], "No starters found.")}
            </div>
            <div className="md:col-span-1">
              {renderRoster("Relievers", selectedTeam.relievers || [])}
            </div>
          </div>
        </section>
      )}
      <TopTable />
    </main>
  );
}
