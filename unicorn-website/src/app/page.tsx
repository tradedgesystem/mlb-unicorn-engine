"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { TopTable } from "../components/TopTable";
import { fetchJson } from "../lib/fetchJson";
import { TeamsListSchema } from "../lib/schemas";

type Team = { team_id: number; team_name: string; abbrev: string };

export default function Home() {
  const [teams, setTeams] = useState<Team[]>([]);
  const [teamsError, setTeamsError] = useState<string | null>(null);
  const [teamsLoading, setTeamsLoading] = useState<boolean>(true);

  useEffect(() => {
    const url = "/api/teams";
    fetchJson<unknown>(url, { timeoutMs: 4000, init: { next: { revalidate: 300 } } })
      .then((res) => {
        if (!res.ok) {
          const status = res.status ? ` (status ${res.status})` : "";
          const host = typeof window !== "undefined" ? ` from ${window.location.host}` : "";
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

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <p className="text-sm text-neutral-500">MLB Unicorn Engine</p>
          <h1 className="text-3xl font-semibold text-neutral-900">Unicorn Top 50</h1>
        </div>
        <div className="text-xs text-neutral-500">
          Live data via /api proxy
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
              <Link
                key={team.team_id}
                href={`/teams/${team.team_id}`}
                className="text-left glass p-4 hover:bg-neutral-200"
              >
                <p className="text-xs uppercase text-neutral-500">{team.abbrev}</p>
                <h3 className="text-lg font-semibold text-neutral-900">{team.team_name}</h3>
              </Link>
            ))}
          </div>
        )}
      </section>
      <TopTable />
    </main>
  );
}
