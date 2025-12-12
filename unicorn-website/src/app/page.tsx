"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { TopTable } from "../components/TopTable";
import { fetchTeams } from "../lib/api";

type Team = { team_id: number; team_name: string; abbrev: string };

export default function Home() {
  const [teams, setTeams] = useState<Team[]>([]);
  const [teamsError, setTeamsError] = useState<string | null>(null);

  useEffect(() => {
    fetchTeams()
      .then((data) => setTeams(data))
      .catch((err: unknown) => {
        const e = err as { status?: number; host?: string };
        const status = e?.status ? ` (${e.status})` : "";
        const host = e?.host ? ` from ${e.host}` : "";
        setTeamsError(`Unable to load teams${status}${host}`);
      });
  }, []);

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
          <div className="rounded-2xl bg-red-50 text-red-700 px-4 py-3 text-sm">
            {teamsError}
          </div>
        ) : (
          <div className="grid gap-3 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5">
            {teams.map((team) => (
              <Link
                key={team.team_id}
                href={`/teams/${team.team_id}`}
                className="glass rounded-2xl p-4 hover:-translate-y-1 transition shadow-sm"
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
