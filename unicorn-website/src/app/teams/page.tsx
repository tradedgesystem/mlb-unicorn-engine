"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { fetchTeams } from "../../lib/api";
import { slugifyTeam } from "../../lib/slugs";

type Team = { team_id: number; team_name: string; abbrev: string };

export default function TeamsPage() {
  const [teams, setTeams] = useState<Team[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchTeams()
      .then((data) => setTeams(data))
      .catch(() => setError("Unable to load teams"));
  }, []);

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-neutral-500">MLB Unicorn Engine</p>
          <h1 className="text-3xl font-semibold text-neutral-900">Teams</h1>
        </div>
        <Link href="/" className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition">
          ← Top 50
        </Link>
      </div>

      {error ? (
        <p className="text-red-600">{error}</p>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
          {teams.map((team) => (
            <Link
              key={team.team_id}
              href={`/teams/${slugifyTeam(team.team_name)}`}
              className="glass rounded-3xl p-4 hover:-translate-y-1 transition shadow-sm"
            >
              <p className="text-xs uppercase text-neutral-500">{team.abbrev}</p>
              <h2 className="text-lg font-semibold text-neutral-900">{team.team_name}</h2>
            </Link>
          ))}
        </div>
      )}
    </main>
  );
}
