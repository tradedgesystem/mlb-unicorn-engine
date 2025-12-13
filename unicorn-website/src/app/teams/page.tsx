"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { API_BASE } from "../../lib/apiBase";
import { fetchJson } from "../../lib/fetchJson";
import { TeamsListSchema } from "../../lib/schemas";

type Team = { team_id: number; team_name: string; abbrev: string };

export default function TeamsPage() {
  const [teams, setTeams] = useState<Team[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    const url = `${API_BASE}/api/teams`;
    fetchJson<unknown>(url, { timeoutMs: 4000, init: { next: { revalidate: 300 } } })
      .then((res) => {
        if (!res.ok) {
          setError(res.status ? `Unable to load teams (status ${res.status})` : "Unable to load teams");
          setTeams([]);
          return;
        }
        const parsed = TeamsListSchema.safeParse(res.data);
        if (!parsed.success) {
          setError("Unable to load teams (invalid data).");
          setTeams([]);
          return;
        }
        setTeams(parsed.data);
      })
      .catch((err) => {
        console.error("Teams list unexpected error", err);
        setError("Unable to load teams (unexpected error).");
        setTeams([]);
      })
      .finally(() => setLoading(false));
  }, []);

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-neutral-500">MLB Unicorn Engine</p>
          <h1 className="text-3xl font-semibold text-neutral-900">Teams</h1>
        </div>
        <Link href="/" className="glass px-4 py-2 text-sm text-neutral-900 hover:bg-neutral-200">
          ← Top 50
        </Link>
      </div>

      {error ? (
        <div className="border border-red-600 bg-red-50 text-red-700 px-4 py-3 text-sm">{error}</div>
      ) : loading ? (
        <div className="glass p-4 text-sm text-neutral-800">Loading...</div>
      ) : teams.length === 0 ? (
        <div className="glass p-4 text-sm text-neutral-800">No teams available.</div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
          {teams.map((team) => (
            <Link
              key={team.team_id}
              href={`/teams/${team.team_id}`}
              className="glass p-4 hover:bg-neutral-200"
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
