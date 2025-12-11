"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { fetchTeam, fetchTeams } from "../../../lib/api";
import { getTeamIdFromSlug, slugifyTeam } from "../../../lib/slugs";

type Team = { team_id: number; team_name: string; abbrev: string };

export default function TeamPage({ params }: { params: { teamSlug: string } }) {
  const [team, setTeam] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const teams: Team[] = await fetchTeams();
        const teamId = getTeamIdFromSlug(params.teamSlug, teams);
        if (!teamId) throw new Error("Not found");
        const detail = await fetchTeam(teamId);
        setTeam(detail);
      } catch (err) {
        setError("Unable to load team");
      }
    };
    load();
  }, [params.teamSlug]);

  if (error) {
    return (
      <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16">
        <p className="text-red-600">{error}</p>
      </main>
    );
  }

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-neutral-500">MLB Unicorn Engine</p>
          <h1 className="text-3xl font-semibold text-neutral-900">{team?.team_name || "Loading..."}</h1>
        </div>
        <div className="flex gap-2">
          <Link href="/teams" className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition">
            ← Teams
          </Link>
          <Link href="/" className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition">
            Top 50
          </Link>
        </div>
      </div>

      {team && (
        <div className="glass rounded-3xl p-6 space-y-4">
          <p className="text-neutral-700">Choose a roster view:</p>
          <div className="flex flex-wrap gap-3">
            <Link href={`/teams/${slugifyTeam(team.team_name)}/hitters`} className="glass px-4 py-2 rounded-full text-sm font-medium hover:-translate-y-0.5 transition">
              Hitters
            </Link>
            <Link href={`/teams/${slugifyTeam(team.team_name)}/starters`} className="glass px-4 py-2 rounded-full text-sm font-medium hover:-translate-y-0.5 transition">
              Starters
            </Link>
            <Link href={`/teams/${slugifyTeam(team.team_name)}/relievers`} className="glass px-4 py-2 rounded-full text-sm font-medium hover:-translate-y-0.5 transition">
              Relievers
            </Link>
          </div>
        </div>
      )}
    </main>
  );
}
