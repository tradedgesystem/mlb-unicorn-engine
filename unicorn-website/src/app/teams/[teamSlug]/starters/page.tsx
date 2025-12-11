"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { fetchTeam, fetchTeams } from "../../../../lib/api";
import { getTeamIdFromSlug, slugifyPlayer } from "../../../../lib/slugs";

type Team = { team_id: number; team_name: string; abbrev: string };

export default function TeamStartersPage({ params }: { params: { teamSlug: string } }) {
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
          <p className="text-sm text-neutral-500">Starters</p>
          <h1 className="text-3xl font-semibold text-neutral-900">{team?.team_name || "Loading..."}</h1>
        </div>
        <Link href={`/teams/${params.teamSlug}`} className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition">
          ← Team
        </Link>
      </div>

      <div className="glass rounded-3xl p-6 space-y-2">
        {team?.starters?.length ? (
          <ul className="divide-y divide-white/40">
            {team.starters.map((p: any) => (
              <li key={p.player_id} className="py-3">
                <Link href={`/players/${slugifyPlayer(p.player_name || p.full_name)}`} className="text-neutral-900 hover:underline">
                  {p.player_name || p.full_name}
                </Link>
              </li>
            ))}
          </ul>
        ) : (
          <p className="text-neutral-600">No starters listed.</p>
        )}
      </div>
    </main>
  );
}
