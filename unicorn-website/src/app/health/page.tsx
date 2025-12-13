"use client";

import { useEffect, useMemo, useState } from "react";

type CheckResult = {
  url: string;
  status: number | null;
  ok: boolean;
  error?: string;
};

async function ping(url: string): Promise<CheckResult> {
  try {
    const res = await fetch(url, { cache: "no-store" });
    return { url, status: res.status, ok: res.ok, error: res.ok ? undefined : res.statusText };
  } catch (err) {
    return { url, status: null, ok: false, error: err instanceof Error ? err.message : "Unknown error" };
  }
}

export default function HealthPage() {
  const [teamsCheck, setTeamsCheck] = useState<CheckResult | null>(null);
  const [playerCheck, setPlayerCheck] = useState<CheckResult | null>(null);
  const [playerId, setPlayerId] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const run = async () => {
      const teamsUrl = "/api/teams";
      const teamsRes = await ping(teamsUrl);
      setTeamsCheck(teamsRes);
      if (!teamsRes.ok) {
        setLoading(false);
        return;
      }

      try {
        const teamsData = await fetch(teamsUrl, { cache: "no-store" }).then((r) => r.json());
        const firstTeam = Array.isArray(teamsData) ? teamsData[0] : null;
        if (!firstTeam?.team_id) {
          throw new Error("No team id found");
        }
        const rosterUrl = `/api/teams/${firstTeam.team_id}`;
        const rosterRes = await fetch(rosterUrl, { cache: "no-store" });
        if (!rosterRes.ok) {
          throw new Error(`Roster load failed (${rosterRes.status})`);
        }
        const roster = await rosterRes.json();
        const candidate =
          roster?.hitters?.[0]?.player_id ||
          roster?.starters?.[0]?.player_id ||
          roster?.relievers?.[0]?.player_id;
        if (!candidate) {
          throw new Error("No player found in roster");
        }
        setPlayerId(candidate);
        setPlayerCheck(await ping(`/api/players/${candidate}`));
      } catch (err) {
        setPlayerCheck({
          url: "/api/players/{derived}",
          status: null,
          ok: false,
          error: err instanceof Error ? err.message : "Unknown error",
        });
      } finally {
        setLoading(false);
      }
    };
    run();
  }, []);

  const cards = useMemo(
    () => [
      {
        title: "Proxy",
        content: typeof window !== "undefined" ? window.location.origin : "(same origin)",
      },
      {
        title: "Teams endpoint",
        content: teamsCheck
          ? `${teamsCheck.url} → ${teamsCheck.status ?? "n/a"}${teamsCheck.error ? ` (${teamsCheck.error})` : ""}`
          : "Checking…",
        ok: teamsCheck?.ok,
      },
      {
        title: "Player endpoint",
        content: playerCheck
          ? `${playerCheck.url} → ${playerCheck.status ?? "n/a"}${playerCheck.error ? ` (${playerCheck.error})` : ""}`
          : "Waiting for teams…",
        ok: playerCheck?.ok,
        extra: playerId ? `player_id=${playerId}` : undefined,
      },
    ],
    [teamsCheck, playerCheck, playerId]
  );

  return (
    <main className="min-h-screen px-4 py-10 sm:px-8 lg:px-16 space-y-6">
      <div>
        <p className="text-sm text-neutral-500">MLB Unicorn Engine</p>
        <h1 className="text-3xl font-semibold text-neutral-900">Health</h1>
        <p className="text-neutral-600 mt-1">Checks direct calls to the Render API base.</p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {cards.map((card) => (
          <div
            key={card.title}
            className={`glass rounded-2xl p-4 shadow-sm border ${
              card.ok === false ? "border-red-300" : "border-white/40"
            }`}
          >
            <p className="text-xs uppercase text-neutral-500">{card.title}</p>
            <p className="mt-1 text-sm text-neutral-900 break-words">{card.content}</p>
            {card.extra && <p className="text-xs text-neutral-500 mt-1">{card.extra}</p>}
          </div>
        ))}
      </div>

      {loading && <p className="text-neutral-600">Running checks…</p>}
    </main>
  );
}
