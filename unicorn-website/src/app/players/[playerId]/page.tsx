import Link from "next/link";

type PlayerResponse = {
  player_id: number;
  player_name: string;
  team_id?: number;
  team_name?: string;
  role?: string;
  metrics?: Record<string, number | null>;
  two_way?: boolean;
  roles?: string[];
  hitter_metrics?: Record<string, number | null>;
  pitcher_metrics?: Record<string, number | null>;
  recent_unicorns?: {
    run_date: string;
    pattern_id: string;
    description: string;
    metric_value: number;
    score: number;
  }[];
};

type LeagueAveragesResponse = {
  role: string;
  as_of_date: string;
  metrics: Record<string, number | null>;
};

const METRIC_KEYS: Record<string, { key: string; label: string }[]> = {
  hitter: [
    { key: "barrel_pct_last_50", label: "Barrel %" },
    { key: "hard_hit_pct_last_50", label: "Hard-Hit %" },
    { key: "xwoba_last_50", label: "xwOBA" },
    { key: "contact_pct_last_50", label: "Contact %" },
    { key: "chase_pct_last_50", label: "Chase %" },
  ],
  starter: [
    { key: "xwoba_last_3_starts", label: "xwOBA Allowed" },
    { key: "whiff_pct_last_3_starts", label: "Whiff %" },
    { key: "k_pct_last_3_starts", label: "K %" },
    { key: "bb_pct_last_3_starts", label: "BB %" },
    { key: "hard_hit_pct_last_3_starts", label: "Hard-Hit % Allowed" },
  ],
  reliever: [
    { key: "xwoba_last_5_apps", label: "xwOBA Allowed" },
    { key: "whiff_pct_last_5_apps", label: "Whiff %" },
    { key: "k_pct_last_5_apps", label: "K %" },
    { key: "bb_pct_last_5_apps", label: "BB %" },
    { key: "hard_hit_pct_last_5_apps", label: "Hard-Hit % Allowed" },
  ],
};

export default async function PlayerPage({
  params,
  searchParams,
}: {
  params: { playerId: string } | Promise<{ playerId: string }>;
  searchParams?: Record<string, string | string[] | undefined> | Promise<
    Record<string, string | string[] | undefined>
  >;
}) {
  const resolvedParams = (await Promise.resolve(params as unknown)) as { playerId?: unknown };
  const resolvedSearch = (await Promise.resolve(searchParams as unknown)) as
    | Record<string, string | string[] | undefined>
    | undefined;
  const rawFromParams = Array.isArray(resolvedParams?.playerId)
    ? resolvedParams.playerId[0]
    : resolvedParams?.playerId;
  const playerIdNum = Number(rawFromParams);
  const debugRequested =
    (Array.isArray(resolvedSearch?.debug) ? resolvedSearch?.debug[0] : resolvedSearch?.debug) ===
    "1";
  const debug = debugRequested && process.env.NODE_ENV !== "production";
  const base = process.env.NEXT_PUBLIC_API_BASE ?? "";
  const invalidId = !Number.isFinite(playerIdNum);

  const url = base && Number.isFinite(playerIdNum)
    ? new URL(`/api/players/${playerIdNum}`, base).toString()
    : "";
  let data: PlayerResponse | null = null;
  let league: LeagueAveragesResponse | null = null;
  let leagueHitter: LeagueAveragesResponse | null = null;
  let leagueStarter: LeagueAveragesResponse | null = null;
  let error: string | null = null;
  let status: number | null = null;

  if (invalidId) {
    error = `Invalid player id: ${String(rawFromParams)}`;
  } else if (!base) {
    error = "Missing NEXT_PUBLIC_API_BASE";
  } else {
    try {
      const resp = await fetch(url, { cache: "no-store" });
      status = resp.status;
      if (!resp.ok) {
        error = `Unable to load player data (status ${resp.status})`;
      } else {
        data = await resp.json();
      }
    } catch (err) {
      console.error("Player fetch failed", err);
      error = "Unable to load player data.";
    }
  }

  if (!error && base && data) {
    try {
      if (data.two_way) {
        const leagueHitterUrl = new URL("/api/league-averages?role=hitter", base).toString();
        const leagueStarterUrl = new URL("/api/league-averages?role=starter", base).toString();
        const [hitterResp, starterResp] = await Promise.all([
          fetch(leagueHitterUrl, { cache: "no-store" }),
          fetch(leagueStarterUrl, { cache: "no-store" }),
        ]);
        if (hitterResp.ok) leagueHitter = await hitterResp.json();
        if (starterResp.ok) leagueStarter = await starterResp.json();
      } else if (data.role) {
        const role = data.role.toLowerCase();
        const leagueUrl = new URL(`/api/league-averages?role=${encodeURIComponent(role)}`, base).toString();
        const resp = await fetch(leagueUrl, { cache: "no-store" });
        if (resp.ok) {
          league = await resp.json();
        }
      }
    } catch {
      // Non-fatal: omit league averages when unavailable.
    }
  }

  const isTwoWay = data?.two_way === true;
  const roleKey = (data?.role || "hitter").toLowerCase();
  const configs = METRIC_KEYS[roleKey] || METRIC_KEYS.hitter;
  const metrics = configs.map((cfg) => ({
    ...cfg,
    value: data?.metrics?.[cfg.key],
    leagueAvg: league?.metrics?.[cfg.key],
  }));

  const hasMetrics = metrics.some((m) => m.value !== null && m.value !== undefined);
  const showLeagueAvg = Boolean(league && league.metrics);
  const showLeagueAvgHitter = Boolean(leagueHitter && leagueHitter.metrics);
  const showLeagueAvgStarter = Boolean(leagueStarter && leagueStarter.metrics);

  const debugPanel = debug ? (
    <div className="rounded-xl border border-dashed border-neutral-300 bg-white/70 p-3 text-xs text-neutral-700 space-y-1">
      <p>
        <strong>Resolved params:</strong> {JSON.stringify(resolvedParams)}
      </p>
      <p>
        <strong>Resolved search:</strong> {JSON.stringify(resolvedSearch)}
      </p>
      <p>
        <strong>Raw param:</strong> {String(rawFromParams)}
      </p>
      <p>
        <strong>Parsed playerIdNum:</strong> {Number.isFinite(playerIdNum) ? playerIdNum : "NaN"}
      </p>
      <p>
        <strong>API Base:</strong> {base || "(empty)"}
      </p>
      <p>
        <strong>Fetch URL:</strong> {url || "(not fired)"}
      </p>
      <p>
        <strong>HTTP status:</strong> {status ?? "(unknown)"}
      </p>
      <p>
        <strong>Metric keys:</strong> {Object.keys(data?.metrics || {}).join(", ") || "(none)"}
      </p>
    </div>
  ) : null;

  const hittingMetrics = METRIC_KEYS.hitter.map((cfg) => ({
    ...cfg,
    value: data?.hitter_metrics?.[cfg.key],
    leagueAvg: leagueHitter?.metrics?.[cfg.key],
  }));
  const pitchingMetrics = METRIC_KEYS.starter.map((cfg) => ({
    ...cfg,
    value: data?.pitcher_metrics?.[cfg.key],
    leagueAvg: leagueStarter?.metrics?.[cfg.key],
  }));
  const hasHittingMetrics = hittingMetrics.some((m) => m.value !== null && m.value !== undefined);
  const hasPitchingMetrics = pitchingMetrics.some((m) => m.value !== null && m.value !== undefined);

  return (
    <main className="min-h-screen px-4 py-8 sm:px-8 lg:px-16 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-neutral-500">{data?.team_name || "Player"}</p>
          <h1 className="text-3xl font-semibold text-neutral-900">
            {data?.player_name || `Player ${Number.isFinite(playerIdNum) ? playerIdNum : ""}`}
          </h1>
          {isTwoWay ? (
            <span className="text-sm text-neutral-600 uppercase tracking-wide">TWO-WAY</span>
          ) : (
            data?.role && (
            <span className="text-sm text-neutral-600 uppercase tracking-wide">{data.role}</span>
            )
          )}
        </div>
        <div className="flex gap-2">
          <Link
            href="/"
            className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition"
          >
            Top 50
          </Link>
          <Link
            href="/teams"
            className="glass rounded-full px-4 py-2 text-sm text-neutral-800 hover:-translate-y-0.5 transition"
          >
            Teams
          </Link>
        </div>
      </div>

      {isTwoWay ? (
        <>
          <div className="glass rounded-3xl p-6 space-y-4">
            <h2 className="text-xl font-semibold text-neutral-900">Hitting Metrics</h2>
            {error && <p className="text-red-600">{error}</p>}
            {!error && !hasHittingMetrics && (
              <p className="text-neutral-600">No predictive metrics available yet</p>
            )}
            {debugPanel}
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {hittingMetrics.map((m) => (
                <div key={m.key} className="glass rounded-2xl p-4 shadow-sm">
                  <p className="text-xs uppercase text-neutral-500">{m.label}</p>
                  <p className="text-lg font-semibold text-neutral-900">
                    {m.value !== null && m.value !== undefined
                      ? Number(m.value).toFixed(3)
                      : "—"}
                  </p>
                  {showLeagueAvgHitter && (
                    <p className="text-xs text-neutral-500">
                      League avg:{" "}
                      {m.leagueAvg !== null && m.leagueAvg !== undefined
                        ? Number(m.leagueAvg).toFixed(3)
                        : "—"}
                    </p>
                  )}
                </div>
              ))}
            </div>
          </div>

          <div className="glass rounded-3xl p-6 space-y-4">
            <h2 className="text-xl font-semibold text-neutral-900">Pitching Metrics</h2>
            {error && <p className="text-red-600">{error}</p>}
            {!error && !hasPitchingMetrics && (
              <p className="text-neutral-600">No predictive metrics available yet</p>
            )}
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {pitchingMetrics.map((m) => (
                <div key={m.key} className="glass rounded-2xl p-4 shadow-sm">
                  <p className="text-xs uppercase text-neutral-500">{m.label}</p>
                  <p className="text-lg font-semibold text-neutral-900">
                    {m.value !== null && m.value !== undefined
                      ? Number(m.value).toFixed(3)
                      : "—"}
                  </p>
                  {showLeagueAvgStarter && (
                    <p className="text-xs text-neutral-500">
                      League avg:{" "}
                      {m.leagueAvg !== null && m.leagueAvg !== undefined
                        ? Number(m.leagueAvg).toFixed(3)
                        : "—"}
                    </p>
                  )}
                </div>
              ))}
            </div>
          </div>
        </>
      ) : (
        <div className="glass rounded-3xl p-6 space-y-4">
          <h2 className="text-xl font-semibold text-neutral-900">Predictive Metrics</h2>
          {error && <p className="text-red-600">{error}</p>}
          {!error && !hasMetrics && <p className="text-neutral-600">Metrics unavailable.</p>}
          {debugPanel}
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {metrics.map((m) => (
              <div key={m.key} className="glass rounded-2xl p-4 shadow-sm">
                <p className="text-xs uppercase text-neutral-500">{m.label}</p>
                <p className="text-lg font-semibold text-neutral-900">
                  {m.value !== null && m.value !== undefined
                    ? Number(m.value).toFixed(3)
                    : "—"}
                </p>
                {showLeagueAvg && (
                  <p className="text-xs text-neutral-500">
                    League avg:{" "}
                    {m.leagueAvg !== null && m.leagueAvg !== undefined
                      ? Number(m.leagueAvg).toFixed(3)
                      : "—"}
                  </p>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="glass rounded-3xl p-6 space-y-3">
        <h2 className="text-xl font-semibold text-neutral-900">Recent Unicorns</h2>
        {data?.recent_unicorns?.length ? (
          <div className="space-y-3">
            {data.recent_unicorns.map((u) => (
              <div key={`${u.run_date}-${u.pattern_id}`} className="rounded-2xl bg-white/70 px-4 py-3">
                <div className="flex items-center justify-between text-sm text-neutral-600">
                  <span>{u.run_date}</span>
                  <span className="font-mono">{u.pattern_id}</span>
                </div>
                <p className="mt-1 text-neutral-900">{u.description}</p>
                <p className="text-sm text-neutral-600 mt-1">
                  Score {Number(u.score).toFixed(2)} · Metric {Number(u.metric_value).toFixed(3)}
                </p>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-neutral-600">No recent unicorns.</p>
        )}
      </div>
    </main>
  );
}
