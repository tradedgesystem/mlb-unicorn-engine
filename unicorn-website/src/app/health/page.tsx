import { API_BASE } from '@/lib/apiBase';

async function fetchStatus(url: string) {
  try {
    const res = await fetch(url);
    return { url, status: res.status, ok: res.ok };
  } catch (error: any) {
    return { url, status: 0, ok: false, error: error?.message ?? 'error' };
  }
}

export default async function Page() {
  const apiBase = API_BASE ?? '';
  let teamsResult: any = {};
  let playerResult: any = {};

  try {
    const res = await fetch(`${apiBase}/api/teams`);
    teamsResult = { url: `${apiBase}/api/teams`, status: res.status, ok: res.ok };
    if (res.ok) {
      const teams = await res.json();
      let playerId: string | null = null;
      if (Array.isArray(teams) && teams.length > 0) {
        const firstTeam = teams[0];
        const hitters = firstTeam?.hitters ?? firstTeam?.players ?? [];
        if (Array.isArray(hitters) && hitters.length > 0) {
          playerId = hitters[0]?.id ?? hitters[0]?.playerId ?? null;
        }
      }
      if (playerId) {
        try {
          const playerRes = await fetch(`${apiBase}/api/players/${playerId}`);
          playerResult = {
            url: `${apiBase}/api/players/${playerId}`,
            status: playerRes.status,
            ok: playerRes.ok,
          };
        } catch (err: any) {
          playerResult = {
            url: `${apiBase}/api/players/${playerId}`,
            status: 0,
            ok: false,
            error: err?.message ?? 'error',
          };
        }
      } else {
        playerResult = {
          url: '',
          status: 0,
          ok: false,
          error: 'No player ID found',
        };
      }
    }
  } catch (err: any) {
    teamsResult = {
      url: `${apiBase}/api/teams`,
      status: 0,
      ok: false,
      error: err?.message ?? 'error',
    };
  }

  return (
    <div style={{ padding: '1rem' }}>
      <h1>Health Check</h1>
      <p>API Base: {apiBase}</p>
      <h2>Teams fetch</h2>
      <pre>{JSON.stringify(teamsResult, null, 2)}</pre>
      <h2>Player fetch</h2>
      <pre>{JSON.stringify(playerResult, null, 2)}</pre>
    </div>
  );
}
