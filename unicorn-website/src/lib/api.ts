import { API_BASE } from "./apiBase";

type FetchError = Error & { status?: number; host?: string };

const apiHost = (() => {
  try {
    return new URL(API_BASE).host;
  } catch {
    return API_BASE;
  }
})();

async function fetchJson(path: string) {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    const err: FetchError = new Error(`Request failed with ${res.status} from ${apiHost}`);
    err.status = res.status;
    err.host = apiHost;
    throw err;
  }
  return res.json();
}

export async function fetchTeams() {
  return fetchJson("/api/teams");
}

export async function fetchTeam(teamId: number) {
  return fetchJson(`/api/teams/${teamId}`);
}

export async function fetchPlayers() {
  return fetchJson("/players");
}

export async function fetchPlayerProfile(playerId: number) {
  return fetchJson(`/api/players/${playerId}`);
}
