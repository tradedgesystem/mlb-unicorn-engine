import { API_BASE } from "./apiBase";

export async function fetchTeams() {
  const res = await fetch(`${API_BASE}/api/teams`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to fetch teams");
  return res.json();
}

export async function fetchTeam(teamId: number) {
  const res = await fetch(`${API_BASE}/api/teams/${teamId}`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to fetch team");
  return res.json();
}

export async function fetchPlayers() {
  const res = await fetch(`${API_BASE}/players`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to fetch players");
  return res.json();
}

export async function fetchPlayerProfile(playerId: number) {
  const res = await fetch(`${API_BASE}/api/players/${playerId}`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to fetch player profile");
  return res.json();
}
