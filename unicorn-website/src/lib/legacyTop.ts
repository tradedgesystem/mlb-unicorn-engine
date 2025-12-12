import axios from "axios";
import { API_BASE } from "./apiBase";

export type UnicornRow = {
  run_date: string;
  rank: number;
  entity_type: string;
  entity_id: number;
  pattern_id: string;
  metric_value: number;
  sample_size: number;
  score: number;
  description: string;
};

export type Player = { id: number; full_name: string };
export type PlayerMap = Record<number, string>;

export async function fetchTop50(date: string): Promise<UnicornRow[]> {
  const url = `${API_BASE}/top50/${date}`;
  // Render free-tier cold starts can exceed 15s; allow more headroom.
  const res = await axios.get<UnicornRow[]>(url, { timeout: 60000 });
  return res.data;
}

export async function fetchPlayers(): Promise<PlayerMap> {
  const url = `${API_BASE}/players`;
  const res = await axios.get<Player[] | Record<string, string>>(url, {
    timeout: 60000,
  });
  const map: PlayerMap = {};
  if (Array.isArray(res.data)) {
    res.data.forEach((p) => {
      map[p.id] = p.full_name || String(p.id);
    });
  } else if (res.data && typeof res.data === "object") {
    Object.entries(res.data).forEach(([id, name]) => {
      const numId = Number(id);
      if (!Number.isNaN(numId)) {
        map[numId] = name || id;
      }
    });
  }
  return map;
}
