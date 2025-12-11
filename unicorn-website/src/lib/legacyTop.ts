import axios from "axios";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "https://mlb-unicorn-engine.onrender.com";

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
  const res = await axios.get<UnicornRow[]>(url, { timeout: 15000 });
  return res.data;
}

export async function fetchPlayers(): Promise<PlayerMap> {
  const url = `${API_BASE}/players`;
  const res = await axios.get<Player[]>(url, { timeout: 15000 });
  const map: PlayerMap = {};
  res.data.forEach((p) => {
    map[p.id] = p.full_name || String(p.id);
  });
  return map;
}
