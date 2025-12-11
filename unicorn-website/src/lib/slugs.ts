export function slugifyPlayer(name: string): string {
  return name.toLowerCase().replace(/\s+/g, "-");
}

export function slugifyTeam(name: string): string {
  return name.toLowerCase().replace(/\s+/g, "-");
}

export function reverseSlugToName(slug: string): string {
  return slug.replace(/-/g, " ");
}

function normalize(str: string): string {
  return str.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

export function getPlayerIdFromSlug(slug: string, players: { id: number; full_name: string }[]): number | null {
  const target = normalize(slug);
  for (const p of players) {
    if (normalize(p.full_name) === target) {
      return p.id;
    }
  }
  return null;
}

export function getTeamIdFromSlug(slug: string, teams: { team_id: number; team_name: string }[]): number | null {
  const target = normalize(slug);
  for (const t of teams) {
    if (normalize(t.team_name) === target) {
      return t.team_id;
    }
  }
  return null;
}
