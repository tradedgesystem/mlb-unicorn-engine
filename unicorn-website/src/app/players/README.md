This folder exposes the single dynamic player route at `/players/[playerId]`.

- Do not add additional dynamic segments (e.g. `[playerSlug]`) at this level, because Next.js would treat them as ambiguous siblings and break numeric `/players/{id}` routing in production.
- If a slug route is ever needed, place it under a separate path such as `/players/slug/[playerSlug]`.
