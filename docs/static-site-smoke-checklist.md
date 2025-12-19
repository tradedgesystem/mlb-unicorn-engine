# Static Site Smoke Checklist (Parallel Deploy)

Fill in your deployed static base URL:

- Static site base: `https://<STATIC_DEPLOY_HOST>`

## Must-pass URLs

- Home: `https://<STATIC_DEPLOY_HOST>/`
- Team page: `https://<STATIC_DEPLOY_HOST>/teams/119/`
- Player page: `https://<STATIC_DEPLOY_HOST>/players/605141/`

## Expected behaviors

- **Loads without backend**: No calls to `/api/*` and no fetches to the backend domain; all data loads from same-origin `/data/latest/*`.
- **Last updated**: The “Last updated” value in the sidebar matches `GET /data/latest/meta.json` (`last_updated`).
- **Sidebar team links**: Team abbreviations render (30 items) and each links to `/teams/{teamId}/`.
- **Player search**
  - Typing performs a contains-match search against `GET /data/latest/players_index.json`.
  - Shows up to 10 results.
  - Clicking a result navigates to `/players/{playerId}/`.
- **Home page**
  - Renders links to all 30 teams from `GET /data/latest/teams.json`.
  - Each link navigates to `/teams/{teamId}/`.
- **Team page**
  - Loads roster from `GET /data/latest/teams/{teamId}.json`.
  - Tabs switch between hitters/starters/relievers without a full page reload.
  - Each player links to `/players/{playerId}/`.
- **Player page**
  - Loads player payload from `GET /data/latest/players/{playerId}.json`.
  - “Back to current team” uses `current_team_id` and links to `/teams/{teamId}/`.

## Missing/invalid JSON handling

- If any required JSON fails to load or parse, the page shows a clear “Unable to load …” message and does not crash.
