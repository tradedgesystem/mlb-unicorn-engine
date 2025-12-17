# Parallel Static Site Deployment (JustHTML)

This repo now supports deploying a **second** frontend that serves only static files (HTML/CSS/JS + JSON) from `static-site/dist/`, while keeping the existing Next.js site unchanged.

## What gets deployed

- Static pages (no SSR, no API routes, no backend fetches)
  - `/` (unicorns list)
  - `/teams/`
  - `/teams/{teamId}/`
  - `/players/{playerId}/`
- Same-origin JSON data product (copied into output at build time)
  - `/data/latest/meta.json`
  - `/data/latest/unicorns.json`
  - `/data/latest/teams.json`
  - `/data/latest/teams/{teamId}.json`
  - `/data/latest/players/{playerId}.json`
  - `/data/latest/players_index.json`

## Vercel setup (recommended)

Create a **new Vercel project** (separate from the existing Next.js one):

- **Root Directory**: `static-site`
- **Framework Preset**: Other
- **Build Command**: `npm run build`
- **Output Directory**: `dist`

Notes:
- The build copies the committed data product from `unicorn-website/public/data/` into `static-site/dist/data/` so the app can fetch it from the same origin at `/data/latest/...`.
- The nightly workflow `Site Data Product (Nightly)` commits updated JSON once per day; that commit should trigger Vercel to redeploy the static site automatically.

## Local build

Requires the repo to already have `unicorn-website/public/data/latest/*` committed.

```bash
cd static-site
npm run build
```

