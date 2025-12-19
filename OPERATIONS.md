# Operations (Static-Only Site)

## Nightly data updates

- Workflow: `.github/workflows/site-data-product.yml`
- Schedule: daily (plus manual trigger)
- Output committed into this repo:
  - `unicorn-website/public/data/latest/` (current)
  - `unicorn-website/public/data/snapshots/YYYY-MM-DD/` (history)
- Retention: keeps the most recent 7 snapshot days and deletes older snapshot folders.

The static site deployment copies `unicorn-website/public/data/` into the deployed output so it is reachable at same-origin `/data/latest/...`.

## Daily wRC+ recalibration

Run this before publishing the data product each day:

```sh
./scripts/daily_site_update.sh
```

This recalibrates wRC+ from the current Statcast window and publishes the data
product into `unicorn-website/public/data/latest/` and `snapshots/`.

## Rollback (fast)

Goal: restore `latest/` from yesterday’s snapshot and redeploy.

1. Pick the snapshot date to roll back to (example `YYYY-MM-DD`):
   - `ls unicorn-website/public/data/snapshots`
2. Replace `latest/` with that snapshot:
   - `rm -rf unicorn-website/public/data/latest`
   - `cp -R unicorn-website/public/data/snapshots/YYYY-MM-DD unicorn-website/public/data/latest`
3. Commit and push, then redeploy (Vercel auto-deploy on push):
   - `git add unicorn-website/public/data/latest`
   - `git commit -m "rollback: data latest -> YYYY-MM-DD"`
   - `git push`

## Validate a deployment

- Use the smoke checklist: `docs/static-site-smoke-checklist.md`

## Static-only guardrails

CI includes a static-only check that fails if the runtime JS references backend URLs or Next.js `/api/*` routes:

- Script: `static-site/scripts/static-only-guard.mjs`
