---
name: stats-remove
description: Remove batting/pitching stats from the MLB Unicorn Engine data product and UI. Use when a user asks to drop stats like OPS+/wRC+ from player JSONs, table headers, or front-end lists, and when you need to regenerate `unicorn-website/public/data/latest` after schema changes.
---

# Stats Remove

## Overview

Remove stat fields from the data product schema + UI lists, then regenerate the static JSON data product so all player files reflect the change.

## Workflow

1) Update the data product schema
2) Update the UI stat list
3) Regenerate the data product (ask before long runs)
4) Sanity-check a sample player JSON

### 1) Update the data product schema

Edit `backend/app/tools/generate_site_data_product.py`:
- Remove the stat keys from `_BATTING_STAT_SPECS` or `_PITCHING_STAT_SPECS`.
- Remove any calculations that populate those stats in `_fetch_statcast_batting_stats` or `_fetch_bref_*` helpers.

### 2) Update the UI stat list

Edit `unicorn-website/justhtml/assets/site.js`:
- Remove the stat from the `basic` stat list (labels like `OPS+`, `wRC+`).

### 3) Regenerate the data product

Use the script in `scripts/refresh_data_product.sh` (or run the generator directly).
This can take 10–15 minutes if hitting the remote API; ask before running.

### 4) Sanity-check

Check a known player JSON in `unicorn-website/public/data/latest/players/{playerId}.json`
to confirm the removed keys are gone.

## Resources

### scripts/
- `scripts/refresh_data_product.sh`: regenerate latest + snapshot data product.

### references/
- `references/paths.md`: key files + grep hints for stat removal.
