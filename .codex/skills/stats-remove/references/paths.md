# Stat Removal Pointers

## Key files
- Data product stat schema: `backend/app/tools/generate_site_data_product.py`
  - `_BATTING_STAT_SPECS`, `_PITCHING_STAT_SPECS`
  - `_fetch_statcast_batting_stats` (Statcast-derived batting stats)
  - `_fetch_bref_*` (Baseball-Reference batting/pitching stats)
- UI basic stats list: `unicorn-website/justhtml/assets/site.js`

## Fast search hints
- `rg -n "_BATTING_STAT_SPECS|_PITCHING_STAT_SPECS" backend/app/tools/generate_site_data_product.py`
- `rg -n "ops_plus|wrc_plus|OPS\+|wRC\+" backend/app/tools/generate_site_data_product.py unicorn-website/justhtml/assets/site.js`

## Regenerate data product
- `scripts/refresh_data_product.sh` (in this skill)
- Or run: `python3 -m backend.app.tools.generate_site_data_product --base-url https://mlb-unicorn-engine.onrender.com --data-root unicorn-website/public/data --workers 4`

## Sanity check
- Inspect a player file: `unicorn-website/public/data/latest/players/{playerId}.json`
