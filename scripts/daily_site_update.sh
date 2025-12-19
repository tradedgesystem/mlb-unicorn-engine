#!/usr/bin/env bash
set -euo pipefail

# Daily pipeline: calibrate wRC+ on current Statcast window, then publish data product.

python3 scripts/calibrate_wrc_plus.py --min-pa 50

UNICORN_API_BASE_URL="${UNICORN_API_BASE_URL:-https://mlb-unicorn-engine.onrender.com}" \
  python3 -m backend.app.tools.generate_site_data_product \
  --data-root unicorn-website/public/data \
  --workers 4
