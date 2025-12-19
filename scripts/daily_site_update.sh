#!/usr/bin/env bash
set -euo pipefail

# Daily pipeline: calibrate wRC+ on current Statcast window, then publish data product.

python3 scripts/calibrate_wrc_plus.py --min-pa 50

BASE_URL="${UNICORN_API_BASE_URL:-https://mlb-unicorn-engine.onrender.com}"
SNAPSHOT_DATE="${SNAPSHOT_DATE:-}"

if [[ -n "${SNAPSHOT_DATE}" ]]; then
  UNICORN_API_BASE_URL="${BASE_URL}" \
    python3 -m backend.app.tools.generate_site_data_product \
    --data-root unicorn-website/public/data \
    --workers 4 \
    --snapshot-date "${SNAPSHOT_DATE}"
else
  UNICORN_API_BASE_URL="${BASE_URL}" \
    python3 -m backend.app.tools.generate_site_data_product \
    --data-root unicorn-website/public/data \
    --workers 4
fi
