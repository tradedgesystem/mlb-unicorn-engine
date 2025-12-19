#!/usr/bin/env bash
set -euo pipefail

# Daily pipeline: calibrate wRC+ on current Statcast window, then publish data product.

TARGET_WRC_PLUS="${WRC_PLUS_TARGET_WRC_PLUS:-210}"
TARGET_PLAYER_ID="${WRC_PLUS_TARGET_PLAYER_ID:-}"
TARGET_FIRST="${WRC_PLUS_TARGET_FIRST:-Fernando}"
TARGET_LAST="${WRC_PLUS_TARGET_LAST:-Tatis}"

calibration_args=(
  python3 scripts/calibrate_wrc_plus.py
  --min-pa 50
  --use-target
  --target-wrc-plus "${TARGET_WRC_PLUS}"
  --target-first "${TARGET_FIRST}"
  --target-last "${TARGET_LAST}"
)
if [[ -n "${TARGET_PLAYER_ID}" ]]; then
  calibration_args+=(--target-player-id "${TARGET_PLAYER_ID}")
fi

"${calibration_args[@]}"

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
