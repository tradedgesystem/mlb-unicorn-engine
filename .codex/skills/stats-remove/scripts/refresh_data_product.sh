#!/usr/bin/env bash
set -euo pipefail

# Regenerate latest + snapshot data product.
# Defaults to the deployed API to avoid local DB setup.

BASE_URL="${UNICORN_API_BASE_URL:-https://mlb-unicorn-engine.onrender.com}"
DATA_ROOT="${DATA_ROOT:-unicorn-website/public/data}"
WORKERS="${WORKERS:-4}"
SNAPSHOT_DATE="${SNAPSHOT_DATE:-}"

if [[ -n "${SNAPSHOT_DATE}" ]]; then
  python3 -m backend.app.tools.generate_site_data_product \
    --base-url "${BASE_URL}" \
    --data-root "${DATA_ROOT}" \
    --workers "${WORKERS}" \
    --snapshot-date "${SNAPSHOT_DATE}"
else
  python3 -m backend.app.tools.generate_site_data_product \
    --base-url "${BASE_URL}" \
    --data-root "${DATA_ROOT}" \
    --workers "${WORKERS}"
fi
