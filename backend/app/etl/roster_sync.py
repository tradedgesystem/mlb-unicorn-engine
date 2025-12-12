"""Utility to force-sync player team assignments from MLB official rosters."""

from __future__ import annotations

import argparse
from datetime import date

from backend.app.core.logging import logger
from backend.app.etl.backfill import _sync_official_rosters


def sync_range(start_date: date, end_date: date) -> None:
    current = start_date
    while current <= end_date:
        _sync_official_rosters(current)
        current = date.fromordinal(current.toordinal() + 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync player teams from MLB official rosters")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="Single date (YYYY-MM-DD) to sync")
    group.add_argument("--start", help="Start date (YYYY-MM-DD) for range sync")
    parser.add_argument("--end", help="End date (YYYY-MM-DD) required when using --start")
    args = parser.parse_args()

    if args.date:
        run_date = date.fromisoformat(args.date)
        _sync_official_rosters(run_date)
        logger.info("Roster sync complete for %s", run_date)
        return

    if not args.end:
        raise SystemExit("--end is required when using --start")
    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    sync_range(start_date, end_date)
    logger.info("Roster sync complete for range %s to %s", start_date, end_date)


if __name__ == "__main__":
    main()
