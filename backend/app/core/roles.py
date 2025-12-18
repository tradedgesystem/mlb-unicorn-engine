"""Role classification helpers for pitchers based on recent usage."""
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Dict, Mapping, Optional

from sqlalchemy import func, select

from backend.app.db import models

# Defaults can be overridden via environment for tuning.
DEFAULT_LOOKBACK_DAYS = int(os.getenv("PITCHER_ROLE_LOOKBACK_DAYS", "30"))
DEFAULT_STARTS_THRESHOLD = int(os.getenv("PITCHER_STARTS_THRESHOLD", "1"))
DEFAULT_STARTS_RATIO_THRESHOLD = float(os.getenv("PITCHER_STARTS_RATIO_THRESHOLD", "0.5"))


def _window(as_of_date: date, lookback_days: int) -> tuple[date, date]:
    start_date = as_of_date - timedelta(days=lookback_days)
    return start_date, as_of_date


def get_pitcher_usage_counts(
    session, as_of_date: Optional[date] = None, lookback_days: Optional[int] = None
) -> Dict[int, Dict[str, int]]:
    """Return per-pitcher starts/apps counts in a single query over the window."""
    as_of = as_of_date or date.today()
    lookback = lookback_days if lookback_days is not None else DEFAULT_LOOKBACK_DAYS
    start_date, end_date = _window(as_of, lookback)

    # Limit PAs to window and compute first PA per game+half to identify starters.
    pas = (
        select(
            models.PlateAppearance.game_id,
            models.PlateAppearance.pitcher_id,
            models.PlateAppearance.top_bottom,
            models.PlateAppearance.inning,
            models.PlateAppearance.pa_id,
        )
        .join(models.Game, models.Game.game_id == models.PlateAppearance.game_id)
        .where(
            models.Game.game_date >= start_date,
            models.Game.game_date <= end_date,
            models.PlateAppearance.pitcher_id != None,  # noqa: E711
        )
    ).cte("pas")

    first_pa = (
        select(
            pas.c.pitcher_id,
            func.row_number()
            .over(
                partition_by=[pas.c.game_id, pas.c.top_bottom],
                order_by=[pas.c.inning.asc(), pas.c.pa_id.asc()],
            )
            .label("rn"),
        )
        .select_from(pas)
    ).cte("first_pa")

    starters = (
        select(first_pa.c.pitcher_id, func.count().label("starts"))
        .where(first_pa.c.rn == 1)
        .group_by(first_pa.c.pitcher_id)
        .cte("starters")
    )

    apps = (
        select(pas.c.pitcher_id, func.count(func.distinct(pas.c.game_id)).label("apps"))
        .group_by(pas.c.pitcher_id)
        .cte("apps")
    )

    results = session.execute(
        select(
            apps.c.pitcher_id,
            apps.c.apps,
            func.coalesce(starters.c.starts, 0).label("starts"),
        ).select_from(apps.outerjoin(starters, starters.c.pitcher_id == apps.c.pitcher_id))
    )

    counts: Dict[int, Dict[str, int]] = {}
    for row in results:
        if row.pitcher_id is None:
            continue
        counts[int(row.pitcher_id)] = {
            "starts": int(row.starts or 0),
            "apps": int(row.apps or 0),
        }
    return counts


def classify_pitcher_role(
    starts: Optional[int],
    apps: Optional[int],
    *,
    starts_threshold: Optional[int] = None,
    starts_ratio_threshold: Optional[float] = None,
) -> str:
    """Return starter/reliever using configurable thresholds."""
    threshold = starts_threshold if starts_threshold is not None else DEFAULT_STARTS_THRESHOLD
    ratio_threshold = (
        starts_ratio_threshold if starts_ratio_threshold is not None else DEFAULT_STARTS_RATIO_THRESHOLD
    )
    starts_val = int(starts or 0)
    apps_val = int(apps or 0)
    if starts_val <= 0:
        return "reliever"
    if apps_val <= 0:
        return "starter" if starts_val >= threshold else "reliever"
    if starts_val >= threshold and (starts_val / apps_val) >= ratio_threshold:
        return "starter"
    return "reliever"
