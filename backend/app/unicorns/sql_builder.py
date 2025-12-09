"""SQL generator for pattern evaluation."""
from __future__ import annotations

from datetime import date
from textwrap import dedent
from typing import Dict, Tuple

from backend.db import models
from backend.app.unicorns.filters import build_filter_clause
from backend.app.unicorns.metrics import get_metric_expr

DEFAULT_LIMIT_PER_PATTERN = 500


def _normalize_order(order_direction: str) -> str:
    direction = (order_direction or "desc").lower()
    if direction not in {"asc", "desc"}:
        direction = "desc"
    return direction


def build_query(pattern: models.PatternTemplate, as_of_date: date) -> Tuple[str, Dict[str, object]]:
    metric_expr = get_metric_expr(pattern.metric, pattern.metric_expr)
    group_by = "batter_id" if pattern.entity_type == "batter" else "pitcher_id"
    where_sql, params = build_filter_clause(pattern.filters_json)
    params.update({"as_of_date": as_of_date, "min_sample": pattern.min_sample or 0})

    query = dedent(
        f"""
        SELECT {group_by} AS entity_id,
               {metric_expr} AS metric_value,
               COUNT(*) AS sample_size
        FROM {pattern.base_table}
        JOIN games USING (game_id)
        WHERE games.game_date <= :as_of_date{where_sql}
        GROUP BY {group_by}
        HAVING COUNT(*) >= :min_sample
        ORDER BY metric_value {_normalize_order(pattern.order_direction)}
        LIMIT {DEFAULT_LIMIT_PER_PATTERN}
        """
    )
    return query, params
