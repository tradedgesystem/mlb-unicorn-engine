"""SQL generator for pattern evaluation."""
from __future__ import annotations

from datetime import date, timedelta
from textwrap import dedent
from typing import Dict, Tuple

from backend.app.db import models
from backend.app.unicorns.filters import build_filter_clause
from backend.app.unicorns.metrics import get_metric_expr

DEFAULT_LIMIT_PER_PATTERN = 500


def _normalize_order(order_direction: str) -> str:
    direction = (order_direction or "desc").lower()
    if direction not in {"asc", "desc"}:
        direction = "desc"
    return direction


_NON_AB_RESULTS = (
    "hit_by_pitch",
    "sac_fly",
    "sac_bunt",
    "sac_fly_double_play",
    "sac_bunt_double_play",
    "catcher_interf",
)


def _windowed_from_clause(
    pattern: models.PatternTemplate,
    *,
    as_of_date: date,
    params: Dict[str, object],
) -> tuple[str, str]:
    """Return (with_sql, from_table) for windowed batter queries.

    Windowing is configured via `filters_json.window` (optional), e.g.:
      - {"type": "last_n_pa", "n": 50}
      - {"type": "last_n_ab", "n": 50}
    """
    filters_json = pattern.filters_json if isinstance(pattern.filters_json, dict) else {}
    window = filters_json.get("window")
    if not isinstance(window, dict):
        return "", pattern.base_table

    window_type = str(window.get("type") or "").strip().lower()
    if window_type not in {"last_n_pa", "last_n_ab"}:
        return "", pattern.base_table

    if pattern.entity_type != "batter":
        raise ValueError(f"Window '{window_type}' only supported for batter patterns")

    if pattern.base_table not in {"pitch_facts", "pa_facts"}:
        raise ValueError(f"Window '{window_type}' only supported for pitch_facts/pa_facts (got {pattern.base_table})")

    try:
        window_n = int(window.get("n") or 50)
    except (TypeError, ValueError):
        window_n = 50
    window_n = max(1, window_n)
    params["window_n"] = window_n

    extra_ab_filter = ""
    if window_type == "last_n_ab":
        extra_ab_filter = dedent(
            f"""
            AND COALESCE(pa.is_bb, FALSE) = FALSE
            AND (pa.result IS NULL OR pa.result NOT IN ({", ".join(repr(v) for v in _NON_AB_RESULTS)}))
            """
        ).strip()

    with_sql = dedent(
        f"""
        WITH ranked_pas AS (
            SELECT
                pa.pa_id AS pa_id,
                pa.batter_id AS batter_id,
                ROW_NUMBER() OVER (
                    PARTITION BY pa.batter_id
                    ORDER BY games.game_date DESC, pa.pa_id DESC
                ) AS rn
            FROM pa_facts pa
            JOIN games ON games.game_id = pa.game_id
            WHERE games.game_date <= :as_of_date
              AND pa.batter_id IS NOT NULL
              {extra_ab_filter}
        ),
        window_pas AS (
            SELECT pa_id, batter_id
            FROM ranked_pas
            WHERE rn <= :window_n
        ),
        windowed AS (
            SELECT base.*
            FROM {pattern.base_table} base
            JOIN window_pas
              ON window_pas.pa_id = base.pa_id
             AND window_pas.batter_id = base.batter_id
        )
        """
    ).strip()
    return with_sql, "windowed"


def build_query(pattern: models.PatternTemplate, as_of_date: date) -> Tuple[str, Dict[str, object]]:
    metric_expr = get_metric_expr(pattern.metric, pattern.metric_expr)
    group_by = "batter_id" if pattern.entity_type == "batter" else "pitcher_id"
    where_sql, params = build_filter_clause(pattern.filters_json)
    params.update({"as_of_date": as_of_date, "min_sample": pattern.min_sample or 0})

    filters_json = pattern.filters_json if isinstance(pattern.filters_json, dict) else {}
    window = filters_json.get("window") if isinstance(filters_json, dict) else None
    if isinstance(window, dict) and str(window.get("type") or "").strip().lower() == "last_n_days":
        try:
            window_n = int(window.get("n") or 7)
        except (TypeError, ValueError):
            window_n = 7
        window_n = max(1, window_n)
        params["window_start_date"] = as_of_date - timedelta(days=window_n - 1)
        where_sql += " AND games.game_date >= :window_start_date"

    sample_expr_raw = filters_json.get("sample_expr")
    sample_expr = sample_expr_raw if isinstance(sample_expr_raw, str) and sample_expr_raw.strip() else "COUNT(*)"

    order_expr_raw = filters_json.get("order_expr")
    order_expr = order_expr_raw if isinstance(order_expr_raw, str) and order_expr_raw.strip() else "metric_value"

    with_sql, from_table = _windowed_from_clause(pattern, as_of_date=as_of_date, params=params)

    query = dedent(
        f"""
        {with_sql}
        SELECT entity_id, metric_value, sample_size
        FROM (
            SELECT {group_by} AS entity_id,
                   {metric_expr} AS metric_value,
                   {sample_expr} AS sample_size
            FROM {from_table}
            JOIN games USING (game_id)
            WHERE games.game_date <= :as_of_date{where_sql}
            GROUP BY {group_by}
            HAVING {sample_expr} >= :min_sample
        ) aggregated
        ORDER BY {order_expr} {_normalize_order(pattern.order_direction)},
                 sample_size DESC,
                 entity_id ASC
        LIMIT {DEFAULT_LIMIT_PER_PATTERN}
        """
    )
    return query, params
