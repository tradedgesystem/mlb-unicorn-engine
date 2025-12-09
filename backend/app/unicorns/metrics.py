"""Metric registry mapping logical metric names to SQL expressions."""
from __future__ import annotations

from typing import Dict

MetricRegistry = Dict[str, str]


METRIC_REGISTRY: MetricRegistry = {
    "count_hr": "SUM(CASE WHEN is_hr THEN 1 ELSE 0 END)",
    "hr_rate": "SUM(CASE WHEN is_hr THEN 1 ELSE 0 END)::float / COUNT(*)",
    "hard_hit_rate": "SUM(CASE WHEN is_hard_hit THEN 1 ELSE 0 END)::float / COUNT(*)",
    "count_barrels": "SUM(CASE WHEN is_barrel THEN 1 ELSE 0 END)",
    "avg_ev": "AVG(launch_speed)",
    "xwoba_avg": "AVG(xwoba)",
    "whiff_rate": "SUM(CASE WHEN result_pitch = 'swinging_strike' THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN result_pitch IN ('swinging_strike','foul','in_play') THEN 1 ELSE 0 END), 0)",
    "contact_rate": "SUM(CASE WHEN result_pitch = 'in_play' THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN result_pitch IN ('swinging_strike','foul','in_play') THEN 1 ELSE 0 END), 0)",
    "chase_rate": "SUM(CASE WHEN is_in_zone = FALSE AND result_pitch IN ('swinging_strike','foul','in_play') THEN 1 ELSE 0 END)::float / NULLIF(SUM(CASE WHEN is_in_zone = FALSE THEN 1 ELSE 0 END), 0)",
}


PUBLIC_WEIGHT_DEFAULTS = {
    "tier1": 1.2,
    "tier2": 1.0,
    "tier3": 0.8,
}


CATEGORY_PUBLIC_WEIGHT = {
    "A_BARRELS": PUBLIC_WEIGHT_DEFAULTS["tier1"],
    "B_DIRECTION": PUBLIC_WEIGHT_DEFAULTS["tier2"],
    "COUNT": PUBLIC_WEIGHT_DEFAULTS["tier1"],
    "STARTER": PUBLIC_WEIGHT_DEFAULTS["tier1"],
    "RELIEVER": PUBLIC_WEIGHT_DEFAULTS["tier1"],
    "FATIGUE": PUBLIC_WEIGHT_DEFAULTS["tier1"],
    "PARK": PUBLIC_WEIGHT_DEFAULTS["tier1"],
}


def get_metric_expr(metric: str, metric_expr: str | None = None) -> str:
    if metric_expr:
        return metric_expr
    if metric not in METRIC_REGISTRY:
        raise KeyError(f"Metric '{metric}' is not registered")
    return METRIC_REGISTRY[metric]


def public_weight_for_category(category: str | None) -> float:
    if not category:
        return PUBLIC_WEIGHT_DEFAULTS["tier2"]
    for key, weight in CATEGORY_PUBLIC_WEIGHT.items():
        if category.startswith(key):
            return weight
    return PUBLIC_WEIGHT_DEFAULTS["tier3"]
