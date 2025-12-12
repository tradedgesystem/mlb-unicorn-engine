"""Scoring utilities for unicorn evaluation."""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Callable, Iterable, List, Sequence

from backend.app.db import models
from backend.app.unicorns.metrics import public_weight_for_category


@dataclass
class ScoredRow:
    entity_id: int
    metric_value: float
    sample_size: int
    z_raw: float
    z_adjusted: float
    score: float
    rank: int


def _rank_based_z(values: Sequence[float], descending: bool) -> List[float]:
    n = len(values)
    if n == 1:
        return [0.0]
    indexed = list(enumerate(values))
    indexed.sort(key=lambda x: x[1], reverse=descending)
    z_map = {}
    for idx, (orig_idx, _) in enumerate(indexed):
        z_map[orig_idx] = (n - 1 - idx) / (n - 1) if descending else idx / (n - 1)
    return [z_map[i] for i in range(n)]


def _rank_spread_z(values: Sequence[float], descending: bool) -> List[float]:
    """Assign wider-spread rank-based z_raw in [0,2], top at 2.0, bottom at 0.0."""
    n = len(values)
    if n == 1:
        return [2.0]
    indexed = list(enumerate(values))
    indexed.sort(key=lambda x: x[1], reverse=descending)
    denom = max(1, n - 1)
    z_map = {}
    for rank_idx, (orig_idx, _) in enumerate(indexed):
        z_map[orig_idx] = 2.0 * (1 - (rank_idx / denom))
    return [z_map[i] for i in range(n)]


def compute_scores(
    pattern: models.PatternTemplate,
    rows: Iterable[dict],
    market_weight_lookup: Callable[[int], float],
) -> List[ScoredRow]:
    """
    Compute normalized scores for a pattern.
    Skip rows where metric_value is None or cannot be converted to float.
    """
    cleaned_rows: List[dict] = []
    for r in rows:
        mv = r.get("metric_value")
        if mv is None:
            continue
        try:
            mv_float = float(mv)
        except (TypeError, ValueError):
            continue
        if math.isnan(mv_float):
            continue
        sample_size = r.get("sample_size", 0)
        try:
            sample_size_int = int(sample_size)
        except (TypeError, ValueError):
            sample_size_int = 0
        cleaned_rows.append({**r, "metric_value": mv_float, "sample_size": sample_size_int})

    if not cleaned_rows:
        return []

    metric_values = [r["metric_value"] for r in cleaned_rows]
    sample_sizes = [r["sample_size"] for r in cleaned_rows]
    descending = (pattern.order_direction or "desc").lower() == "desc"

    try:
        mean_val = statistics.mean(metric_values)
        std_val = statistics.stdev(metric_values)
    except statistics.StatisticsError:
        mean_val = 0.0
        std_val = 0.0

    if std_val <= 1e-9 or len(metric_values) < 5:
        z_values = _rank_spread_z(metric_values, descending=descending)
    else:
        z_values = [
            (val - mean_val) / std_val if descending else (mean_val - val) / std_val
            for val in metric_values
        ]

    target_sample = pattern.target_sample or 0
    unicorn_weight = float(pattern.unicorn_weight or 1.0)
    public_weight = float(pattern.public_weight or public_weight_for_category(pattern.category))

    scored: List[ScoredRow] = []
    for idx, row in enumerate(cleaned_rows):
        sample_size = sample_sizes[idx]
        sample_weight = 1.0
        if target_sample > 0:
            sample_weight = math.sqrt(sample_size / (sample_size + target_sample))

        z_raw = z_values[idx]
        z_adjusted = z_raw * sample_weight
        lookup_val = market_weight_lookup(int(row["entity_id"])) if callable(market_weight_lookup) else None
        market_weight = lookup_val if lookup_val is not None else 1.0
        score = z_adjusted * unicorn_weight * public_weight * market_weight
        scored.append(
            ScoredRow(
                entity_id=int(row["entity_id"]),
                metric_value=float(row["metric_value"]),
                sample_size=sample_size,
                z_raw=z_raw,
                z_adjusted=z_adjusted,
                score=score,
                rank=0,
            )
        )

    metric_sort_direction = -1 if descending else 1
    scored.sort(
        key=lambda r: (
            -r.score,
            -r.sample_size,
            metric_sort_direction * r.metric_value,
            r.entity_id,
        )
    )
    # Enforce a minimal 1% drop between adjacent scores to avoid plateaus.
    for idx in range(1, len(scored)):
        prev = scored[idx - 1].score
        cur = scored[idx].score
        if prev > 0 and (prev - cur) < 0.01 * prev:
            new_score = prev * 0.99
            if cur != 0:
                factor = new_score / cur
                scored[idx].z_adjusted *= factor
            scored[idx].score = new_score

    for i, r in enumerate(scored, start=1):
        r.rank = i
    return scored
