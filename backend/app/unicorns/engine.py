"""Unicorn evaluation engine (patterns -> SQL -> scores -> unicorn_results)."""
from __future__ import annotations

import argparse
import os
import random
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List
from collections import defaultdict

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.app.core.logging import logger
from backend.app.db import models
from backend.app.db.session import SessionLocal
from backend.app.unicorns.patterns import validate_pattern
from backend.app.unicorns.sql_builder import build_query

MAX_PER_PATTERN_PER_DAY = 1
MIN_REL_GAP = 0.011  # 1.1% spacing for final Top 50
TOP50_LOOKBACK_DAYS = int(os.getenv("TOP50_LOOKBACK_DAYS", "7"))
TOP50_MAX_APPEARANCES_LOOKBACK = int(os.getenv("TOP50_MAX_APPEARANCES_LOOKBACK", "3"))
TOP10_MAX_APPEARANCES_LOOKBACK = int(os.getenv("TOP10_MAX_APPEARANCES_LOOKBACK", "2"))


@dataclass(frozen=True)
class EvaluatedRow:
    entity_id: int
    metric_value: float
    sample_size: int
    score: float
    rank: int


def _format_metric_value(value: float | None) -> str:
    if value is None:
        return ""
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        return ""
    if abs(as_float - round(as_float)) < 1e-9:
        return str(int(round(as_float)))
    return f"{as_float:.3f}"


def _render_description(
    template: str,
    player_name: str | None,
    team_name: str | None,
    metric_value: float | None,
    sample_size: int | None = None,
) -> str:
    if not template:
        return ""
    description = template
    replacements = {
        "player_name": player_name or "",
        "team_name": team_name or "",
        "metric_value": _format_metric_value(metric_value),
        "sample_size": "" if sample_size is None else str(sample_size),
    }
    for key, val in replacements.items():
        description = description.replace(f"{{{{{key}}}}}", str(val))
    return description


def apply_min_score_spacing(rows, min_rel_gap: float = MIN_REL_GAP) -> None:
    """Adjust scores in-place to enforce a minimum relative gap without changing order."""
    if not rows:
        return
    rows[0].score = max(0.0, float(rows[0].score))
    for i in range(1, len(rows)):
        prev = max(0.0, float(rows[i - 1].score))
        cur = max(0.0, float(rows[i].score))
        max_allowed = prev * (1 - min_rel_gap)
        if cur > max_allowed:
            cur = max_allowed
        rows[i].score = max(0.0, cur)


def _select_top50(
    rows,
    run_date: date,
) -> List[models.UnicornTop50Daily]:
    seen_entities: set[int] = set()
    pattern_counts: Dict[str, int] = defaultdict(int)
    top: List[models.UnicornTop50Daily] = []

    for row in rows:
        result: models.UnicornResult = row[0]
        player_name = row[1]
        team_name = row[2]
        template = row[3]
        if result.entity_id in seen_entities:
            continue
        if pattern_counts[result.pattern_id] >= MAX_PER_PATTERN_PER_DAY:
            continue

        rank = len(top) + 1
        description = _render_description(
            template,
            player_name,
            team_name,
            float(result.metric_value) if result.metric_value is not None else None,
            sample_size=int(result.sample_size) if result.sample_size is not None else None,
        )
        top.append(
            models.UnicornTop50Daily(
                run_date=run_date,
                rank=rank,
                entity_type=result.entity_type,
                entity_id=result.entity_id,
                pattern_id=result.pattern_id,
                metric_value=result.metric_value,
                sample_size=result.sample_size,
                score=result.score,
                description=description,
            )
        )
        seen_entities.add(result.entity_id)
        pattern_counts[result.pattern_id] += 1
        if rank >= 50:
            break
    return top


def generate_top50(session: Session, run_date: date) -> None:
    stmt = (
        sa.select(
            models.UnicornResult,
            models.Player.full_name.label("player_name"),
            models.Team.team_name.label("team_name"),
            models.PatternTemplate.description_template.label("description_template"),
        )
        .join(models.Player, models.Player.player_id == models.UnicornResult.entity_id)
        .join(models.PatternTemplate, models.PatternTemplate.pattern_id == models.UnicornResult.pattern_id)
        .join(models.Team, models.Team.team_id == models.Player.current_team_id, isouter=True)
        .where(models.UnicornResult.run_date == run_date)
        .order_by(models.UnicornResult.pattern_id.asc(), models.UnicornResult.rank.asc())
    )
    rows = session.execute(stmt).all()

    by_pattern: Dict[str, list] = defaultdict(list)
    for row in rows:
        result = row[0]
        by_pattern[str(result.pattern_id)].append(row)

    pattern_ids = list(by_pattern.keys())
    random.Random(run_date.toordinal()).shuffle(pattern_ids)

    shuffled_rows = []
    for pid in pattern_ids:
        shuffled_rows.extend(by_pattern[pid])

    top = _select_top50(shuffled_rows, run_date)

    session.execute(sa.delete(models.UnicornTop50Daily).where(models.UnicornTop50Daily.run_date == run_date))
    if top:
        session.bulk_save_objects(top)
    logger.info("Generated top50 for %s (rows=%s)", run_date, len(top))


def _persist_results(
    session: Session,
    run_date: date,
    pattern: models.PatternTemplate,
    evaluated_rows: Iterable[EvaluatedRow],
) -> None:
    session.execute(
        sa.delete(models.UnicornResult).where(
            models.UnicornResult.run_date == run_date,
            models.UnicornResult.pattern_id == pattern.pattern_id,
        )
    )

    objects = [
        models.UnicornResult(
            run_date=run_date,
            pattern_id=pattern.pattern_id,
            entity_type=pattern.entity_type,
            entity_id=row.entity_id,
            rank=row.rank,
            metric_value=row.metric_value,
            sample_size=row.sample_size,
            z_raw=None,
            z_adjusted=None,
            score=row.score,
        )
        for row in evaluated_rows
    ]
    if objects:
        session.bulk_save_objects(objects)


def evaluate_pattern(
    session: Session,
    pattern: models.PatternTemplate,
    run_date: date,
) -> List[EvaluatedRow]:
    query, params = build_query(pattern, run_date)
    result = session.execute(text(query), params)
    raw_rows = [dict(r._mapping) for r in result]
    if not raw_rows:
        return []

    evaluated: list[EvaluatedRow] = []
    for idx, row in enumerate(raw_rows, start=1):
        mv = row.get("metric_value")
        sv = row.get("sample_size")
        if mv is None or sv is None:
            continue
        try:
            metric_value = float(mv)
            sample_size = int(sv)
        except (TypeError, ValueError):
            continue
        evaluated.append(
            EvaluatedRow(
                entity_id=int(row["entity_id"]),
                metric_value=metric_value,
                sample_size=sample_size,
                score=metric_value,
                rank=idx,
            )
        )

    if not evaluated:
        return []

    _persist_results(session, run_date, pattern, evaluated)
    logger.info("Evaluated pattern %s (%s rows)", pattern.pattern_id, len(evaluated))
    return evaluated


def run_for_date(run_date: date, season_year: int | None = None) -> None:
    with SessionLocal() as session:
        patterns: List[models.PatternTemplate] = (
            session.query(models.PatternTemplate).filter_by(enabled=True).all()
        )

        for pattern in patterns:
            try:
                validate_pattern(pattern)
            except ValueError as exc:
                logger.warning("Skipping pattern %s: %s", pattern.pattern_id, exc)
                continue
            evaluate_pattern(session, pattern, run_date)

        session.commit()
        generate_top50(session, run_date)
        session.commit()
        logger.info("Finished unicorn evaluation for %s", run_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run unicorn evaluation for a given date")
    parser.add_argument("--date", required=True, help="Run date YYYY-MM-DD")
    parser.add_argument("--season-year", type=int, default=None, help="Optional season year for market weights")
    args = parser.parse_args()
    run_for_date(date.fromisoformat(args.date), season_year=args.season_year)


if __name__ == "__main__":
    main()
