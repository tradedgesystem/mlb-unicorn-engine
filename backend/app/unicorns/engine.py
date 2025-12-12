"""Unicorn evaluation engine (patterns -> SQL -> scores -> unicorn_results)."""
from __future__ import annotations

import argparse
import os
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
from backend.app.unicorns.scoring import ScoredRow, compute_scores
from backend.app.unicorns.sql_builder import build_query

MAX_PER_PATTERN_PER_DAY = 5
MIN_REL_GAP = 0.011  # 1.1% spacing for final Top 50
TOP50_LOOKBACK_DAYS = int(os.getenv("TOP50_LOOKBACK_DAYS", "7"))
TOP50_MAX_APPEARANCES_LOOKBACK = int(os.getenv("TOP50_MAX_APPEARANCES_LOOKBACK", "3"))
TOP10_MAX_APPEARANCES_LOOKBACK = int(os.getenv("TOP10_MAX_APPEARANCES_LOOKBACK", "2"))

def _render_description(template: str, player_name: str | None, team_name: str | None, metric_value: float | None) -> str:
    if not template:
        return ""
    description = template
    replacements = {
        "player_name": player_name or "",
        "team_name": team_name or "",
        "metric_value": f"{metric_value:.3f}" if metric_value is not None else "",
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
    recent_top50: Dict[int, int],
    recent_top10: Dict[int, int],
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
        prior_top50 = recent_top50.get(result.entity_id, 0)
        if prior_top50 >= TOP50_MAX_APPEARANCES_LOOKBACK:
            continue
        # prospective rank is current length + 1
        prospective_rank = len(top) + 1
        if prospective_rank <= 10 and recent_top10.get(result.entity_id, 0) >= TOP10_MAX_APPEARANCES_LOOKBACK:
            continue

        rank = len(top) + 1
        description = _render_description(template, player_name, team_name, float(result.metric_value))
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
    lookback_start = run_date - timedelta(days=TOP50_LOOKBACK_DAYS)
    historical = (
        session.query(models.UnicornTop50Daily.entity_id, models.UnicornTop50Daily.rank)
        .filter(models.UnicornTop50Daily.run_date >= lookback_start)
        .filter(models.UnicornTop50Daily.run_date < run_date)
        .all()
    )
    recent_top50: Dict[int, int] = defaultdict(int)
    recent_top10: Dict[int, int] = defaultdict(int)
    for pid, rank in historical:
        recent_top50[int(pid)] += 1
        if rank <= 10:
            recent_top10[int(pid)] += 1

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
        .order_by(models.UnicornResult.score.desc())
    )
    rows = session.execute(stmt).all()
    top = _select_top50(rows, run_date, recent_top50, recent_top10)
    apply_min_score_spacing(top, min_rel_gap=MIN_REL_GAP)

    session.execute(sa.delete(models.UnicornTop50Daily).where(models.UnicornTop50Daily.run_date == run_date))
    if top:
        session.bulk_save_objects(top)
    logger.info("Generated top50 for %s (rows=%s)", run_date, len(top))


def _load_market_weights(session: Session, season_year: int | None = None) -> Dict[int, float]:
    stmt = (
        sa.select(models.Player.player_id, models.TeamMarketContext.market_weight_adj)
        .join(
            models.TeamMarketContext,
            models.TeamMarketContext.team_id == models.Player.current_team_id,
            isouter=True,
        )
    )
    if season_year is not None:
        stmt = stmt.where(models.TeamMarketContext.season_year == season_year)

    weights: Dict[int, float] = {}
    for player_id, weight in session.execute(stmt):
        weights[int(player_id)] = float(weight) if weight is not None else 1.0
    return weights


def _market_weight_lookup(weights: Dict[int, float]):
    def lookup(player_id: int) -> float:
        return weights.get(player_id, 1.0)

    return lookup


def _persist_results(
    session: Session,
    run_date: date,
    pattern: models.PatternTemplate,
    scored_rows: Iterable[ScoredRow],
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
            z_raw=row.z_raw,
            z_adjusted=row.z_adjusted,
            score=row.score,
        )
        for row in scored_rows
    ]
    if objects:
        session.bulk_save_objects(objects)


def evaluate_pattern(
    session: Session,
    pattern: models.PatternTemplate,
    run_date: date,
    market_lookup,
) -> List[ScoredRow]:
    query, params = build_query(pattern, run_date)
    result = session.execute(text(query), params)
    rows = [dict(r._mapping) for r in result]
    if not rows:
        return []
    scored = compute_scores(pattern, rows, market_lookup)
    _persist_results(session, run_date, pattern, scored)
    logger.info("Evaluated pattern %s (%s rows)", pattern.pattern_id, len(scored))
    return scored


def run_for_date(run_date: date, season_year: int | None = None) -> None:
    with SessionLocal() as session:
        patterns: List[models.PatternTemplate] = (
            session.query(models.PatternTemplate).filter_by(enabled=True).all()
        )
        weights = _load_market_weights(session, season_year)
        market_lookup = _market_weight_lookup(weights)

        for pattern in patterns:
            try:
                validate_pattern(pattern)
            except ValueError as exc:
                logger.warning("Skipping pattern %s: %s", pattern.pattern_id, exc)
                continue
            evaluate_pattern(session, pattern, run_date, market_lookup)

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
