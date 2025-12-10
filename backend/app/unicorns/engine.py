"""Unicorn evaluation engine (patterns -> SQL -> scores -> unicorn_results)."""
from __future__ import annotations

import argparse
from datetime import date
from typing import Dict, Iterable, List

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.app.core.logging import logger
from backend.app.db import models
from backend.app.db.session import SessionLocal
from backend.app.unicorns.patterns import validate_pattern
from backend.app.unicorns.scoring import ScoredRow, compute_scores
from backend.app.unicorns.sql_builder import build_query


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
        .order_by(models.UnicornResult.score.desc())
    )
    rows = session.execute(stmt).all()
    seen_entities: set[int] = set()
    top: List[models.UnicornTop50Daily] = []

    for row in rows:
        result: models.UnicornResult = row[0]
        player_name = row.player_name
        team_name = row.team_name
        template = row.description_template
        if result.entity_id in seen_entities:
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
        if rank >= 50:
            break

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
