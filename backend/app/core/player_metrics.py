"""Compute holy-grail predictive metrics and persist to player_summary."""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence

import requests

from sqlalchemy import and_, case, func, select

from backend.app.core.logging import logger
from backend.app.db import models
from backend.app.db.session import SessionLocal

SWING_RESULTS = {"swinging_strike", "swinging_strike_blocked", "foul", "foul_tip", "in_play", "hit_into_play"}
CONTACT_RESULTS = {"in_play", "hit_into_play"}

_POSITION_CACHE: Dict[int, Optional[str]] = {}


def _chunked(values: Sequence[int], size: int = 200) -> Iterable[Sequence[int]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def preload_primary_positions(player_ids: Sequence[int]) -> None:
    """Warm the in-process cache of primary positions from MLBAM people endpoint."""
    missing = [pid for pid in player_ids if pid not in _POSITION_CACHE]
    if not missing:
        return
    url = "https://statsapi.mlb.com/api/v1/people"
    for chunk in _chunked(missing, size=200):
        try:
            resp = requests.get(
                url,
                params={"personIds": ",".join(str(pid) for pid in chunk)},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            for person in data.get("people", []):
                pid = int(person.get("id"))
                abbrev = (
                    (person.get("primaryPosition") or {}).get("abbreviation")
                    if isinstance(person, dict)
                    else None
                )
                _POSITION_CACHE[pid] = abbrev
            # Ensure any missing IDs in response are still cached as None
            returned = {int(p.get("id")) for p in data.get("people", []) if isinstance(p, dict)}
            for pid in chunk:
                if pid not in returned:
                    _POSITION_CACHE[pid] = None
        except Exception as exc:  # noqa: BLE001
            logger.warning("Primary position lookup failed for %s ids: %s", len(chunk), exc)
            for pid in chunk:
                _POSITION_CACHE[pid] = None


def _primary_position_abbrev(player_id: int) -> Optional[str]:
    if player_id not in _POSITION_CACHE:
        preload_primary_positions([player_id])
    return _POSITION_CACHE.get(player_id)


def _safe_div(numer: Optional[float], denom: Optional[float]) -> Optional[float]:
    try:
        if numer is None or denom is None or denom == 0:
            return None
        return float(numer) / float(denom)
    except Exception:
        return None


def _get_player_role(session, player_id: int) -> str:
    pos_abbrev = (_primary_position_abbrev(player_id) or "").upper()
    base_is_pitcher = pos_abbrev == "P"

    # If they actually pitched in the ingested window, classify as starter/reliever.
    pitcher_pitches = session.scalar(
        select(func.count()).select_from(models.PitchFact).where(models.PitchFact.pitcher_id == player_id)
    )
    if pitcher_pitches and pitcher_pitches > 0:
        # determine starter vs reliever by earliest pitch number threshold per game
        min_pitch_sub = (
            select(
                models.PitchFact.game_id,
                func.min(models.PitchFact.pitch_number_game).label("min_pitch"),
            )
            .where(models.PitchFact.pitcher_id == player_id)
            .group_by(models.PitchFact.game_id)
            .subquery()
        )
        starter_count = session.scalar(
            select(func.count()).select_from(min_pitch_sub).where(min_pitch_sub.c.min_pitch <= 15)
        )
        reliever_count = session.scalar(
            select(func.count()).select_from(min_pitch_sub).where(min_pitch_sub.c.min_pitch > 15)
        )
        if (starter_count or 0) >= (reliever_count or 0):
            return "starter"
        return "reliever"

    # Otherwise, fall back to stable primary position.
    if base_is_pitcher:
        # No starts observed -> treat as reliever by default.
        return "reliever"
    return "hitter"


def get_player_role(session, player_id: int) -> str:
    """Public wrapper so API routes can infer role."""
    return _get_player_role(session, player_id)


def _last_pa_ids(session, batter_id: int, limit: int = 50) -> List[int]:
    stmt = (
        select(models.PlateAppearance.pa_id)
        .join(models.Game, models.Game.game_id == models.PlateAppearance.game_id)
        .where(models.PlateAppearance.batter_id == batter_id)
        .order_by(models.Game.game_date.desc(), models.PlateAppearance.pa_id.desc())
        .limit(limit)
    )
    return [row.pa_id for row in session.execute(stmt)]


def _hitter_metrics(session, batter_id: int) -> dict:
    pa_ids = _last_pa_ids(session, batter_id, limit=50)
    if not pa_ids:
        return {}

    xwoba = session.scalar(
        select(func.avg(models.PlateAppearance.xwoba)).where(models.PlateAppearance.pa_id.in_(pa_ids))
    )

    pitch_query = select(
        func.count().label("total"),
        func.sum(case((models.PitchFact.is_barrel == True, 1), else_=0)).label("barrels"),
        func.sum(case((models.PitchFact.is_hard_hit == True, 1), else_=0)).label("hard_hits"),
        func.sum(case((models.PitchFact.result_pitch.in_(CONTACT_RESULTS), 1), else_=0)).label(
            "contact_swings"
        ),
        func.sum(case((models.PitchFact.result_pitch.in_(SWING_RESULTS), 1), else_=0)).label("swings"),
        func.sum(
            case(
                (
                    and_(models.PitchFact.is_in_zone == False, models.PitchFact.result_pitch.in_(SWING_RESULTS)),
                    1,
                ),
                else_=0,
            )
        ).label("swings_outside"),
        func.sum(case((models.PitchFact.is_in_zone == False, 1), else_=0)).label("pitches_outside"),
        func.sum(case((models.PitchFact.launch_speed != None, 1), else_=0)).label("batted_balls"),
    ).where(models.PitchFact.pa_id.in_(pa_ids))

    stats = session.execute(pitch_query).one()
    batted_balls = stats.batted_balls or 0
    swings = stats.swings or 0
    pitches_outside = stats.pitches_outside or 0

    return {
        "barrel_pct_last_50": _safe_div(stats.barrels, batted_balls),
        "hard_hit_pct_last_50": _safe_div(stats.hard_hits, batted_balls),
        "xwoba_last_50": float(xwoba) if xwoba is not None else None,
        "contact_pct_last_50": _safe_div(stats.contact_swings, swings),
        "chase_pct_last_50": _safe_div(stats.swings_outside, pitches_outside),
    }


def _pitcher_game_sets(session, pitcher_id: int):
    min_pitch_sub = (
        select(
            models.PitchFact.game_id,
            func.min(models.PitchFact.pitch_number_game).label("min_pitch"),
        )
        .where(models.PitchFact.pitcher_id == pitcher_id)
        .group_by(models.PitchFact.game_id)
        .subquery()
    )

    starter_games = (
        select(min_pitch_sub.c.game_id, models.Game.game_date)
        .join(models.Game, models.Game.game_id == min_pitch_sub.c.game_id)
        .where(min_pitch_sub.c.min_pitch <= 15)
        .order_by(models.Game.game_date.desc())
    )
    reliever_games = (
        select(min_pitch_sub.c.game_id, models.Game.game_date)
        .join(models.Game, models.Game.game_id == min_pitch_sub.c.game_id)
        .where(min_pitch_sub.c.min_pitch > 15)
        .order_by(models.Game.game_date.desc())
    )
    return starter_games, reliever_games


def _pitcher_metrics_for_games(session, pitcher_id: int, game_ids: Sequence[int]) -> dict:
    if not game_ids:
        return {}

    pa_stmt = select(
        func.avg(models.PlateAppearance.xwoba).label("xwoba"),
        func.sum(case((models.PlateAppearance.result.ilike("%strikeout%"), 1), else_=0)).label("ks"),
        func.sum(case((models.PlateAppearance.result.ilike("%walk%"), 1), else_=0)).label("bbs"),
        func.count().label("bf"),
    ).where(
        models.PlateAppearance.pitcher_id == pitcher_id,
        models.PlateAppearance.game_id.in_(game_ids),
    )
    pa_stats = session.execute(pa_stmt).one()

    pitch_stmt = select(
        func.count().label("total"),
        func.sum(case((models.PitchFact.result_pitch.in_(SWING_RESULTS), 1), else_=0)).label("swings"),
        func.sum(case((models.PitchFact.result_pitch.like("swinging_strike%"), 1), else_=0)).label("whiffs"),
        func.sum(case((models.PitchFact.launch_speed != None, 1), else_=0)).label("batted_balls"),
        func.sum(case((models.PitchFact.is_hard_hit == True, 1), else_=0)).label("hard_hits"),
    ).where(
        models.PitchFact.pitcher_id == pitcher_id,
        models.PitchFact.game_id.in_(game_ids),
    )
    pitch_stats = session.execute(pitch_stmt).one()

    return {
        "xwoba": float(pa_stats.xwoba) if pa_stats.xwoba is not None else None,
        "whiff_pct": _safe_div(pitch_stats.whiffs, pitch_stats.swings),
        "k_pct": _safe_div(pa_stats.ks, pa_stats.bf),
        "bb_pct": _safe_div(pa_stats.bbs, pa_stats.bf),
        "hard_hit_pct": _safe_div(pitch_stats.hard_hits, pitch_stats.batted_balls),
    }


def _starter_metrics(session, pitcher_id: int) -> dict:
    starter_games_stmt, _ = _pitcher_game_sets(session, pitcher_id)
    starter_games = [row.game_id for row in session.execute(starter_games_stmt.limit(3))]
    stats = _pitcher_metrics_for_games(session, pitcher_id, starter_games)
    if not stats:
        return {}
    return {
        "xwoba_last_3_starts": stats["xwoba"],
        "whiff_pct_last_3_starts": stats["whiff_pct"],
        "k_pct_last_3_starts": stats["k_pct"],
        "bb_pct_last_3_starts": stats["bb_pct"],
        "hard_hit_pct_last_3_starts": stats["hard_hit_pct"],
    }


def _reliever_metrics(session, pitcher_id: int) -> dict:
    _, reliever_games_stmt = _pitcher_game_sets(session, pitcher_id)
    reliever_games = [row.game_id for row in session.execute(reliever_games_stmt.limit(5))]
    stats = _pitcher_metrics_for_games(session, pitcher_id, reliever_games)
    if not stats:
        return {}
    return {
        "xwoba_last_5_apps": stats["xwoba"],
        "whiff_pct_last_5_apps": stats["whiff_pct"],
        "k_pct_last_5_apps": stats["k_pct"],
        "bb_pct_last_5_apps": stats["bb_pct"],
        "hard_hit_pct_last_5_apps": stats["hard_hit_pct"],
    }


def compute_player_summary(session, player_id: int) -> None:
    role = _get_player_role(session, player_id)
    metrics: dict = {}
    if role == "hitter":
        metrics.update(_hitter_metrics(session, player_id))
    elif role == "starter":
        metrics.update(_starter_metrics(session, player_id))
    elif role == "reliever":
        metrics.update(_reliever_metrics(session, player_id))

    stmt = (
        select(models.PlayerSummary)
        .where(models.PlayerSummary.player_id == player_id)
        .limit(1)
    )
    existing = session.execute(stmt).scalar_one_or_none()
    if existing is None:
        existing = models.PlayerSummary(player_id=player_id, role=role)
        session.add(existing)
    existing.role = role
    for key, value in metrics.items():
        setattr(existing, key, value)


def update_all() -> None:
    with SessionLocal() as session:
        player_ids = [row.player_id for row in session.execute(select(models.Player.player_id))]
        preload_primary_positions(player_ids)
        logger.info("Computing metrics for %s players", len(player_ids))
        for idx, pid in enumerate(player_ids, start=1):
            compute_player_summary(session, pid)
            if idx % 200 == 0:
                session.commit()
        session.commit()
        logger.info("Finished player_summary refresh")


def main() -> None:
    update_all()


if __name__ == "__main__":
    main()
