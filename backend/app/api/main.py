from datetime import date

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware

from backend.app.db import models
from backend.app.db.base import Base
from backend.app.db.session import SessionLocal, engine
from backend.app.unicorns.queries import fetch_top50_for_date
from backend.app.core.player_metrics import get_player_role, update_all as refresh_player_metrics
from backend.app.core.mlbam_people import (
    get_full_name,
    get_primary_position_abbrev,
    is_placeholder_name,
)
from backend.app.tools.audit_top50_quality import Top50Entry, audit_range, _role_from_player
from backend.app.unicorns.engine import apply_min_score_spacing, MIN_REL_GAP

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def init_db() -> None:
    # Ensure core tables exist (useful for fresh Render/Postgres instances).
    Base.metadata.create_all(bind=engine)
    _seed_sample_top50()


@app.get("/")
def root(response: Response):
    response.headers["Cache-Control"] = "public, max-age=120"
    return {"status": "ok", "message": "MLB Unicorn Engine API is running"}


def to_dict(row):
    return {
        "run_date": str(row.run_date),
        "rank": row.rank,
        "entity_type": row.entity_type,
        "entity_id": row.entity_id,
        "pattern_id": row.pattern_id,
        "metric_value": float(row.metric_value) if row.metric_value is not None else None,
        "sample_size": row.sample_size,
        "score": float(row.score) if row.score is not None else None,
        "description": row.description,
    }


@app.get("/top50/{run_date}")
def get_top50(run_date: date, response: Response):
    session = SessionLocal()
    try:
        rows = fetch_top50_for_date(session, run_date)
        apply_min_score_spacing(rows, min_rel_gap=MIN_REL_GAP)
        response.headers["Cache-Control"] = "public, max-age=300"
        return [to_dict(r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()


@app.get("/players")
def get_players(response: Response):
    session = SessionLocal()
    try:
        players = session.query(models.Player).all()
        response.headers["Cache-Control"] = "public, max-age=600"
        return [{"id": p.player_id, "full_name": p.full_name} for p in players]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()


@app.get("/api/players")
def get_players_api(response: Response):
    return get_players(response)


def _role_metrics(summary: models.PlayerSummary) -> dict:
    if summary is None:
        return {}
    role = summary.role
    if role == "hitter":
        return {
            "barrel_pct_last_50": summary.barrel_pct_last_50,
            "hard_hit_pct_last_50": summary.hard_hit_pct_last_50,
            "xwoba_last_50": summary.xwoba_last_50,
            "contact_pct_last_50": summary.contact_pct_last_50,
            "chase_pct_last_50": summary.chase_pct_last_50,
        }
    if role == "starter":
        return {
            "xwoba_last_3_starts": summary.xwoba_last_3_starts,
            "whiff_pct_last_3_starts": summary.whiff_pct_last_3_starts,
            "k_pct_last_3_starts": summary.k_pct_last_3_starts,
            "bb_pct_last_3_starts": summary.bb_pct_last_3_starts,
            "hard_hit_pct_last_3_starts": summary.hard_hit_pct_last_3_starts,
        }
    if role == "reliever":
        return {
            "xwoba_last_5_apps": summary.xwoba_last_5_apps,
            "whiff_pct_last_5_apps": summary.whiff_pct_last_5_apps,
            "k_pct_last_5_apps": summary.k_pct_last_5_apps,
            "bb_pct_last_5_apps": summary.bb_pct_last_5_apps,
            "hard_hit_pct_last_5_apps": summary.hard_hit_pct_last_5_apps,
        }
    return {}


@app.get("/players/{player_id}")
def get_player_profile(player_id: int, response: Response):
    session = SessionLocal()
    try:
        player = (
            session.query(models.Player, models.Team)
            .outerjoin(models.Team, models.Team.team_id == models.Player.current_team_id)
            .filter(models.Player.player_id == player_id)
            .first()
        )
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        summary = session.get(models.PlayerSummary, player_id)
        if summary is None:
            refresh_player_metrics()  # compute fresh summaries
            summary = session.get(models.PlayerSummary, player_id)

        metrics = _role_metrics(summary)
        unicorns = (
            session.query(models.UnicornTop50Daily)
            .filter(models.UnicornTop50Daily.entity_id == player_id)
            .order_by(models.UnicornTop50Daily.run_date.desc(), models.UnicornTop50Daily.rank.asc())
            .limit(5)
            .all()
        )

        response.headers["Cache-Control"] = "public, max-age=300"
        return {
            "player_id": player[0].player_id,
            "player_name": player[0].full_name,
            "team_id": player[1].team_id if player[1] else None,
            "team_name": player[1].team_name if player[1] else None,
            "role": summary.role if summary else None,
            "metrics": metrics,
            "recent_unicorns": [
                {
                    "run_date": str(u.run_date),
                    "pattern_id": u.pattern_id,
                    "description": u.description,
                    "metric_value": float(u.metric_value),
                    "score": float(u.score),
                }
                for u in unicorns
            ],
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()


@app.get("/api/players/{player_id}")
def get_player_profile_api(player_id: int, response: Response):
    return get_player_profile(player_id, response)


@app.get("/api/teams")
def list_teams(response: Response):
    session = SessionLocal()
    try:
        teams = session.query(models.Team).order_by(models.Team.team_name.asc()).all()
        response.headers["Cache-Control"] = "public, max-age=600"
        return [
            {"team_id": t.team_id, "team_name": t.team_name, "abbrev": t.abbrev}
            for t in teams
        ]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()


@app.get("/api/teams/{team_id}")
def get_team(team_id: int, response: Response):
    session = SessionLocal()
    try:
        team = session.get(models.Team, team_id)
        if not team:
            raise HTTPException(status_code=404, detail="Team not found")
        players = session.query(models.Player).filter(models.Player.current_team_id == team_id).all()
        hitters = []
        starters = []
        relievers = []
        for p in players:
            summary = session.get(models.PlayerSummary, p.player_id)
            role = summary.role if summary else get_player_role(session, p.player_id)
            player_name = p.full_name
            if is_placeholder_name(player_name, p.player_id):
                resolved = get_full_name(p.player_id)
                player_name = resolved or str(p.player_id)
            position = get_primary_position_abbrev(p.player_id) or p.primary_pos
            payload = {
                "player_id": p.player_id,
                "player_name": player_name,
                "full_name": player_name,  # backward compatibility
                "role": role,
                "position": position,
            }
            if role == "starter":
                starters.append(payload)
            elif role == "reliever":
                relievers.append(payload)
            else:
                hitters.append(payload)
        response.headers["Cache-Control"] = "public, max-age=300"
        return {
            "team_id": team.team_id,
            "team_name": team.team_name,
            "abbrev": team.abbrev,
            "hitters": hitters,
            "starters": starters,
            "relievers": relievers,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()


def _seed_sample_top50() -> None:
    """Insert a demo Top 50 row so the endpoint returns data on a fresh database."""
    session = SessionLocal()
    try:
        existing = session.query(models.UnicornTop50Daily).count()
        if existing:
            return
        sample_date = date(2025, 3, 27)
        sample_row = models.UnicornTop50Daily(
            run_date=sample_date,
            rank=1,
            entity_type="player",
            entity_id=123456,
            pattern_id="DEMO-001",
            metric_value=1.234,
            sample_size=50,
            score=2.5,
            description="Sample unicorn for demo purposes.",
        )
        session.add(sample_row)
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()


def _db_top50_loader(session):
    def _load(run_date: date):
        rows = (
            session.query(
                models.UnicornTop50Daily,
                models.Player,
                models.PlayerSummary,
            )
            .join(models.Player, models.Player.player_id == models.UnicornTop50Daily.entity_id)
            .outerjoin(models.PlayerSummary, models.PlayerSummary.player_id == models.Player.player_id)
            .filter(models.UnicornTop50Daily.run_date == run_date)
            .order_by(models.UnicornTop50Daily.rank.asc())
            .all()
        )
        results = []
        for row in rows:
            top = row[0]
            player = row[1]
            summary = row[2]
            results.append(
                Top50Entry(
                    run_date=str(top.run_date),
                    rank=top.rank,
                    entity_type=top.entity_type,
                    entity_id=top.entity_id,
                    pattern_id=top.pattern_id,
                    score=float(top.score),
                    description=top.description,
                    team_id=player.current_team_id,
                    role=summary.role if summary else None,
                )
            )
        return results

    return _load


def _db_player_loader(session):
    cache = {}

    def _load(player_id: int):
        if player_id in cache:
            return cache[player_id]
        player = session.get(models.Player, player_id)
        summary = session.get(models.PlayerSummary, player_id)
        payload = {
            "player_id": player_id,
            "full_name": player.full_name if player else None,
            "primary_pos": player.primary_pos if player else None,
            "team_id": player.current_team_id if player else None,
            "role": summary.role if summary else None,
        }
        cache[player_id] = payload
        return payload

    return _load


@app.get("/api/admin/audit/top50")
def audit_top50_admin(start: date, end: date, response: Response):
    from os import getenv

    if getenv("ADMIN_ENABLED", "").lower() != "true":
        raise HTTPException(status_code=404, detail="Not found")
    session = SessionLocal()
    try:
        report = audit_range(
            start,
            end,
            load_top50=_db_top50_loader(session),
            load_player=_db_player_loader(session),
        )
        report["base_url"] = "db://local"
        response.headers["Cache-Control"] = "no-store"
        return report
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()
