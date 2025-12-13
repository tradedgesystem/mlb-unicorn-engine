from copy import deepcopy
from datetime import date
import logging
import os
from time import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from starlette.requests import Request
from sqlalchemy import func, select

from backend.app.db import models
from backend.app.db.base import Base
from backend.app.db.session import SessionLocal, engine
from backend.app.unicorns.queries import fetch_top50_for_date
from backend.app.core.player_metrics import (
    _hitter_metrics as compute_hitter_metrics,
    _starter_metrics as compute_starter_metrics,
    _reliever_metrics as compute_reliever_metrics,
    league_hitter_metrics as compute_league_hitter_metrics,
    get_player_role,
    update_all as refresh_player_metrics,
)
from backend.app.core.roles import (
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_STARTS_THRESHOLD,
    classify_pitcher_role,
    get_pitcher_usage_counts,
)
from backend.app.core.mlbam_people import (
    get_full_name,
    get_primary_position_abbrev,
    is_placeholder_name,
)
from backend.app.tools.audit_top50_quality import Top50Entry, audit_range, _role_from_player
from backend.app.unicorns.engine import apply_min_score_spacing, MIN_REL_GAP

app = FastAPI()

OHTANI_TWO_WAY_ID = 660271

logger = logging.getLogger(__name__)

_HOT_CACHE_CONTROL = "public, max-age=0, s-maxage=60, stale-while-revalidate=300"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_LEAGUE_AVG_TTL_SECONDS = 15 * 60
_LEAGUE_AVG_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}

_TOP50_TTL_SECONDS = 60
_TOP50_CACHE: dict[str, tuple[float, list[dict]]] = {}

_TEAM_TTL_SECONDS = 120
_TEAM_CACHE: dict[int, tuple[float, dict]] = {}

_HITTER_METRIC_KEYS = (
    "barrel_pct_last_50",
    "hard_hit_pct_last_50",
    "xwoba_last_50",
    "contact_pct_last_50",
    "chase_pct_last_50",
)
_STARTER_METRIC_KEYS = (
    "xwoba_last_3_starts",
    "whiff_pct_last_3_starts",
    "k_pct_last_3_starts",
    "bb_pct_last_3_starts",
    "hard_hit_pct_last_3_starts",
)
_RELIEVER_METRIC_KEYS = (
    "xwoba_last_5_apps",
    "whiff_pct_last_5_apps",
    "k_pct_last_5_apps",
    "bb_pct_last_5_apps",
    "hard_hit_pct_last_5_apps",
)


def _metrics_for_team_role(session, player_id: int, role: str) -> dict:
    """Return a stable 5-key metrics dict for a roster player role.

    Never raises; if metric computation fails or sample is unavailable, values are None.
    """
    try:
        normalized = (role or "").strip().lower()
        if normalized == "hitter":
            raw = compute_hitter_metrics(session, player_id) or {}
            return {key: raw.get(key) for key in _HITTER_METRIC_KEYS}
        if normalized == "starter":
            raw = compute_starter_metrics(session, player_id) or {}
            return {key: raw.get(key) for key in _STARTER_METRIC_KEYS}
        if normalized == "reliever":
            raw = compute_reliever_metrics(session, player_id) or {}
            return {key: raw.get(key) for key in _RELIEVER_METRIC_KEYS}
    except Exception:
        logger.exception("Failed to compute roster metrics for player_id=%s role=%s", player_id, role)

    normalized = (role or "").strip().lower()
    if normalized == "starter":
        return {key: None for key in _STARTER_METRIC_KEYS}
    if normalized == "reliever":
        return {key: None for key in _RELIEVER_METRIC_KEYS}
    return {key: None for key in _HITTER_METRIC_KEYS}


def _init_sentry() -> None:
    dsn = (os.getenv("SENTRY_DSN") or "").strip()
    if not dsn:
        return

    try:
        traces_sample_rate = float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1"))
    except ValueError:
        traces_sample_rate = 0.1

    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv("SENTRY_ENVIRONMENT", "production"),
        release=os.getenv("SENTRY_RELEASE") or os.getenv("RENDER_GIT_COMMIT") or "unknown",
        traces_sample_rate=traces_sample_rate,
        integrations=[
            FastApiIntegration(),
            LoggingIntegration(level=logging.INFO, event_level=logging.ERROR),
        ],
    )


_init_sentry()


def _set_hot_cache_headers(response: Response) -> None:
    response.headers["Cache-Control"] = _HOT_CACHE_CONTROL


@app.middleware("http")
async def sentry_request_context(request: Request, call_next):
    scope = sentry_sdk.get_current_scope()
    scope.set_tag("method", request.method)
    scope.set_tag("path", request.url.path)
    route = request.scope.get("route")
    if route is not None:
        scope.set_tag("endpoint", getattr(route, "path", None) or getattr(route, "name", None))
    team_id = request.path_params.get("team_id")
    player_id = request.path_params.get("player_id")
    if team_id is not None:
        scope.set_tag("team_id", str(team_id))
    if player_id is not None:
        scope.set_tag("player_id", str(player_id))

    return await call_next(request)


def _top50_cache_get(run_date: date) -> Optional[list[dict]]:
    cached = _TOP50_CACHE.get(run_date.isoformat())
    if not cached:
        return None
    expires_at, payload = cached
    if time() < expires_at:
        return payload
    _TOP50_CACHE.pop(run_date.isoformat(), None)
    return None


def _top50_cache_set(run_date: date, payload: list[dict]) -> None:
    _TOP50_CACHE[run_date.isoformat()] = (time() + _TOP50_TTL_SECONDS, payload)


def _team_cache_get(team_id: int) -> Optional[dict]:
    cached = _TEAM_CACHE.get(team_id)
    if not cached:
        return None
    expires_at, payload = cached
    if time() < expires_at:
        return payload
    _TEAM_CACHE.pop(team_id, None)
    return None


def _team_cache_set(team_id: int, payload: dict) -> None:
    encoded = jsonable_encoder(payload)
    _TEAM_CACHE[team_id] = (time() + _TEAM_TTL_SECONDS, deepcopy(encoded))


def _league_avg_cache_get(role: str, as_of: date) -> Optional[dict]:
    key = (role, as_of.isoformat())
    cached = _LEAGUE_AVG_CACHE.get(key)
    if not cached:
        return None
    expires_at, payload = cached
    if time() < expires_at:
        return payload
    _LEAGUE_AVG_CACHE.pop(key, None)
    return None


def _league_avg_cache_set(role: str, as_of: date, payload: dict) -> None:
    _LEAGUE_AVG_CACHE[(role, as_of.isoformat())] = (time() + _LEAGUE_AVG_TTL_SECONDS, payload)


def _league_avg_metric_columns(role: str) -> dict[str, object]:
    if role == "hitter":
        return {
            "barrel_pct_last_50": models.PlayerSummary.barrel_pct_last_50,
            "hard_hit_pct_last_50": models.PlayerSummary.hard_hit_pct_last_50,
            "xwoba_last_50": models.PlayerSummary.xwoba_last_50,
            "contact_pct_last_50": models.PlayerSummary.contact_pct_last_50,
            "chase_pct_last_50": models.PlayerSummary.chase_pct_last_50,
        }
    if role == "starter":
        return {
            "xwoba_last_3_starts": models.PlayerSummary.xwoba_last_3_starts,
            "whiff_pct_last_3_starts": models.PlayerSummary.whiff_pct_last_3_starts,
            "k_pct_last_3_starts": models.PlayerSummary.k_pct_last_3_starts,
            "bb_pct_last_3_starts": models.PlayerSummary.bb_pct_last_3_starts,
            "hard_hit_pct_last_3_starts": models.PlayerSummary.hard_hit_pct_last_3_starts,
        }
    if role == "reliever":
        return {
            "xwoba_last_5_apps": models.PlayerSummary.xwoba_last_5_apps,
            "whiff_pct_last_5_apps": models.PlayerSummary.whiff_pct_last_5_apps,
            "k_pct_last_5_apps": models.PlayerSummary.k_pct_last_5_apps,
            "bb_pct_last_5_apps": models.PlayerSummary.bb_pct_last_5_apps,
            "hard_hit_pct_last_5_apps": models.PlayerSummary.hard_hit_pct_last_5_apps,
        }
    return {}


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
    started = time()
    session = SessionLocal()
    try:
        cached = _top50_cache_get(run_date)
        if cached is not None:
            _set_hot_cache_headers(response)
            return cached
        rows = fetch_top50_for_date(session, run_date)
        apply_min_score_spacing(rows, min_rel_gap=MIN_REL_GAP)
        entity_ids = [int(r.entity_id) for r in rows if r.entity_id is not None]
        player_meta = {}
        if entity_ids:
            for pid, primary_pos, role in (
                session.query(
                    models.Player.player_id,
                    models.Player.primary_pos,
                    models.PlayerSummary.role,
                )
                .outerjoin(models.PlayerSummary, models.PlayerSummary.player_id == models.Player.player_id)
                .filter(models.Player.player_id.in_(entity_ids))
                .all()
            ):
                player_meta[int(pid)] = {
                    "primary_pos": primary_pos,
                    "role": role,
                }
        payload = []
        for r in rows:
            base = to_dict(r)
            meta = player_meta.get(int(r.entity_id)) if r.entity_id is not None else None
            base["primary_pos"] = (meta or {}).get("primary_pos")
            base["role"] = (meta or {}).get("role")
            payload.append(base)
        _top50_cache_set(run_date, payload)
        _set_hot_cache_headers(response)
        return payload
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()
        duration = time() - started
        if duration > 2:
            logger.warning("Slow request: /top50/%s %.3fs", run_date.isoformat(), duration)
        else:
            logger.debug("Request: /top50/%s %.3fs", run_date.isoformat(), duration)


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
def get_player_profile(player_id: int, response: Response, as_of_date: Optional[date] = None):
    session = SessionLocal()
    try:
        as_of = as_of_date or date.today()
        lookback = DEFAULT_LOOKBACK_DAYS
        usage_counts = get_pitcher_usage_counts(session, as_of_date=as_of, lookback_days=lookback)
        player = (
            session.query(models.Player, models.Team)
            .outerjoin(models.Team, models.Team.team_id == models.Player.current_team_id)
            .filter(models.Player.player_id == player_id)
            .first()
        )
        if not player:
            raise HTTPException(status_code=404, detail="Player not found")

        if player_id == OHTANI_TWO_WAY_ID:
            hitter_keys = [
                "barrel_pct_last_50",
                "hard_hit_pct_last_50",
                "xwoba_last_50",
                "contact_pct_last_50",
                "chase_pct_last_50",
            ]
            starter_keys = [
                "xwoba_last_3_starts",
                "whiff_pct_last_3_starts",
                "k_pct_last_3_starts",
                "bb_pct_last_3_starts",
                "hard_hit_pct_last_3_starts",
            ]
            hitter_raw = compute_hitter_metrics(session, player_id) or {}
            starter_raw = compute_starter_metrics(session, player_id) or {}
            hitter_metrics = {key: hitter_raw.get(key) for key in hitter_keys}
            pitcher_metrics = {key: starter_raw.get(key) for key in starter_keys}

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
                "role": "hitter",
                "metrics": hitter_metrics,
                "two_way": True,
                "roles": ["hitter", "starter"],
                "hitter_metrics": hitter_metrics,
                "pitcher_metrics": pitcher_metrics,
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

        role = get_player_role(
            session,
            player_id,
            as_of_date=as_of,
            lookback_days=lookback,
            usage_counts=usage_counts,
        )

        if role == "hitter":
            hitter_keys = [
                "barrel_pct_last_50",
                "hard_hit_pct_last_50",
                "xwoba_last_50",
                "contact_pct_last_50",
                "chase_pct_last_50",
            ]
            hitter_raw = compute_hitter_metrics(session, player_id) or {}
            metrics = {key: hitter_raw.get(key) for key in hitter_keys}
        else:
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
            "role": role,
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
def get_player_profile_api(player_id: int, response: Response, as_of_date: Optional[date] = None):
    return get_player_profile(player_id, response, as_of_date=as_of_date)


@app.get("/api/sentry-test")
def sentry_test(debug: Optional[str] = None):
    environment = (os.getenv("SENTRY_ENVIRONMENT") or "production").strip().lower()
    if environment == "production":
        raise HTTPException(status_code=404, detail="Not found")
    if debug not in {None, "", "1"}:
        raise HTTPException(status_code=400, detail="Invalid debug")
    raise Exception("Sentry test (backend)")


@app.get("/api/league-averages")
def get_league_averages(role: str, response: Response, as_of_date: Optional[date] = None):
    session = SessionLocal()
    try:
        normalized_role = (role or "").strip().lower()
        if normalized_role not in {"hitter", "starter", "reliever"}:
            raise HTTPException(status_code=400, detail="Invalid role")

        as_of = as_of_date or date.today()
        cached = _league_avg_cache_get(normalized_role, as_of)
        if cached is not None:
            _set_hot_cache_headers(response)
            return cached

        if normalized_role == "hitter":
            metrics = compute_league_hitter_metrics(session, as_of_date=as_of)
            payload = {
                "role": normalized_role,
                "as_of_date": as_of.isoformat(),
                "metrics": metrics,
            }
            _league_avg_cache_set(normalized_role, as_of, payload)
            _set_hot_cache_headers(response)
            return payload
        if session.query(models.PlayerSummary).count() == 0:
            refresh_player_metrics()

        columns = _league_avg_metric_columns(normalized_role)
        if not columns:
            raise HTTPException(status_code=400, detail="Invalid role")

        stmt = select(*(func.avg(col).label(key) for key, col in columns.items())).where(
            models.PlayerSummary.role == normalized_role
        )
        row = session.execute(stmt).first()
        metrics = {}
        for key in columns:
            value = row._mapping.get(key) if row else None
            metrics[key] = float(value) if value is not None else None

        payload = {
            "role": normalized_role,
            "as_of_date": as_of.isoformat(),
            "metrics": metrics,
        }
        _league_avg_cache_set(normalized_role, as_of, payload)
        _set_hot_cache_headers(response)
        return payload
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()


@app.get("/api/teams")
def list_teams(response: Response):
    session = SessionLocal()
    try:
        teams = session.query(models.Team).order_by(models.Team.team_name.asc()).all()
        _set_hot_cache_headers(response)
        return [
            {"team_id": t.team_id, "team_name": t.team_name, "abbrev": t.abbrev}
            for t in teams
        ]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()


def _effective_as_of_date(session, as_of_date: Optional[date]) -> date:
    if as_of_date:
        return as_of_date
    try:
        latest_row = session.execute(select(func.max(models.Game.game_date))).first()
        latest = latest_row[0] if latest_row else None
        if latest:
            if hasattr(latest, "date"):
                return latest.date()
            if isinstance(latest, date):
                return latest
            # Handle string dates like "2025-03-29"
            return date.fromisoformat(str(latest))
    except Exception:
        # Fall back silently; callers will get today.
        pass
    return date.today()


@app.get("/api/teams/{team_id}")
def get_team(team_id: int, response: Response, as_of_date: Optional[date] = None):
    started = time()
    if as_of_date is None:
        cached = _team_cache_get(team_id)
        if cached is not None:
            _set_hot_cache_headers(response)
            duration = time() - started
            if duration > 2:
                logger.warning("Slow request: /api/teams/%s %.3fs", team_id, duration)
            else:
                logger.debug("Request: /api/teams/%s %.3fs", team_id, duration)
            return cached

    session = SessionLocal()
    try:
        team = session.get(models.Team, team_id)
        if not team:
            raise HTTPException(status_code=404, detail="Team not found")
        players = session.query(models.Player).filter(models.Player.current_team_id == team_id).all()
        if not players:
            payload = {
                "team_id": team.team_id,
                "team_name": team.team_name,
                "abbrev": team.abbrev,
                "hitters": [],
                "starters": [],
                "relievers": [],
            }
            if as_of_date is None:
                _team_cache_set(team_id, payload)
            _set_hot_cache_headers(response)
            return jsonable_encoder(payload)

        as_of = _effective_as_of_date(session, as_of_date)
        lookback = DEFAULT_LOOKBACK_DAYS
        usage_counts = get_pitcher_usage_counts(session, as_of_date=as_of, lookback_days=lookback)
        hitters = []
        starters = []
        relievers = []
        for p in players:
            position = get_primary_position_abbrev(p.player_id) or p.primary_pos
            summary = session.get(models.PlayerSummary, p.player_id)
            base_role = summary.role if summary else None
            counts = usage_counts.get(p.player_id, {"starts": 0, "apps": 0})
            is_pitcher = (
                (position or "").upper() in {"P", "TWP"}
                or (base_role in {"starter", "reliever"})
                or (counts.get("apps", 0) > 0)
            )
            if p.player_id == OHTANI_TWO_WAY_ID:
                role = "starter"
                is_pitcher = True
            else:
                role = (
                    classify_pitcher_role(
                        counts.get("starts", 0),
                        counts.get("apps", 0),
                        starts_threshold=DEFAULT_STARTS_THRESHOLD,
                    )
                    if is_pitcher
                    else (base_role or "hitter")
                )
            player_name = p.full_name
            if is_placeholder_name(player_name, p.player_id):
                resolved = get_full_name(p.player_id)
                player_name = resolved or str(p.player_id)
            payload = {
                "player_id": p.player_id,
                "player_name": player_name,
                "full_name": player_name,  # backward compatibility
                "role": role,
                "position": position,
                "metrics": _metrics_for_team_role(session, p.player_id, role),
            }
            if role == "starter":
                starters.append(payload)
            elif role == "reliever":
                relievers.append(payload)
            else:
                hitters.append(payload)
        # If starters are empty but we have relievers with starts, promote the top ones.
        if not starters and relievers:
            reliever_with_starts = [
                (rc.get("starts", 0), payload)
                for payload in relievers
                for rc in [usage_counts.get(payload["player_id"], {"starts": 0})]
                if rc.get("starts", 0) > 0
            ]
            reliever_with_starts.sort(key=lambda x: x[0], reverse=True)
            promoted = [p for _, p in reliever_with_starts[:5]]
            if promoted:
                starter_ids = {p["player_id"] for p in promoted}
                for p in promoted:
                    p["role"] = "starter"
                    p["metrics"] = _metrics_for_team_role(session, p["player_id"], "starter")
                starters.extend(promoted)
                relievers = [p for p in relievers if p["player_id"] not in starter_ids]
        payload = {
            "team_id": team.team_id,
            "team_name": team.team_name,
            "abbrev": team.abbrev,
            "hitters": hitters,
            "starters": starters,
            "relievers": relievers,
        }
        if as_of_date is None:
            _team_cache_set(team_id, payload)
        _set_hot_cache_headers(response)
        return jsonable_encoder(payload)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        session.close()
        duration = time() - started
        if duration > 2:
            logger.warning("Slow request: /api/teams/%s %.3fs", team_id, duration)
        else:
            logger.debug("Request: /api/teams/%s %.3fs", team_id, duration)


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
