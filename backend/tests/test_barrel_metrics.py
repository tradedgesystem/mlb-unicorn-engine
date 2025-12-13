from datetime import date

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.app.api.main import app
from backend.app.api import main as api_main
from backend.app.core import mlbam_people
from backend.app.core.player_metrics import pitch_facts_barrel_diagnostics
from backend.app.db import models
from backend.app.db.base import Base


def setup_db(monkeypatch):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    monkeypatch.setattr(api_main, "engine", engine)
    monkeypatch.setattr(api_main, "SessionLocal", TestingSessionLocal)
    api_main._LEAGUE_AVG_CACHE.clear()
    Base.metadata.create_all(
        bind=engine,
        tables=[
            models.Team.__table__,
            models.Player.__table__,
            models.Game.__table__,
            models.PlateAppearance.__table__,
            models.PitchFact.__table__,
            models.UnicornTop50Daily.__table__,
        ],
    )
    return TestingSessionLocal


def seed_barrel_eligible_contact(SessionLocal):
    hitter_id = 1
    mlbam_people._PEOPLE_CACHE[hitter_id] = ("Barrel Hitter", "CF")
    with SessionLocal() as session:
        session.add(models.Team(team_id=119, team_name="Test Team", abbrev="TT"))
        session.add(
            models.Player(
                player_id=hitter_id,
                full_name="Barrel Hitter",
                primary_pos="CF",
                current_team_id=119,
            )
        )
        session.add(models.Player(player_id=999, full_name="Opp Pitcher", primary_pos="P"))
        game = models.Game(game_id=10, game_date=date(2025, 3, 29), home_team_id=119, away_team_id=120)
        pa = models.PlateAppearance(
            pa_id=100,
            game_id=10,
            inning=1,
            top_bottom="T",
            batter_id=hitter_id,
            pitcher_id=999,
            result="in_play",
            xwoba=0.500,
        )
        pitch = models.PitchFact(
            pitch_id=1000,
            game_id=10,
            pa_id=100,
            inning=1,
            top_bottom="T",
            batter_id=hitter_id,
            pitcher_id=999,
            pitch_number_game=1,
            is_last_pitch_of_pa=True,
            result_pitch="in_play",
            launch_speed=100.0,
            launch_angle=28.0,
            is_barrel=None,  # simulate missing/broken ETL flag
            is_hard_hit=True,
        )
        session.add_all([game, pa, pitch])
        session.commit()


def test_barrel_diagnostics_and_player_barrel_pct(monkeypatch):
    SessionLocal = setup_db(monkeypatch)
    seed_barrel_eligible_contact(SessionLocal)
    with SessionLocal() as session:
        diag = pitch_facts_barrel_diagnostics(session)
        assert diag["batted_ball_candidates"] >= 1
        assert diag["flagged_barrels"] == 0

    client = TestClient(app)
    resp = client.get("/api/players/1?as_of_date=2025-03-29")
    assert resp.status_code == 200
    data = resp.json()
    barrel_pct = (data.get("metrics") or {}).get("barrel_pct_last_50")
    assert barrel_pct is not None
    assert barrel_pct > 0


def test_league_average_barrel_pct_nonzero_when_barrel_exists(monkeypatch):
    SessionLocal = setup_db(monkeypatch)
    seed_barrel_eligible_contact(SessionLocal)
    client = TestClient(app)

    resp = client.get("/api/league-averages?role=hitter&as_of_date=2025-03-29")
    assert resp.status_code == 200
    metrics = resp.json().get("metrics") or {}
    barrel_pct = metrics.get("barrel_pct_last_50")
    assert barrel_pct is not None
    assert barrel_pct > 0
