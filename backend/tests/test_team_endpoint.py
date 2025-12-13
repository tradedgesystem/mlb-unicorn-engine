from datetime import date

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.app.api.main import app
from backend.app.api import main as api_main
from backend.app.db import models
from backend.app.db.base import Base
from backend.app.core import mlbam_people


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
    Base.metadata.create_all(
        bind=engine,
        tables=[
            models.Team.__table__,
            models.Player.__table__,
            models.Game.__table__,
            models.PlateAppearance.__table__,
            models.PitchFact.__table__,
            models.PlayerSummary.__table__,
        ],
    )
    return TestingSessionLocal


def seed_team_data(SessionLocal):
    mlbam_people._PEOPLE_CACHE[1] = ("Hitter One", "CF")
    mlbam_people._PEOPLE_CACHE[2] = ("Pitcher Two", "P")
    with SessionLocal() as session:
        team = models.Team(team_id=119, team_name="Test Team", abbrev="TT")
        hitter = models.Player(player_id=1, full_name="Hitter One", primary_pos="CF", current_team_id=119)
        pitcher = models.Player(player_id=2, full_name="Pitcher Two", primary_pos="P", current_team_id=119)
        game = models.Game(
            game_id=10,
            game_date=date(2025, 3, 29),
            home_team_id=119,
            away_team_id=120,
        )
        pa = models.PlateAppearance(
            pa_id=100,
            game_id=10,
            inning=1,
            top_bottom="T",
            batter_id=999,
            pitcher_id=2,
        )
        session.add_all([team, hitter, pitcher, game, pa])
        session.commit()


def test_team_endpoint_without_as_of_date(monkeypatch):
    SessionLocal = setup_db(monkeypatch)
    seed_team_data(SessionLocal)
    client = TestClient(app)

    resp = client.get("/api/teams/119")
    assert resp.status_code == 200
    data = resp.json()
    assert data["team_id"] == 119
    assert "hitters" in data and "starters" in data and "relievers" in data
    assert isinstance(data.get("starters"), list)
    assert isinstance(data.get("relievers"), list)
    assert isinstance(data.get("hitters"), list)
    hitter_keys = {
        "barrel_pct_last_50",
        "hard_hit_pct_last_50",
        "xwoba_last_50",
        "contact_pct_last_50",
        "chase_pct_last_50",
    }
    starter_keys = {
        "xwoba_last_3_starts",
        "whiff_pct_last_3_starts",
        "k_pct_last_3_starts",
        "bb_pct_last_3_starts",
        "hard_hit_pct_last_3_starts",
    }
    reliever_keys = {
        "xwoba_last_5_apps",
        "whiff_pct_last_5_apps",
        "k_pct_last_5_apps",
        "bb_pct_last_5_apps",
        "hard_hit_pct_last_5_apps",
    }
    for p in data.get("hitters", []):
        assert set((p.get("metrics") or {}).keys()) == hitter_keys
    for p in data.get("starters", []):
        assert set((p.get("metrics") or {}).keys()) == starter_keys
    for p in data.get("relievers", []):
        assert set((p.get("metrics") or {}).keys()) == reliever_keys


def test_team_endpoint_with_as_of_date(monkeypatch):
    SessionLocal = setup_db(monkeypatch)
    seed_team_data(SessionLocal)
    client = TestClient(app)

    resp = client.get("/api/teams/119?as_of_date=2025-03-29")
    assert resp.status_code == 200
    data = resp.json()
    assert data["team_id"] == 119
    assert "hitters" in data and "starters" in data and "relievers" in data
    assert all("metrics" in p for p in data.get("hitters", []))
    assert all("metrics" in p for p in data.get("starters", []))
    assert all("metrics" in p for p in data.get("relievers", []))


def test_team_endpoint_cache_returns_identical_payload(monkeypatch):
    SessionLocal = setup_db(monkeypatch)
    seed_team_data(SessionLocal)
    api_main._TEAM_CACHE.clear()
    client = TestClient(app)

    resp1 = client.get("/api/teams/119")
    resp2 = client.get("/api/teams/119")

    assert resp1.status_code == 200
    assert resp2.status_code == 200
    data1 = resp1.json()
    data2 = resp2.json()

    assert data1 == data2
    for key in ("hitters", "starters", "relievers"):
        assert isinstance(data1.get(key), list)
        assert isinstance(data2.get(key), list)
