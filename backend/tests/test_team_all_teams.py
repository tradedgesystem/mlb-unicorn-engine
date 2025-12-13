from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.app.api.main import app
from backend.app.api import main as api_main
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
    Base.metadata.create_all(
        bind=engine,
        tables=[
            models.Team.__table__,
            models.Player.__table__,
            models.Game.__table__,
            models.PlateAppearance.__table__,
            models.PlayerSummary.__table__,
        ],
    )
    return TestingSessionLocal


def test_team_endpoint_stable_for_all_teams(monkeypatch):
    SessionLocal = setup_db(monkeypatch)
    api_main._TEAM_CACHE.clear()

    with SessionLocal() as session:
        session.add_all(
            [
                models.Team(team_id=1, team_name="Alpha Team", abbrev="ALP"),
                models.Team(team_id=2, team_name="Bravo Team", abbrev="BRV"),
                models.Team(team_id=3, team_name="Charlie Team", abbrev="CHL"),
            ]
        )
        session.commit()

    client = TestClient(app)
    resp = client.get("/api/teams")
    assert resp.status_code == 200
    teams = resp.json()
    assert isinstance(teams, list)
    assert teams, "Expected at least one team in /api/teams response"

    for t in teams:
        team_id = t.get("team_id")
        assert isinstance(team_id, int)
        detail = client.get(f"/api/teams/{team_id}")
        assert detail.status_code == 200
        payload = detail.json()
        for key in ("hitters", "starters", "relievers"):
            assert key in payload
            assert isinstance(payload[key], list)

