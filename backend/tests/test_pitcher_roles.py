from datetime import date

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.app.api.main import app
from backend.app.db import models
from backend.app.db.base import Base
from backend.app.core import mlbam_people


def _setup_test_db(monkeypatch):
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    monkeypatch.setattr("backend.app.api.main.engine", engine)
    monkeypatch.setattr("backend.app.api.main.SessionLocal", TestingSessionLocal)
    Base.metadata.create_all(
        bind=engine,
        tables=[
            models.Player.__table__,
            models.Team.__table__,
            models.Game.__table__,
            models.PlateAppearance.__table__,
            models.PlayerSummary.__table__,
        ],
    )
    return TestingSessionLocal


def test_team_endpoint_uses_pitcher_usage(monkeypatch):
    session_factory = _setup_test_db(monkeypatch)

    # Prevent MLBAM network lookups.
    mlbam_people._PEOPLE_CACHE[100] = ("Pitcher A", "P")
    mlbam_people._PEOPLE_CACHE[101] = ("Pitcher B", "P")

    with session_factory() as session:
        team = models.Team(team_id=1, team_name="Team A", abbrev="TA")
        other_team = models.Team(team_id=2, team_name="Team B", abbrev="TB")
        session.add_all([team, other_team])

        pitcher_a = models.Player(player_id=100, full_name="Pitcher A", primary_pos="P", current_team_id=1)
        pitcher_b = models.Player(player_id=101, full_name="Pitcher B", primary_pos="P", current_team_id=1)
        session.add_all([pitcher_a, pitcher_b])

        # Provide summaries so the API doesn't try to refresh metrics during the test.
        session.add_all(
            [
                models.PlayerSummary(player_id=100, role="starter"),
                models.PlayerSummary(player_id=101, role="reliever"),
            ]
        )

        game1 = models.Game(game_id=1, game_date=date(2025, 6, 1), home_team_id=1, away_team_id=2)
        game2 = models.Game(game_id=2, game_date=date(2025, 6, 2), home_team_id=1, away_team_id=2)
        session.add_all([game1, game2])

        pas = [
            # Game 1: pitcher A is starter (first PA for top half)
            models.PlateAppearance(
                pa_id=1,
                game_id=1,
                inning=1,
                top_bottom="T",
                batter_id=2000,
                pitcher_id=100,
            ),
            models.PlateAppearance(
                pa_id=2,
                game_id=1,
                inning=1,
                top_bottom="T",
                batter_id=2001,
                pitcher_id=100,
            ),
            # pitcher B appears later in the same game
            models.PlateAppearance(
                pa_id=3,
                game_id=1,
                inning=7,
                top_bottom="T",
                batter_id=2002,
                pitcher_id=101,
            ),
            # Game 2: pitcher A starts again
            models.PlateAppearance(
                pa_id=10,
                game_id=2,
                inning=1,
                top_bottom="T",
                batter_id=2003,
                pitcher_id=100,
            ),
            models.PlateAppearance(
                pa_id=11,
                game_id=2,
                inning=1,
                top_bottom="T",
                batter_id=2004,
                pitcher_id=100,
            ),
            models.PlateAppearance(
                pa_id=12,
                game_id=2,
                inning=6,
                top_bottom="T",
                batter_id=2005,
                pitcher_id=101,
            ),
        ]
        session.add_all(pas)
        session.commit()

    # Avoid calling the real startup hook (which creates all tables including JSONB columns).
    app.router.on_startup.clear()

    with TestClient(app) as client:
        resp = client.get("/api/teams/1", params={"as_of_date": "2025-06-06"})
        assert resp.status_code == 200
        data = resp.json()

    starter_ids = {p["player_id"] for p in data["starters"]}
    reliever_ids = {p["player_id"] for p in data["relievers"]}
    assert 100 in starter_ids
    assert 101 in reliever_ids
