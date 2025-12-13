from datetime import date

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from backend.app.api.main import app
from backend.app.api import main as api_main
from backend.app.core import mlbam_people
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
            models.UnicornTop50Daily.__table__,
        ],
    )
    return TestingSessionLocal


def seed_players(SessionLocal):
    mlbam_people._PEOPLE_CACHE[1] = ("Hitter One", "CF")
    mlbam_people._PEOPLE_CACHE[2] = ("Starter Two", "P")
    mlbam_people._PEOPLE_CACHE[3] = ("Reliever Three", "P")
    with SessionLocal() as session:
        team = models.Team(team_id=119, team_name="Test Team", abbrev="TT")
        hitter = models.Player(player_id=1, full_name="Hitter One", primary_pos="CF", current_team_id=119)
        starter = models.Player(player_id=2, full_name="Starter Two", primary_pos="P", current_team_id=119)
        reliever = models.Player(player_id=3, full_name="Reliever Three", primary_pos="P", current_team_id=119)

        # Two games where pitcher 2 is the first PA pitcher (starter classification).
        game1 = models.Game(game_id=10, game_date=date(2025, 3, 29), home_team_id=119, away_team_id=120)
        game2 = models.Game(game_id=11, game_date=date(2025, 3, 28), home_team_id=119, away_team_id=120)
        pa1 = models.PlateAppearance(
            pa_id=100, game_id=10, inning=1, top_bottom="T", batter_id=999, pitcher_id=2
        )
        pa2 = models.PlateAppearance(
            pa_id=101, game_id=11, inning=1, top_bottom="T", batter_id=998, pitcher_id=2
        )
        # Pitcher 3 appears but is not the first PA pitcher (reliever classification).
        pa3 = models.PlateAppearance(
            pa_id=200, game_id=10, inning=2, top_bottom="T", batter_id=997, pitcher_id=3
        )

        session.add_all([team, hitter, starter, reliever, game1, game2, pa1, pa2, pa3])

        session.add(
            models.PlayerSummary(
                player_id=1,
                role="hitter",
                barrel_pct_last_50=0.1,
                hard_hit_pct_last_50=0.4,
                xwoba_last_50=0.35,
                contact_pct_last_50=0.75,
                chase_pct_last_50=0.22,
            )
        )
        session.add(
            models.PlayerSummary(
                player_id=2,
                role="starter",
                xwoba_last_3_starts=0.28,
                whiff_pct_last_3_starts=0.31,
                k_pct_last_3_starts=0.27,
                bb_pct_last_3_starts=0.08,
                hard_hit_pct_last_3_starts=0.33,
            )
        )
        session.add(
            models.PlayerSummary(
                player_id=3,
                role="reliever",
                xwoba_last_5_apps=0.30,
                whiff_pct_last_5_apps=0.29,
                k_pct_last_5_apps=0.25,
                bb_pct_last_5_apps=0.09,
                hard_hit_pct_last_5_apps=0.36,
            )
        )
        session.commit()


def _assert_numeric_or_null(value):
    assert value is None or isinstance(value, (int, float))


def test_league_averages_keys_match_player_metrics(monkeypatch):
    SessionLocal = setup_db(monkeypatch)
    seed_players(SessionLocal)
    client = TestClient(app)

    # Fixed as_of_date so pitcher role classification is stable in the test DB.
    as_of = "2025-03-29"

    role_cases = [
        ("hitter", 1),
        ("starter", 2),
        ("reliever", 3),
    ]

    for role, player_id in role_cases:
        league_resp = client.get(f"/api/league-averages?role={role}&as_of_date={as_of}")
        assert league_resp.status_code == 200
        league = league_resp.json()
        assert league["role"] == role
        assert isinstance(league.get("metrics"), dict)
        assert len(league["metrics"]) == 5
        for v in league["metrics"].values():
            _assert_numeric_or_null(v)

        player_resp = client.get(f"/api/players/{player_id}?as_of_date={as_of}")
        assert player_resp.status_code == 200
        player = player_resp.json()
        assert set(league["metrics"].keys()) == set((player.get("metrics") or {}).keys())
