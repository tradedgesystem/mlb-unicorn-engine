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


OHTANI_ID = 660271


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
            models.UnicornTop50Daily.__table__,
            models.PlayerSummary.__table__,
        ],
    )
    return TestingSessionLocal


def test_team_roster_places_ohtani_in_starters(monkeypatch):
    SessionLocal = setup_db(monkeypatch)
    mlbam_people._PEOPLE_CACHE[OHTANI_ID] = ("Shohei Ohtani", "TWP")
    with SessionLocal() as session:
        session.add(models.Team(team_id=119, team_name="Los Angeles Dodgers", abbrev="LAD"))
        session.add(
            models.Player(
                player_id=OHTANI_ID,
                full_name="Shohei Ohtani",
                primary_pos="TWP",
                current_team_id=119,
            )
        )
        session.commit()

    client = TestClient(app)
    resp = client.get("/api/teams/119")
    assert resp.status_code == 200
    data = resp.json()
    starter_ids = {p["player_id"] for p in data.get("starters", [])}
    reliever_ids = {p["player_id"] for p in data.get("relievers", [])}
    assert OHTANI_ID in starter_ids
    assert OHTANI_ID not in reliever_ids


def test_player_endpoint_two_way_payload(monkeypatch):
    SessionLocal = setup_db(monkeypatch)
    mlbam_people._PEOPLE_CACHE[OHTANI_ID] = ("Shohei Ohtani", "TWP")
    with SessionLocal() as session:
        session.add(models.Team(team_id=119, team_name="Los Angeles Dodgers", abbrev="LAD"))
        session.add_all(
            [
                models.Player(
                    player_id=OHTANI_ID,
                    full_name="Shohei Ohtani",
                    primary_pos="TWP",
                    current_team_id=119,
                ),
                models.Player(player_id=999, full_name="Opponent Pitcher", primary_pos="P"),
                models.Player(player_id=888, full_name="Opponent Batter 1", primary_pos="CF"),
                models.Player(player_id=889, full_name="Opponent Batter 2", primary_pos="1B"),
            ]
        )
        # One game for hitter metrics and one game for pitcher metrics.
        game_hit = models.Game(game_id=1000, game_date=date(2025, 3, 29), home_team_id=119, away_team_id=120)
        game_pit = models.Game(game_id=1001, game_date=date(2025, 3, 28), home_team_id=119, away_team_id=120)
        session.add_all([game_hit, game_pit])

        # Hitting: last PA + pitch facts.
        pa_hit = models.PlateAppearance(
            pa_id=5000,
            game_id=1000,
            inning=1,
            top_bottom="T",
            batter_id=OHTANI_ID,
            pitcher_id=999,
            result="single",
            xwoba=0.400,
        )
        pitch_hit = models.PitchFact(
            pitch_id=7000,
            game_id=1000,
            pa_id=5000,
            inning=1,
            top_bottom="T",
            batter_id=OHTANI_ID,
            pitcher_id=999,
            pitch_number_game=1,
            is_in_zone=False,
            result_pitch="in_play",
            launch_speed=100.0,
            is_barrel=True,
            is_hard_hit=True,
        )

        # Pitching: starter metrics derived from last starts.
        pa_p1 = models.PlateAppearance(
            pa_id=5100,
            game_id=1001,
            inning=1,
            top_bottom="T",
            batter_id=888,
            pitcher_id=OHTANI_ID,
            result="strikeout",
            xwoba=0.300,
        )
        pa_p2 = models.PlateAppearance(
            pa_id=5101,
            game_id=1001,
            inning=1,
            top_bottom="T",
            batter_id=889,
            pitcher_id=OHTANI_ID,
            result="walk",
            xwoba=0.200,
        )
        pitch_p1 = models.PitchFact(
            pitch_id=7100,
            game_id=1001,
            pa_id=5100,
            inning=1,
            top_bottom="T",
            batter_id=888,
            pitcher_id=OHTANI_ID,
            pitch_number_game=1,
            is_in_zone=True,
            result_pitch="swinging_strike",
            is_hard_hit=False,
        )
        pitch_p2 = models.PitchFact(
            pitch_id=7101,
            game_id=1001,
            pa_id=5101,
            inning=1,
            top_bottom="T",
            batter_id=889,
            pitcher_id=OHTANI_ID,
            pitch_number_game=2,
            is_in_zone=True,
            result_pitch="in_play",
            launch_speed=95.0,
            is_hard_hit=True,
        )

        session.add_all([pa_hit, pitch_hit, pa_p1, pa_p2, pitch_p1, pitch_p2])
        session.commit()

    client = TestClient(app)
    resp = client.get(f"/api/players/{OHTANI_ID}?as_of_date=2025-03-29")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("two_way") is True
    assert data.get("roles") == ["hitter", "starter"]
    assert data.get("role") == "hitter"

    hitter_metrics = data.get("hitter_metrics") or {}
    pitcher_metrics = data.get("pitcher_metrics") or {}
    assert set(hitter_metrics.keys()) == {
        "barrel_pct_last_50",
        "hard_hit_pct_last_50",
        "xwoba_last_50",
        "contact_pct_last_50",
        "chase_pct_last_50",
    }
    assert set(pitcher_metrics.keys()) == {
        "xwoba_last_3_starts",
        "whiff_pct_last_3_starts",
        "k_pct_last_3_starts",
        "bb_pct_last_3_starts",
        "hard_hit_pct_last_3_starts",
    }
    # Hitting metrics should be present when sample exists.
    assert hitter_metrics.get("xwoba_last_50") is not None
