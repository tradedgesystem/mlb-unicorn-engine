"""ETL loader for ingesting raw Statcast-like data into core tables."""
from __future__ import annotations

from typing import Iterable, Mapping, Sequence

from sqlalchemy.orm import Session

from backend.db import models
from backend.app.core.logging import logger


def clean_value(v):
    """
    Convert pandas NA, numpy nan, and NaN-like values into None.
    Postgres cannot serialize these values, so we sanitize them here.
    """
    import math
    import pandas as pd
    try:
        if v is None:
            return None
        if v is pd.NA:
            return None
        # numpy.nan and float('nan')
        if isinstance(v, float) and math.isnan(v):
            return None
        return v
    except Exception:
        return None


class StatcastLoader:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_players(self, players: Iterable[Mapping]) -> None:
        """Insert or update players from iterable of dict-like records."""
        for row in players:
            player = self.session.get(models.Player, row["player_id"]) or models.Player(
                player_id=row["player_id"]
            )
            player.mlb_id = row.get("mlb_id")
            player.full_name = row.get("full_name") or row.get("name")
            player.bat_side = row.get("bat_side")
            player.throw_side = row.get("throw_side")
            player.primary_pos = row.get("primary_pos")
            player.current_team_id = row.get("current_team_id")
            self.session.add(player)
        logger.info("Upserted players batch")

    def upsert_teams(self, teams: Iterable[Mapping]) -> None:
        """Upsert team rows using PostgreSQL ON CONFLICT."""
        from sqlalchemy.dialects.postgresql import insert

        for row in teams:
            stmt = (
                insert(models.Team)
                .values(
                    team_id=row["team_id"],
                    team_name=row.get("team_name"),
                    abbrev=row.get("abbrev"),
                )
                .on_conflict_do_update(
                    index_elements=[models.Team.team_id],
                    set_={
                        "team_name": row.get("team_name"),
                        "abbrev": row.get("abbrev"),
                    },
                )
            )
            self.session.execute(stmt)
        logger.info("Upserted teams batch")

    def insert_games(self, games: Iterable[Mapping]) -> None:
        from sqlalchemy.dialects.postgresql import insert

        count = 0
        for row in games:
            stmt = (
                insert(models.Game)
                .values(
                    game_id=row["game_id"],
                    game_date=row["game_date"],
                    home_team_id=row.get("home_team_id"),
                    away_team_id=row.get("away_team_id"),
                    venue_id=row.get("venue_id"),
                    is_day_game=row.get("is_day_game"),
                    is_night_game=row.get("is_night_game"),
                )
                .on_conflict_do_nothing(index_elements=[models.Game.game_id])
            )
            self.session.execute(stmt)
            count += 1
        logger.info("Inserted/upserted %s games", count)

    def insert_pitch_facts(self, pitches: Iterable[Mapping]) -> None:
        objs = []
        allowed_fields = {col.key for col in models.PitchFact.__table__.columns}
        for row in pitches:
            clean_row = {k: clean_value(v) for k, v in row.items()}
            filtered_row = {k: v for k, v in clean_row.items() if k in allowed_fields}
            objs.append(models.PitchFact(**filtered_row))
        if objs:
            self.session.bulk_save_objects(objs)
        logger.info("Inserted %s pitch_facts rows", len(objs))

    def insert_players(self, players):
        from sqlalchemy.dialects.postgresql import insert

        for p in players:
            stmt = (
                insert(models.Player)
                .values(**p)
                .on_conflict_do_update(
                    index_elements=[models.Player.player_id],
                    set_={
                        "mlb_id": p.get("mlb_id"),
                        "full_name": p.get("full_name"),
                        "bat_side": p.get("bat_side"),
                        "throw_side": p.get("throw_side"),
                        "primary_pos": p.get("primary_pos"),
                        "current_team_id": p.get("current_team_id"),
                    },
                )
            )
            self.session.execute(stmt)
        self.session.commit()

    def insert_pa_facts(self, pa_facts):
        from sqlalchemy.dialects.postgresql import insert

        for pa in pa_facts:
            clean_pa = {k: clean_value(v) for k, v in pa.items()}
            stmt = (
                insert(models.PlateAppearance)
                .values(**clean_pa)
                .on_conflict_do_nothing(index_elements=[models.PlateAppearance.pa_id])
            )
            self.session.execute(stmt)
        self.session.commit()

    def load_all(
        self,
        *,
        players: Sequence[Mapping],
        teams: Sequence[Mapping],
        games: Sequence[Mapping],
        pitch_facts: Sequence[Mapping],
        pa_facts: Sequence[Mapping],
    ) -> None:
        self.upsert_teams(teams)
        self.insert_games(games)
        self.insert_players(players)
        self.insert_pa_facts(pa_facts)
        self.insert_pitch_facts(pitch_facts)
        self.session.commit()
        logger.info("Committed full batch load")
