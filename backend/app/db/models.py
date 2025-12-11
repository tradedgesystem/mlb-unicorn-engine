from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.db.base import Base
from backend.app.db.session import database_url

JSONType = JSON if database_url.startswith("sqlite") else JSONB


class Player(Base):
    __tablename__ = "players"

    player_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    mlb_id: Mapped[Optional[int]] = mapped_column(BigInteger, unique=True)
    full_name: Mapped[str] = mapped_column(Text, nullable=False)
    bat_side: Mapped[Optional[str]] = mapped_column(String(1))
    throw_side: Mapped[Optional[str]] = mapped_column(String(1))
    primary_pos: Mapped[Optional[str]] = mapped_column(Text)
    current_team_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("teams.team_id")
    )


class Team(Base):
    __tablename__ = "teams"

    team_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    team_name: Mapped[str] = mapped_column(Text, nullable=False)
    abbrev: Mapped[str] = mapped_column(Text, nullable=False)


class Game(Base):
    __tablename__ = "games"
    __table_args__ = (UniqueConstraint("game_date", "home_team_id", "away_team_id"),)

    game_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    game_date: Mapped[date] = mapped_column(Date, nullable=False)
    home_team_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("teams.team_id"))
    away_team_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("teams.team_id"))
    venue_id: Mapped[Optional[int]] = mapped_column(Integer)
    is_day_game: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_night_game: Mapped[Optional[bool]] = mapped_column(Boolean)


class PitchFact(Base):
    __tablename__ = "pitch_facts"
    __table_args__ = (
        Index("idx_pitch_facts_batter", "batter_id"),
        Index("idx_pitch_facts_pitcher", "pitcher_id"),
        Index("idx_pitch_facts_game_date", "game_id"),
        Index("idx_pitch_facts_count_str", "count_str"),
    )

    pitch_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    game_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("games.game_id"))
    pa_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    inning: Mapped[Optional[int]] = mapped_column(Integer)
    top_bottom: Mapped[Optional[str]] = mapped_column(String(1))
    batter_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("players.player_id"))
    pitcher_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("players.player_id"))
    pitch_number_pa: Mapped[Optional[int]] = mapped_column(Integer)
    pitch_number_game: Mapped[Optional[int]] = mapped_column(Integer)
    pitch_type: Mapped[Optional[str]] = mapped_column(Text)
    vel: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    spin_rate: Mapped[Optional[float]] = mapped_column(Numeric(7, 2))
    count_balls_before: Mapped[Optional[int]] = mapped_column(Integer)
    count_strikes_before: Mapped[Optional[int]] = mapped_column(Integer)
    is_in_zone: Mapped[Optional[bool]] = mapped_column(Boolean)
    result_pitch: Mapped[Optional[str]] = mapped_column(Text)
    is_last_pitch_of_pa: Mapped[Optional[bool]] = mapped_column(Boolean)
    launch_speed: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    launch_angle: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    spray_angle: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    is_barrel: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_hard_hit: Mapped[Optional[bool]] = mapped_column(Boolean)
    batted_ball_type: Mapped[Optional[str]] = mapped_column(Text)
    hit_direction: Mapped[Optional[str]] = mapped_column(Text)
    loc_high_mid_low: Mapped[Optional[str]] = mapped_column(Text)
    loc_in_mid_out: Mapped[Optional[str]] = mapped_column(Text)
    loc_region: Mapped[Optional[str]] = mapped_column(Text)
    pa_outcome: Mapped[Optional[str]] = mapped_column(Text)
    is_hr: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_hit: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_walk: Mapped[Optional[bool]] = mapped_column(Boolean)
    count_str: Mapped[Optional[str]] = mapped_column(Text)


class PlateAppearance(Base):
    __tablename__ = "pa_facts"
    __table_args__ = (
        Index("idx_pa_facts_batter", "batter_id"),
        Index("idx_pa_facts_pitcher", "pitcher_id"),
        Index("idx_pa_facts_game", "game_id"),
    )

    pa_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    game_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("games.game_id"))
    inning: Mapped[Optional[int]] = mapped_column(Integer)
    top_bottom: Mapped[Optional[str]] = mapped_column(String(1))
    batter_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("players.player_id"))
    pitcher_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("players.player_id"))
    result: Mapped[Optional[str]] = mapped_column(Text)
    is_hit: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_hr: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_bb: Mapped[Optional[bool]] = mapped_column(Boolean)
    xwoba: Mapped[Optional[float]] = mapped_column(Numeric(5, 3))
    bases_state_before: Mapped[Optional[str]] = mapped_column(Text)
    outs_before: Mapped[Optional[int]] = mapped_column(Integer)
    score_diff_before: Mapped[Optional[int]] = mapped_column(Integer)
    bat_order: Mapped[Optional[int]] = mapped_column(Integer)
    is_risp: Mapped[Optional[bool]] = mapped_column(Boolean)


class TeamMarketContext(Base):
    __tablename__ = "team_market_context"

    team_id: Mapped[int] = mapped_column(Integer, ForeignKey("teams.team_id"), primary_key=True)
    season_year: Mapped[int] = mapped_column(Integer, nullable=False)
    attendance_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 3))
    media_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 3))
    market_weight: Mapped[Optional[float]] = mapped_column(Numeric(5, 3))
    market_weight_adj: Mapped[Optional[float]] = mapped_column(Numeric(5, 3))


class PatternTemplate(Base):
    __tablename__ = "pattern_templates"
    __table_args__ = (
        CheckConstraint(
            "count_value IS NULL OR count_value IN ('3-0','0-2','3-2')",
            name="ck_pattern_templates_count_value",
        ),
        Index("idx_pattern_enabled", "enabled"),
    )

    pattern_id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description_template: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    base_table: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[Optional[str]] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    filters_json: Mapped[dict] = mapped_column(JSONType, nullable=False)
    order_direction: Mapped[str] = mapped_column(Text, nullable=False)
    metric: Mapped[str] = mapped_column(Text, nullable=False)
    metric_expr: Mapped[Optional[str]] = mapped_column(Text)
    target_sample: Mapped[Optional[int]] = mapped_column(Integer)
    min_sample: Mapped[int] = mapped_column(Integer, default=10)
    unicorn_weight: Mapped[float] = mapped_column(Numeric(4, 2), default=1.0)
    public_weight: Mapped[float] = mapped_column(Numeric(4, 2), default=1.0)
    complexity_score: Mapped[int] = mapped_column(Integer, nullable=False)
    requires_count: Mapped[bool] = mapped_column(Boolean, default=False)
    count_value: Mapped[Optional[str]] = mapped_column(Text)


class UnicornResult(Base):
    __tablename__ = "unicorn_results"
    __table_args__ = (
        Index("idx_unicorn_results_date", "run_date"),
        Index("idx_unicorn_results_score", "run_date", "score"),
    )

    run_date: Mapped[date] = mapped_column(Date, primary_key=True)
    pattern_id: Mapped[str] = mapped_column(Text, ForeignKey("pattern_templates.pattern_id"), primary_key=True)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    metric_value: Mapped[float] = mapped_column(Numeric(14, 6), nullable=False)
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    z_raw: Mapped[Optional[float]] = mapped_column(Numeric(14, 6))
    z_adjusted: Mapped[Optional[float]] = mapped_column(Numeric(14, 6))
    score: Mapped[float] = mapped_column(Numeric(14, 6), nullable=False)


class UnicornTop50Daily(Base):
    __tablename__ = "unicorn_top50_daily"
    __table_args__ = (
        Index("idx_top50_player", "run_date", "entity_id"),
    )

    run_date: Mapped[date] = mapped_column(Date, primary_key=True)
    rank: Mapped[int] = mapped_column(Integer, primary_key=True)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pattern_id: Mapped[str] = mapped_column(Text, nullable=False)
    metric_value: Mapped[float] = mapped_column(Numeric(14, 6), nullable=False)
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[float] = mapped_column(Numeric(14, 6), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)


class PlayerSummary(Base):
    __tablename__ = "player_summary"

    player_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("players.player_id"), primary_key=True)
    role: Mapped[str] = mapped_column(Text, nullable=False)

    barrel_pct_last_50: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    hard_hit_pct_last_50: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    xwoba_last_50: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    contact_pct_last_50: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    chase_pct_last_50: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))

    xwoba_last_3_starts: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    whiff_pct_last_3_starts: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    k_pct_last_3_starts: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    bb_pct_last_3_starts: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    hard_hit_pct_last_3_starts: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))

    xwoba_last_5_apps: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    whiff_pct_last_5_apps: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    k_pct_last_5_apps: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    bb_pct_last_5_apps: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))
    hard_hit_pct_last_5_apps: Mapped[Optional[float]] = mapped_column(Numeric(6, 4))

    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default=func.now())
