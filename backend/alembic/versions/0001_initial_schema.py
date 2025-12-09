"""Initial schema for MLB Unicorn Engine"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teams",
        sa.Column("team_id", sa.Integer(), primary_key=True),
        sa.Column("team_name", sa.Text(), nullable=False),
        sa.Column("abbrev", sa.Text(), nullable=False),
    )

    op.create_table(
        "players",
        sa.Column("player_id", sa.BigInteger(), primary_key=True),
        sa.Column("mlb_id", sa.BigInteger(), unique=True),
        sa.Column("full_name", sa.Text(), nullable=False),
        sa.Column("bat_side", sa.String(length=1)),
        sa.Column("throw_side", sa.String(length=1)),
        sa.Column("primary_pos", sa.Text()),
        sa.Column("current_team_id", sa.Integer(), sa.ForeignKey("teams.team_id")),
    )

    op.create_table(
        "games",
        sa.Column("game_id", sa.BigInteger(), primary_key=True),
        sa.Column("game_date", sa.Date(), nullable=False),
        sa.Column("home_team_id", sa.Integer(), sa.ForeignKey("teams.team_id")),
        sa.Column("away_team_id", sa.Integer(), sa.ForeignKey("teams.team_id")),
        sa.Column("venue_id", sa.Integer()),
        sa.Column("is_day_game", sa.Boolean()),
        sa.Column("is_night_game", sa.Boolean()),
        sa.UniqueConstraint("game_date", "home_team_id", "away_team_id"),
    )

    op.create_table(
        "pitch_facts",
        sa.Column("pitch_id", sa.BigInteger(), primary_key=True),
        sa.Column("game_id", sa.BigInteger(), sa.ForeignKey("games.game_id")),
        sa.Column("pa_id", sa.BigInteger()),
        sa.Column("inning", sa.Integer()),
        sa.Column("top_bottom", sa.String(length=1)),
        sa.Column("batter_id", sa.BigInteger(), sa.ForeignKey("players.player_id")),
        sa.Column("pitcher_id", sa.BigInteger(), sa.ForeignKey("players.player_id")),
        sa.Column("pitch_number_pa", sa.Integer()),
        sa.Column("pitch_number_game", sa.Integer()),
        sa.Column("pitch_type", sa.Text()),
        sa.Column("vel", sa.Numeric(5, 2)),
        sa.Column("spin_rate", sa.Numeric(7, 2)),
        sa.Column("count_balls_before", sa.Integer()),
        sa.Column("count_strikes_before", sa.Integer()),
        sa.Column("is_in_zone", sa.Boolean()),
        sa.Column("result_pitch", sa.Text()),
        sa.Column("is_last_pitch_of_pa", sa.Boolean()),
        sa.Column("launch_speed", sa.Numeric(5, 2)),
        sa.Column("launch_angle", sa.Numeric(5, 2)),
        sa.Column("spray_angle", sa.Numeric(5, 2)),
        sa.Column("is_barrel", sa.Boolean()),
        sa.Column("is_hard_hit", sa.Boolean()),
        sa.Column("batted_ball_type", sa.Text()),
        sa.Column("hit_direction", sa.Text()),
        sa.Column("loc_high_mid_low", sa.Text()),
        sa.Column("loc_in_mid_out", sa.Text()),
        sa.Column("loc_region", sa.Text()),
        sa.Column("pa_outcome", sa.Text()),
        sa.Column("is_hr", sa.Boolean()),
        sa.Column("is_hit", sa.Boolean()),
        sa.Column("is_walk", sa.Boolean()),
        sa.Column("count_str", sa.Text()),
    )
    op.create_index("idx_pitch_facts_batter", "pitch_facts", ["batter_id"])
    op.create_index("idx_pitch_facts_pitcher", "pitch_facts", ["pitcher_id"])
    op.create_index("idx_pitch_facts_game_date", "pitch_facts", ["game_id"])
    op.create_index("idx_pitch_facts_count_str", "pitch_facts", ["count_str"])

    op.create_table(
        "pa_facts",
        sa.Column("pa_id", sa.BigInteger(), primary_key=True),
        sa.Column("game_id", sa.BigInteger(), sa.ForeignKey("games.game_id")),
        sa.Column("inning", sa.Integer()),
        sa.Column("top_bottom", sa.String(length=1)),
        sa.Column("batter_id", sa.BigInteger(), sa.ForeignKey("players.player_id")),
        sa.Column("pitcher_id", sa.BigInteger(), sa.ForeignKey("players.player_id")),
        sa.Column("result", sa.Text()),
        sa.Column("is_hit", sa.Boolean()),
        sa.Column("is_hr", sa.Boolean()),
        sa.Column("is_bb", sa.Boolean()),
        sa.Column("xwoba", sa.Numeric(5, 3)),
        sa.Column("bases_state_before", sa.Text()),
        sa.Column("outs_before", sa.Integer()),
        sa.Column("score_diff_before", sa.Integer()),
        sa.Column("bat_order", sa.Integer()),
        sa.Column("is_risp", sa.Boolean()),
    )
    op.create_index("idx_pa_facts_batter", "pa_facts", ["batter_id"])
    op.create_index("idx_pa_facts_pitcher", "pa_facts", ["pitcher_id"])
    op.create_index("idx_pa_facts_game", "pa_facts", ["game_id"])

    op.create_table(
        "team_market_context",
        sa.Column("team_id", sa.Integer(), sa.ForeignKey("teams.team_id"), primary_key=True),
        sa.Column("season_year", sa.Integer(), nullable=False),
        sa.Column("attendance_score", sa.Numeric(5, 3)),
        sa.Column("media_score", sa.Numeric(5, 3)),
        sa.Column("market_weight", sa.Numeric(5, 3)),
        sa.Column("market_weight_adj", sa.Numeric(5, 3)),
    )

    op.create_table(
        "pattern_templates",
        sa.Column("pattern_id", sa.Text(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description_template", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("base_table", sa.Text(), nullable=False),
        sa.Column("category", sa.Text()),
        sa.Column("enabled", sa.Boolean(), server_default=sa.sql.expression.true(), nullable=False),
        sa.Column("filters_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("order_direction", sa.Text(), nullable=False),
        sa.Column("metric", sa.Text(), nullable=False),
        sa.Column("metric_expr", sa.Text()),
        sa.Column("target_sample", sa.Integer()),
        sa.Column("min_sample", sa.Integer(), server_default="10", nullable=False),
        sa.Column("unicorn_weight", sa.Numeric(4, 2), server_default="1.0", nullable=False),
        sa.Column("public_weight", sa.Numeric(4, 2), server_default="1.0", nullable=False),
        sa.Column("complexity_score", sa.Integer(), nullable=False),
        sa.Column("requires_count", sa.Boolean(), server_default=sa.sql.expression.false(), nullable=False),
        sa.Column("count_value", sa.Text()),
        sa.CheckConstraint(
            "count_value IS NULL OR count_value IN ('3-0','0-2','3-2')",
            name="ck_pattern_templates_count_value",
        ),
    )
    op.create_index("idx_pattern_enabled", "pattern_templates", ["enabled"])

    op.create_table(
        "unicorn_results",
        sa.Column("run_date", sa.Date(), primary_key=True),
        sa.Column("pattern_id", sa.Text(), sa.ForeignKey("pattern_templates.pattern_id"), primary_key=True),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_id", sa.BigInteger(), primary_key=True),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("metric_value", sa.Numeric(14, 6), nullable=False),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("z_raw", sa.Numeric(14, 6)),
        sa.Column("z_adjusted", sa.Numeric(14, 6)),
        sa.Column("score", sa.Numeric(14, 6), nullable=False),
    )
    op.create_index("idx_unicorn_results_date", "unicorn_results", ["run_date"])
    op.create_index("idx_unicorn_results_score", "unicorn_results", ["run_date", "score"], unique=False)

    op.create_table(
        "unicorn_top50_daily",
        sa.Column("run_date", sa.Date(), primary_key=True),
        sa.Column("rank", sa.Integer(), primary_key=True),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_id", sa.BigInteger(), nullable=False),
        sa.Column("pattern_id", sa.Text(), nullable=False),
        sa.Column("metric_value", sa.Numeric(14, 6), nullable=False),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("score", sa.Numeric(14, 6), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
    )
    op.create_index("idx_top50_player", "unicorn_top50_daily", ["run_date", "entity_id"])


def downgrade() -> None:
    op.drop_index("idx_top50_player", table_name="unicorn_top50_daily")
    op.drop_table("unicorn_top50_daily")

    op.drop_index("idx_unicorn_results_score", table_name="unicorn_results")
    op.drop_index("idx_unicorn_results_date", table_name="unicorn_results")
    op.drop_table("unicorn_results")

    op.drop_index("idx_pattern_enabled", table_name="pattern_templates")
    op.drop_table("pattern_templates")

    op.drop_table("team_market_context")

    op.drop_index("idx_pa_facts_game", table_name="pa_facts")
    op.drop_index("idx_pa_facts_pitcher", table_name="pa_facts")
    op.drop_index("idx_pa_facts_batter", table_name="pa_facts")
    op.drop_table("pa_facts")

    op.drop_index("idx_pitch_facts_count_str", table_name="pitch_facts")
    op.drop_index("idx_pitch_facts_game_date", table_name="pitch_facts")
    op.drop_index("idx_pitch_facts_pitcher", table_name="pitch_facts")
    op.drop_index("idx_pitch_facts_batter", table_name="pitch_facts")
    op.drop_table("pitch_facts")

    op.drop_table("games")
    op.drop_table("players")
    op.drop_table("teams")
