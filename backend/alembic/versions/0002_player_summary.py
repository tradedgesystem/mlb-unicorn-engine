"""add player_summary table for predictive metrics"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0002_player_summary"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "player_summary",
        sa.Column("player_id", sa.BigInteger(), sa.ForeignKey("players.player_id"), primary_key=True),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("barrel_pct_last_50", sa.Numeric(6, 4)),
        sa.Column("hard_hit_pct_last_50", sa.Numeric(6, 4)),
        sa.Column("xwoba_last_50", sa.Numeric(6, 4)),
        sa.Column("contact_pct_last_50", sa.Numeric(6, 4)),
        sa.Column("chase_pct_last_50", sa.Numeric(6, 4)),
        sa.Column("xwoba_last_3_starts", sa.Numeric(6, 4)),
        sa.Column("whiff_pct_last_3_starts", sa.Numeric(6, 4)),
        sa.Column("k_pct_last_3_starts", sa.Numeric(6, 4)),
        sa.Column("bb_pct_last_3_starts", sa.Numeric(6, 4)),
        sa.Column("hard_hit_pct_last_3_starts", sa.Numeric(6, 4)),
        sa.Column("xwoba_last_5_apps", sa.Numeric(6, 4)),
        sa.Column("whiff_pct_last_5_apps", sa.Numeric(6, 4)),
        sa.Column("k_pct_last_5_apps", sa.Numeric(6, 4)),
        sa.Column("bb_pct_last_5_apps", sa.Numeric(6, 4)),
        sa.Column("hard_hit_pct_last_5_apps", sa.Numeric(6, 4)),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("player_summary")
