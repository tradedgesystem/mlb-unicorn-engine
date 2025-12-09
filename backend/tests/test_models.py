from backend.app.db.base import Base
from backend.app.db import models


def test_tables_registered():
    tables = Base.metadata.tables
    expected = {
        "players",
        "teams",
        "games",
        "pitch_facts",
        "pa_facts",
        "team_market_context",
        "pattern_templates",
        "unicorn_results",
        "unicorn_top50_daily",
    }
    assert expected.issubset(set(tables.keys()))
