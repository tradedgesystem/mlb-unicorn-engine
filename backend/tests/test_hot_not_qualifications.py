from __future__ import annotations

from backend.app.tools.generate_site_data_product import _build_hot_not_feed


def test_hot_not_feed_filters_unqualified_hitters() -> None:
    # Bench player has extreme (would always win) but is unqualified (<50 AB).
    bench_player = {
        "player_id": 1,
        "player_name": "Bench Guy",
        "position": "OF",
        "metrics": {
            "xwoba_last_50": 0.0,
            "barrel_pct_last_50": 1.0,
            "hard_hit_pct_last_50": 1.0,
            "contact_pct_last_50": 1.0,
            "chase_pct_last_50": 0.0,
        },
        "sample": {"ab_count_last_50": 10},
    }
    qualified_player = {
        "player_id": 2,
        "player_name": "Regular Guy",
        "position": "OF",
        "metrics": {
            "xwoba_last_50": 0.333,
            "barrel_pct_last_50": 0.1,
            "hard_hit_pct_last_50": 0.4,
            "contact_pct_last_50": 0.75,
            "chase_pct_last_50": 0.25,
        },
        "sample": {"ab_count_last_50": 50},
    }

    team_details = {
        123: {
            "team_id": 123,
            "abbrev": "TST",
            "hitters": [bench_player, qualified_player],
            "starters": [],
            "relievers": [],
        }
    }

    items = _build_hot_not_feed(team_details, snapshot_date="2025-01-01")
    assert items, "expected at least one feed item"
    assert all(item["player_id"] != 1 for item in items), "unqualified hitter should not appear in feed"

