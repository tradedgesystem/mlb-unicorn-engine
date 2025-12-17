from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.app.tools.generate_site_data_product import validate_data_product_dir


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_validate_data_product_minimal_pass(tmp_path: Path) -> None:
    teams = [{"team_id": i, "abbreviation": f"T{i:02d}"} for i in range(1, 31)]
    _write_json(tmp_path / "teams.json", teams)
    for t in teams:
        tid = t["team_id"]
        _write_json(
            tmp_path / "teams" / f"{tid}.json",
            {
                "team_id": tid,
                "abbreviation": t["abbreviation"],
                "hitters": [],
                "starters": [],
                "relievers": [],
            },
        )
    _write_json(tmp_path / "unicorns.json", [])
    _write_json(tmp_path / "players_index.json", [])
    _write_json(
        tmp_path / "meta.json",
        {
            "last_updated": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "snapshot_date": "2025-01-01",
            "shuffle_seed_date": "2025-01-01",
            "counts": {"teams_count": 30, "players_count": 0, "unicorns_count": 0},
        },
    )

    validate_data_product_dir(tmp_path)


def test_validate_data_product_fails_on_wrong_team_count(tmp_path: Path) -> None:
    _write_json(tmp_path / "teams.json", [{"team_id": 1, "abbreviation": "A"}])
    _write_json(tmp_path / "unicorns.json", [])
    _write_json(tmp_path / "players_index.json", [])
    _write_json(
        tmp_path / "meta.json",
        {
            "last_updated": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "snapshot_date": "2025-01-01",
            "shuffle_seed_date": "2025-01-01",
            "counts": {"teams_count": 1, "players_count": 0, "unicorns_count": 0},
        },
    )
    with pytest.raises(ValueError):
        validate_data_product_dir(tmp_path)

