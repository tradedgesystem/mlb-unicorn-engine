from pathlib import Path

import pandas as pd

from backend.app.tools.wrc_plus import (
    CalibrationResult,
    WRCPlusConfig,
    aggregate_hitters,
    build_leaderboard,
    build_plate_appearances,
    compute_league_context,
    load_constants,
    load_statcast_data,
)


def _find_constants_path() -> Path:
    output_dir = Path("data/outputs")
    matches = sorted(output_dir.glob("wrc_plus_constants_*.json"))
    if not matches:
        raise AssertionError(
            "No wRC+ constants found. Run scripts/calibrate_wrc_plus.py first."
        )
    return max(matches, key=lambda p: p.stat().st_mtime)


def _load_inputs():
    constants_path = _find_constants_path()
    payload = load_constants(constants_path)
    cfg = payload["config"]
    config = WRCPlusConfig(
        season=cfg["season"],
        start_date=cfg["start_date"],
        end_date=cfg["end_date"],
        min_pa=cfg["min_pa"],
        game_type=cfg["game_type"],
        cache_dir=Path(cfg["cache_dir"]),
        output_dir=Path(cfg["output_dir"]),
    )
    calibration = CalibrationResult(**payload["calibration"])
    target = payload["target"]
    raw = load_statcast_data(
        config.start_date,
        config.end_date,
        config.cache_dir,
        allow_fetch=False,
    )
    pa_df = build_plate_appearances(raw, game_type=config.game_type)
    league_ctx = compute_league_context(pa_df)
    hitters = aggregate_hitters(pa_df, config.cache_dir)
    return config, calibration, target, hitters, league_ctx


def test_wrc_plus_deterministic():
    config, calibration, _, hitters, league_ctx = _load_inputs()
    first = build_leaderboard(hitters, league_ctx, calibration).sort_values("player_id")
    second = build_leaderboard(hitters, league_ctx, calibration).sort_values("player_id")
    pd.testing.assert_series_equal(
        first["wRC_plus"].reset_index(drop=True),
        second["wRC_plus"].reset_index(drop=True),
    )


def test_wrc_plus_target_and_mean():
    config, calibration, target, hitters, league_ctx = _load_inputs()
    leaderboard = build_leaderboard(hitters, league_ctx, calibration)
    tatis_val = float(
        leaderboard.loc[leaderboard["player_id"] == target["player_id"], "wRC_plus"].iloc[0]
    )
    assert abs(tatis_val - target["target_wrc_plus"]) <= 0.5
    mean_wrc = float(
        (leaderboard["wRC_plus"] * leaderboard["PA"]).sum() / leaderboard["PA"].sum()
    )
    assert 99.0 <= mean_wrc <= 101.0


def test_wrc_plus_validity():
    config, calibration, _, hitters, league_ctx = _load_inputs()
    leaderboard = build_leaderboard(hitters, league_ctx, calibration)
    assert leaderboard["wRC_plus"].notna().all()
    qualified = leaderboard[leaderboard["PA"] >= config.min_pa]
    assert len(qualified) >= 20
