#!/usr/bin/env python3
"""Calibrate FanGraphs-free wRC+ using Statcast play-by-play."""

from __future__ import annotations

import argparse
import json
import sys
import unicodedata
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from backend.app.tools.wrc_plus import (  # noqa: E402
    CalibrationTarget,
    WRCPlusConfig,
    aggregate_hitters,
    build_leaderboard,
    build_plate_appearances,
    calibrate_wrc_plus,
    compute_league_context,
    load_statcast_data,
    lookup_player_id,
    summarize_leaderboard,
    write_constants,
)


def _load_park_factors(path: str | None) -> dict[str, float] | None:
    if not path:
        return None
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {str(k): float(v) for k, v in data.items()}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calibrate FanGraphs-free wRC+.")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--start-date", default="2025-03-27")
    parser.add_argument("--end-date", default="2025-04-20")
    parser.add_argument("--min-pa", type=int, default=50)
    parser.add_argument("--game-type", default="R")
    parser.add_argument("--target-player-id", type=int, default=None)
    parser.add_argument("--target-first", default="Fernando")
    parser.add_argument("--target-last", default="Tatis")
    parser.add_argument("--target-wrc-plus", type=float, default=210.0)
    parser.add_argument(
        "--use-target",
        action="store_true",
        help="Enable optional player anchor (default off).",
    )
    parser.add_argument("--cache-dir", default="data/cache/wrc_plus")
    parser.add_argument("--output-dir", default="data/outputs")
    parser.add_argument("--park-factors", default=None)
    parser.add_argument("--enable-park", action="store_true")
    return parser.parse_args()


def _resolve_target_player_id(
    hitters: pd.DataFrame,
    *,
    target_player_id: int | None,
    first: str,
    last: str,
) -> int:
    def _normalize(value: str) -> str:
        return (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
            .lower()
            .replace(" jr.", "")
        )

    if target_player_id:
        return target_player_id
    try:
        return lookup_player_id(first, last)
    except Exception:
        full = _normalize(f"{first} {last}".strip())
        normalized = hitters["name"].fillna("").map(_normalize)
        match = hitters[normalized == full]
        if match.empty:
            match = hitters[normalized.str.contains(_normalize(last))]
        if match.empty:
            raise RuntimeError(f"Target player not found for {first} {last}.")
        return int(match.iloc[0]["player_id"])


def main() -> None:
    args = _parse_args()
    config = WRCPlusConfig(
        season=args.season,
        start_date=args.start_date,
        end_date=args.end_date,
        min_pa=args.min_pa,
        game_type=args.game_type,
        cache_dir=Path(args.cache_dir),
        output_dir=Path(args.output_dir),
    )
    park_factors = _load_park_factors(args.park_factors)

    raw = load_statcast_data(
        config.start_date,
        config.end_date,
        config.cache_dir,
        allow_fetch=True,
    )
    pa_df = build_plate_appearances(raw, game_type=config.game_type)
    league_ctx = compute_league_context(pa_df)
    hitters = aggregate_hitters(pa_df, config.cache_dir)

    target = None
    if args.use_target:
        target_player_id = _resolve_target_player_id(
            hitters,
            target_player_id=args.target_player_id,
            first=args.target_first,
            last=args.target_last,
        )
        target = CalibrationTarget(
            player_id=target_player_id,
            target_wrc_plus=args.target_wrc_plus,
        )

    calibration = calibrate_wrc_plus(
        hitters,
        league_ctx,
        target,
        min_pa=config.min_pa,
        park_factors=park_factors,
        enable_park=args.enable_park,
    )
    if not calibration.success:
        raise RuntimeError(f"Calibration failed: {calibration.message}")

    leaderboard = build_leaderboard(
        hitters,
        league_ctx,
        calibration,
        park_factors=park_factors,
    )

    tatis_val = None
    if target is not None:
        tatis_val = float(
            leaderboard.loc[leaderboard["player_id"] == target.player_id, "wRC_plus"].iloc[0]
        )
    mean_wrc = float(
        (leaderboard["wRC_plus"] * leaderboard["PA"]).sum() / leaderboard["PA"].sum()
    )
    if target is not None and tatis_val is not None and abs(tatis_val - target.target_wrc_plus) > 0.5:
        raise RuntimeError(
            f"Target wRC+ {tatis_val:.2f} does not match {target.target_wrc_plus}."
        )
    if not (99.0 <= mean_wrc <= 101.0):
        raise RuntimeError(f"League mean wRC+ {mean_wrc:.2f} outside [99, 101].")

    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    leaderboard_path = output_dir / (
        f"wrc_plus_leaderboard_{config.season}_{config.start_date}_{config.end_date}.csv"
    )
    constants_path = output_dir / (
        f"wrc_plus_constants_{config.season}_{config.start_date}_{config.end_date}.json"
    )

    columns = [
        "player_id",
        "name",
        "team",
        "PA",
        "runs_per_pa",
        "wRAA",
        "wRC",
        "wRC_plus",
    ]
    leaderboard[columns].sort_values("wRC_plus", ascending=False).to_csv(
        leaderboard_path,
        index=False,
    )

    write_constants(
        constants_path,
        config=config,
        target=target,
        calibration=calibration,
        data_source="pybaseball.statcast",
    )

    summary = summarize_leaderboard(leaderboard, min_pa=config.min_pa)
    print(f"constants_path={constants_path}")
    print(f"leaderboard_path={leaderboard_path}")
    if tatis_val is not None:
        print(f"target_wrc_plus={tatis_val:.2f}")
    print(f"mean_wrc_plus={mean_wrc:.2f}")
    print(f"qualified_count={summary['qualified_count']}")
    qualified = leaderboard[leaderboard["PA"] >= config.min_pa]
    display_df = qualified if not qualified.empty else leaderboard
    print("top_20:")
    print(
        display_df[columns]
        .sort_values("wRC_plus", ascending=False)
        .head(20)
        .to_string(index=False)
    )
    print("bottom_20:")
    print(
        display_df[columns]
        .sort_values("wRC_plus", ascending=True)
        .head(20)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
