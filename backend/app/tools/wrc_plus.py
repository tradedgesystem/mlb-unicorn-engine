"""FanGraphs-free wRC+ calculator built on Statcast play-by-play.

This module uses pybaseball Statcast data to compute RE24, aggregate hitter run
values per PA, and calibrate a small set of parameters either with or without
an anchor player target. It never calls FanGraphs and is fully reproducible
when cached inputs/constants are available.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd
from pybaseball import playerid_lookup, playerid_reverse_lookup, statcast
from scipy.optimize import least_squares

DATE_FORMAT = "%Y-%m-%d"


@dataclass(frozen=True)
class WRCPlusConfig:
    season: int
    start_date: str
    end_date: str
    min_pa: int = 50
    game_type: str = "R"
    cache_dir: Path = Path("data/cache/wrc_plus")
    output_dir: Path = Path("data/outputs")


@dataclass(frozen=True)
class CalibrationTarget:
    player_id: int
    target_wrc_plus: float


@dataclass(frozen=True)
class CalibrationResult:
    alpha: float
    beta: float
    pf_gamma: float
    success: bool
    message: str
    nfev: int
    loss: float


def load_statcast_data(
    start_date: str,
    end_date: str,
    cache_dir: Path | str,
    *,
    allow_fetch: bool = True,
) -> pd.DataFrame:
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"statcast_{start_date}_{end_date}.csv.gz"
    if cache_path.exists():
        return pd.read_csv(cache_path)
    if not allow_fetch:
        raise RuntimeError(
            f"Statcast cache missing at {cache_path}. Run calibration to fetch."
        )
    data = statcast(start_dt=start_date, end_dt=end_date)
    data.to_csv(cache_path, index=False, compression="gzip")
    return data


def _filter_plate_appearances(df: pd.DataFrame, game_type: str) -> pd.DataFrame:
    required = {
        "events",
        "batter",
        "game_pk",
        "at_bat_number",
        "pitch_number",
        "outs_when_up",
        "inning",
        "inning_topbot",
        "on_1b",
        "on_2b",
        "on_3b",
        "bat_score",
        "post_bat_score",
        "home_team",
        "away_team",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"Statcast data missing columns: {missing}")

    filtered = df[df["events"].notna() & df["batter"].notna()].copy()
    if "game_type" in filtered.columns and game_type:
        filtered = filtered[filtered["game_type"] == game_type]
    if filtered.empty:
        raise RuntimeError("No plate appearances found after filtering.")

    filtered = filtered.sort_values(
        ["game_pk", "at_bat_number", "pitch_number"]
    ).drop_duplicates(subset=["game_pk", "at_bat_number"], keep="last")

    filtered["batter"] = filtered["batter"].astype(int)
    filtered["outs_when_up"] = filtered["outs_when_up"].astype(int)
    filtered["on_1b"] = filtered["on_1b"].notna().astype(int)
    filtered["on_2b"] = filtered["on_2b"].notna().astype(int)
    filtered["on_3b"] = filtered["on_3b"].notna().astype(int)
    filtered["bat_score"] = pd.to_numeric(filtered["bat_score"], errors="coerce")
    filtered["post_bat_score"] = pd.to_numeric(
        filtered["post_bat_score"], errors="coerce"
    )
    filtered["bat_team"] = np.where(
        filtered["inning_topbot"] == "Top",
        filtered["away_team"],
        filtered["home_team"],
    )
    filtered["runs_scored"] = (
        filtered["post_bat_score"] - filtered["bat_score"]
    ).fillna(0)
    filtered["half_inning_id"] = (
        filtered["game_pk"].astype(str)
        + "_"
        + filtered["inning"].astype(str)
        + "_"
        + filtered["inning_topbot"].astype(str)
    )
    return filtered


def _compute_run_expectancy(pa_df: pd.DataFrame) -> pd.DataFrame:
    if pa_df.empty:
        raise RuntimeError("No plate appearance data for run expectancy.")

    final_scores = (
        pa_df.groupby("half_inning_id")["post_bat_score"].max().rename("final_score")
    )
    pa_df = pa_df.join(final_scores, on="half_inning_id")
    pa_df["runs_to_end"] = (pa_df["final_score"] - pa_df["bat_score"]).fillna(0)

    re_table = (
        pa_df.groupby(
            ["outs_when_up", "on_1b", "on_2b", "on_3b"], as_index=False
        )["runs_to_end"]
        .mean()
        .rename(columns={"runs_to_end": "re_before"})
    )
    return re_table


def _add_re_after(pa_df: pd.DataFrame, re_table: pd.DataFrame) -> pd.DataFrame:
    pa_df = pa_df.copy()
    pa_df = pa_df.sort_values(
        ["half_inning_id", "at_bat_number", "pitch_number"]
    )
    grouped = pa_df.groupby("half_inning_id", sort=False)
    pa_df["next_outs"] = grouped["outs_when_up"].shift(-1)
    pa_df["next_on_1b"] = grouped["on_1b"].shift(-1)
    pa_df["next_on_2b"] = grouped["on_2b"].shift(-1)
    pa_df["next_on_3b"] = grouped["on_3b"].shift(-1)

    re_after = re_table.rename(
        columns={
            "outs_when_up": "next_outs",
            "on_1b": "next_on_1b",
            "on_2b": "next_on_2b",
            "on_3b": "next_on_3b",
            "re_before": "re_after",
        }
    )
    pa_df = pa_df.merge(
        re_after,
        on=["next_outs", "next_on_1b", "next_on_2b", "next_on_3b"],
        how="left",
    )
    pa_df["re_after"] = pa_df["re_after"].fillna(0)
    return pa_df


def build_plate_appearances(
    df: pd.DataFrame,
    game_type: str,
) -> pd.DataFrame:
    pa_df = _filter_plate_appearances(df, game_type)
    re_table = _compute_run_expectancy(pa_df)
    pa_df = pa_df.merge(
        re_table,
        on=["outs_when_up", "on_1b", "on_2b", "on_3b"],
        how="left",
    )
    if pa_df["re_before"].isna().any():
        raise RuntimeError("Run expectancy lookup failed for some PA states.")
    pa_df = _add_re_after(pa_df, re_table)
    pa_df["run_value"] = pa_df["runs_scored"] + (pa_df["re_after"] - pa_df["re_before"])
    return pa_df


def compute_league_context(pa_df: pd.DataFrame) -> dict[str, float]:
    total_pa = len(pa_df)
    if total_pa == 0:
        raise RuntimeError("No plate appearances available for league context.")
    total_runs = pa_df["runs_scored"].sum()
    total_run_value = pa_df["run_value"].sum()
    return {
        "total_pa": float(total_pa),
        "lg_runs_per_pa_actual": float(total_runs / total_pa),
        "lg_runs_per_pa_outcomes": float(total_run_value / total_pa),
    }


def _load_player_lookup(cache_dir: Path) -> pd.DataFrame:
    cache_path = cache_dir / "player_lookup.csv"
    if cache_path.exists():
        return pd.read_csv(cache_path)
    return pd.DataFrame(columns=["key_mlbam", "name_first", "name_last"])


def _save_player_lookup(cache_dir: Path, df: pd.DataFrame) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "player_lookup.csv"
    df.to_csv(cache_path, index=False)


def _resolve_player_names(player_ids: Iterable[int], cache_dir: Path) -> pd.DataFrame:
    cache_df = _load_player_lookup(cache_dir)
    cached_ids = set(cache_df["key_mlbam"].dropna().astype(int).tolist())
    missing = sorted(set(int(pid) for pid in player_ids) - cached_ids)
    if missing:
        fetched = playerid_reverse_lookup(missing, key_type="mlbam")
        if not fetched.empty:
            fetched = fetched[["key_mlbam", "name_first", "name_last"]]
            cache_df = pd.concat([cache_df, fetched], ignore_index=True)
            cache_df = cache_df.drop_duplicates(subset=["key_mlbam"])
            _save_player_lookup(cache_dir, cache_df)
    return cache_df


def aggregate_hitters(pa_df: pd.DataFrame, cache_dir: Path | str) -> pd.DataFrame:
    cache_dir = Path(cache_dir)
    hitter_runs = (
        pa_df.groupby("batter")["run_value"]
        .sum()
        .rename("run_value_total")
    )
    hitter_pa = pa_df.groupby("batter").size().rename("PA")
    hitter_runs_per_pa = (hitter_runs / hitter_pa).rename("runs_per_pa")
    hitter_team = (
        pa_df.groupby("batter")["bat_team"]
        .agg(lambda s: s.value_counts().index[0])
        .rename("team")
    )
    hitters = pd.concat([hitter_pa, hitter_runs, hitter_runs_per_pa, hitter_team], axis=1)
    hitters = hitters.reset_index().rename(columns={"batter": "player_id"})

    lookup = _resolve_player_names(hitters["player_id"], cache_dir)
    lookup["key_mlbam"] = lookup["key_mlbam"].astype(int)
    lookup["name"] = lookup["name_first"].fillna("") + " " + lookup["name_last"].fillna("")
    hitters = hitters.merge(
        lookup[["key_mlbam", "name"]],
        left_on="player_id",
        right_on="key_mlbam",
        how="left",
    ).drop(columns=["key_mlbam"])
    return hitters


def compute_wrc_plus(
    hitters: pd.DataFrame,
    league_ctx: dict[str, float],
    *,
    alpha: float,
    beta: float,
    pf_gamma: float = 0.0,
    park_factors: Optional[dict[str, float]] = None,
) -> pd.DataFrame:
    if park_factors is None:
        park_factors = {}

    df = hitters.copy()
    df["park_factor"] = df["team"].map(park_factors).fillna(1.0)
    df["adj_runs_per_pa"] = alpha * df["runs_per_pa"] + beta
    df["adj_lg_runs_per_pa_actual"] = alpha * league_ctx["lg_runs_per_pa_actual"] + beta
    df["adj_lg_runs_per_pa_outcomes"] = alpha * league_ctx["lg_runs_per_pa_outcomes"] + beta
    df["wRAA"] = (df["adj_runs_per_pa"] - df["adj_lg_runs_per_pa_outcomes"]) * df["PA"]
    df["wRC"] = df["wRAA"] + df["adj_lg_runs_per_pa_actual"] * df["PA"]
    df["park_term"] = (
        pf_gamma * (df["park_factor"] - 1.0) * df["adj_lg_runs_per_pa_actual"]
    )
    df["wRC_plus"] = 100 * (
        (df["adj_runs_per_pa"] - df["park_term"]) / df["adj_lg_runs_per_pa_outcomes"]
    )
    return df


def _distribution_penalty(df: pd.DataFrame, min_pa: int) -> float:
    qualified = df[df["PA"] >= min_pa]
    used_fallback = False
    if qualified.empty:
        qualified = df.sort_values("PA", ascending=False).head(150)
        used_fallback = True
    if qualified.empty:
        return 10.0
    if used_fallback or len(qualified) < 30:
        return 0.0
    wrc = qualified["wRC_plus"].dropna()
    if wrc.empty:
        return 10.0
    p_hi = float((wrc > 200).mean())
    p_lo = float((wrc < 50).mean())
    stdev = float(wrc.std())

    def _range_penalty(value: float, low: float, high: float) -> float:
        if value < low:
            return low - value
        if value > high:
            return value - high
        return 0.0

    p_hi_pen = _range_penalty(p_hi, 0.005, 0.02)
    p_lo_pen = _range_penalty(p_lo, 0.01, 0.05)
    stdev_pen = _range_penalty(stdev, 20.0, 40.0)
    return p_hi_pen**2 + p_lo_pen**2 + stdev_pen**2


def calibrate_wrc_plus(
    hitters: pd.DataFrame,
    league_ctx: dict[str, float],
    target: CalibrationTarget | None,
    *,
    min_pa: int,
    park_factors: Optional[dict[str, float]] = None,
    enable_park: bool = False,
) -> CalibrationResult:
    if target is not None and target.player_id not in set(hitters["player_id"].astype(int).tolist()):
        raise RuntimeError(f"Target player_id {target.player_id} not found in data.")

    lg_outcomes = league_ctx["lg_runs_per_pa_outcomes"]

    def residuals(params: np.ndarray) -> np.ndarray:
        alpha = params[0]
        beta_offset = params[1]
        beta = beta_offset - alpha * lg_outcomes
        pf_gamma = params[2] if enable_park else 0.0
        df = compute_wrc_plus(
            hitters,
            league_ctx,
            alpha=alpha,
            beta=beta,
            pf_gamma=pf_gamma,
            park_factors=park_factors,
        )
        target_val = None
        if target is not None:
            target_val = float(
                df.loc[df["player_id"] == target.player_id, "wRC_plus"].iloc[0]
            )
        mean_wrc = float((df["wRC_plus"] * df["PA"]).sum() / df["PA"].sum())
        dist_pen = _distribution_penalty(df, min_pa=min_pa)
        reg_pen = (alpha - 1.0) ** 2 + beta**2 + pf_gamma**2

        losses = []
        if target is not None and target_val is not None:
            losses.append(50.0 * (target_val - target.target_wrc_plus))
        losses.extend(
            [
                2.0 * (mean_wrc - 100.0),
                0.1 * dist_pen,
                0.1 * reg_pen,
            ]
        )
        return np.array(losses)

    x0 = np.array([1.0, 0.1, 0.0])
    bounds = (
        np.array([0.25, 1e-4, -1.0]),
        np.array([3.0, 1.0, 1.0]),
    )
    result = least_squares(
        residuals,
        x0,
        bounds=bounds,
        xtol=1e-6,
        ftol=1e-6,
        gtol=1e-6,
        max_nfev=2000,
    )
    alpha = float(result.x[0])
    beta_offset = float(result.x[1])
    beta = beta_offset - alpha * lg_outcomes
    return CalibrationResult(
        alpha=alpha,
        beta=float(beta),
        pf_gamma=float(result.x[2]) if enable_park else 0.0,
        success=bool(result.success),
        message=str(result.message),
        nfev=int(result.nfev),
        loss=float(result.cost),
    )


def lookup_player_id(first: str, last: str) -> int:
    result = playerid_lookup(last, first)
    if result.empty and "jr" not in last.lower():
        result = playerid_lookup(f"{last} Jr.", first)
    if result.empty:
        raise RuntimeError(f"No player found for {first} {last}.")
    if "key_mlbam" not in result.columns:
        raise RuntimeError("Player lookup missing MLBAM IDs.")
    return int(result.iloc[0]["key_mlbam"])


def write_constants(
    path: Path | str,
    *,
    config: WRCPlusConfig,
    target: CalibrationTarget | None,
    calibration: CalibrationResult,
    data_source: str,
) -> None:
    config_payload = asdict(config)
    config_payload["cache_dir"] = str(config.cache_dir)
    config_payload["output_dir"] = str(config.output_dir)
    payload: dict[str, Any] = {
        "config": config_payload,
        "target": asdict(target) if target is not None else None,
        "calibration": asdict(calibration),
        "data_source": data_source,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_constants(path: Path | str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def build_leaderboard(
    hitters: pd.DataFrame,
    league_ctx: dict[str, float],
    calibration: CalibrationResult,
    *,
    park_factors: Optional[dict[str, float]] = None,
) -> pd.DataFrame:
    return compute_wrc_plus(
        hitters,
        league_ctx,
        alpha=calibration.alpha,
        beta=calibration.beta,
        pf_gamma=calibration.pf_gamma,
        park_factors=park_factors,
    )


def summarize_leaderboard(df: pd.DataFrame, min_pa: int) -> dict[str, float]:
    qualified = df[df["PA"] >= min_pa]
    return {
        "mean_wrc_plus": float((df["wRC_plus"] * df["PA"]).sum() / df["PA"].sum()),
        "qualified_count": int(len(qualified)),
        "qualified_mean": float(qualified["wRC_plus"].mean()) if not qualified.empty else 0.0,
    }
