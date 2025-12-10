"""Backfill orchestration using pybaseball Statcast as the data source."""
from __future__ import annotations

import argparse
from datetime import date
from typing import Dict, Iterable, List, Mapping, MutableMapping

import pandas as pd
from backend.app.etl.preprocess import sanitize_value
from pybaseball import statcast

from backend.app.core.logging import logger
from backend.app.db.session import SessionLocal
from backend.app.etl.loader import StatcastLoader
from backend.app.etl import preprocess

# MLB team abbreviation -> MLBAM team_id (all 30 clubs)
TEAM_ABBR_TO_ID: Dict[str, int] = {
    # 3-letter official
    "ARI": 109,
    "ATL": 144,
    "BAL": 110,
    "BOS": 111,
    "CHC": 112,
    "CIN": 113,
    "CLE": 114,
    "COL": 115,
    "CWS": 145,
    "DET": 116,
    "HOU": 117,
    "KCR": 118,
    "LAD": 119,
    "MIA": 146,
    "MIL": 158,
    "MIN": 142,
    "NYM": 121,
    "NYY": 147,
    "OAK": 133,
    "PHI": 143,
    "PIT": 134,
    "SDP": 135,
    "SEA": 136,
    "SFG": 137,
    "STL": 138,
    "TBR": 139,
    "TEX": 140,
    "TOR": 141,
    "WSN": 120,
    # Common aliases / 2-letter variants
    "AZ": 109,
    "TB": 139,
    "KC": 118,
    "SF": 137,
    "SD": 135,
    "LA": 119,
    "NY": 147,  # only if emitted
    "CH": 112,  # only if emitted
    "CWS": 145,
    "CHW": 145,
    "LAA": 108,
    "KC": 118,
    "TB": 139,
    "WSH": 120,
    "WAS": 120,
    "ATH": 133,
}

HIT_EVENTS = {"single", "double", "triple", "home_run"}
WALK_EVENTS = {"walk", "intent_walk"}


def _safe_int(value) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _team_id(abbrev: str | None) -> int | None:
    if not abbrev:
        return None
    team_id = TEAM_ABBR_TO_ID.get(abbrev)
    if team_id is not None:
        return team_id
    if abbrev in ("ATH", "OAKL", "OAKLAND"):
        return TEAM_ABBR_TO_ID["OAK"]
    raise ValueError(f"Unknown team abbreviation: {abbrev}")


def _build_team_records(df: pd.DataFrame) -> List[Mapping]:
    teams = set(df["home_team"].dropna().unique()) | set(df["away_team"].dropna().unique())
    records: List[Mapping] = []
    for abbr in teams:
        if pd.isna(abbr):
            continue
        abbr_str = str(abbr)
        team_id = _team_id(abbr_str)
        records.append({"team_id": team_id, "team_name": abbr_str, "abbrev": abbr_str})
    return records


def _build_game_records(df: pd.DataFrame) -> List[Mapping]:
    games: List[Mapping] = []
    for game_id, gdf in df.groupby("game_pk"):
        first = gdf.iloc[0]
        home_abbr = str(first.get("home_team"))
        away_abbr = str(first.get("away_team"))
        games.append(
            {
                "game_id": int(game_id),
                "game_date": pd.to_datetime(first["game_date"]).date(),
                "home_team_id": _team_id(home_abbr),
                "away_team_id": _team_id(away_abbr),
                "venue_id": None,
                "is_day_game": None,
                "is_night_game": None,
            }
        )
    return games


def _aggregate_players(df: pd.DataFrame) -> List[Mapping]:
    players: Dict[int, MutableMapping] = {}

    for _, row in df.iterrows():
        batter_id = _safe_int(row.get("batter"))
        if batter_id and batter_id not in players:
            players[batter_id] = {
                "player_id": batter_id,
                "mlb_id": batter_id,
                "full_name": row.get("player_name") or str(batter_id),
                "bat_side": row.get("stand"),
                "throw_side": None,
                "primary_pos": None,
                "current_team_id": _team_id(row.get("bat_team")) or _team_id(row.get("home_team")) or _team_id(row.get("away_team")),
            }

        pitcher_id = _safe_int(row.get("pitcher"))
        if pitcher_id and pitcher_id not in players:
            players[pitcher_id] = {
                "player_id": pitcher_id,
                "mlb_id": pitcher_id,
                "full_name": row.get("pitcher_name") or str(pitcher_id),
                "bat_side": None,
                "throw_side": row.get("p_throws"),
                "primary_pos": "P",
                "current_team_id": _team_id(row.get("fld_team")) or _team_id(row.get("home_team")) or _team_id(row.get("away_team")),
            }
    return list(players.values())


def _build_pa_records(df: pd.DataFrame) -> List[Mapping]:
    pa_records: List[Mapping] = []
    for (game_pk, at_bat), g in df.groupby(["game_pk", "at_bat_number"]):
        g_sorted = g.sort_values("pitch_number_pa")
        first = g_sorted.iloc[0]
        last = g_sorted.iloc[-1]
        pa_id = int(f"{int(game_pk)}{int(at_bat):04d}")
        events = last.get("events")
        result_text = events or last.get("description")
        is_hit = bool(events in HIT_EVENTS)
        is_hr = bool(events == "home_run")
        is_bb = bool(events in WALK_EVENTS)

        record = {
            "pa_id": pa_id,
            "game_id": int(game_pk),
            "inning": _safe_int(first.get("inning")),
            "top_bottom": (first.get("inning_topbot") or "T")[0].upper(),
            "batter_id": _safe_int(first.get("batter")),
            "pitcher_id": _safe_int(first.get("pitcher")),
            "result": result_text,
            "is_hit": is_hit,
            "is_hr": is_hr,
            "is_bb": is_bb,
            "xwoba": last.get("estimated_woba_using_speedangle"),
            "bases_state_before": None,
            "outs_before": _safe_int(first.get("outs_when_up")),
            "score_diff_before": None,
            "bat_order": _safe_int(first.get("bat_order")),
            "is_risp": bool(_safe_int(first.get("on_2b")) or _safe_int(first.get("on_3b"))),
        }

        for k in ["xwoba", "result", "outs_before"]:
            record[k] = sanitize_value(record.get(k))

        record["batter_id"] = sanitize_value(record["batter_id"])
        record["pitcher_id"] = sanitize_value(record["pitcher_id"])
        record["game_id"] = sanitize_value(record["game_id"])
        record["pa_id"] = sanitize_value(record["pa_id"])

        pa_records.append(record)
    return pa_records


def _build_pitch_records(df: pd.DataFrame) -> List[Mapping]:
    pitch_records: List[Mapping] = []
    for _, row in df.iterrows():
        balls_ct = _safe_int(row.get("balls"))
        strikes_ct = _safe_int(row.get("strikes"))
        zone_val = row.get("zone")
        is_in_zone = None
        try:
            if zone_val == zone_val and int(zone_val) in range(1, 10):
                is_in_zone = True
            elif zone_val == zone_val:
                is_in_zone = False
        except (TypeError, ValueError):
            is_in_zone = None

        ls = row.get("launch_speed")
        is_hard_hit = (ls is not None and pd.notna(ls) and float(ls) >= 95.0)

        pitch = {
            "pitch_id": _safe_int(row.get("pitch_id")),
            "game_id": _safe_int(row.get("game_pk")),
            "pa_id": _safe_int(row.get("pa_id")),
            "inning": _safe_int(row.get("inning")),
            "top_bottom": (row.get("inning_topbot") or "T")[0].upper(),
            "batter_id": _safe_int(row.get("batter")),
            "pitcher_id": _safe_int(row.get("pitcher")),
            "pitch_number_pa": _safe_int(row.get("pitch_number_pa")),
            "pitch_number_game": _safe_int(row.get("game_pitch_number")),
            "pitch_type": row.get("pitch_type"),
            "vel": row.get("release_speed"),
            "spin_rate": row.get("release_spin_rate"),
            "count_balls_before": balls_ct,
            "count_strikes_before": strikes_ct,
            "is_in_zone": is_in_zone,
            "result_pitch": row.get("description"),
            "is_last_pitch_of_pa": bool(row.get("is_last_pitch")),
            "launch_speed": row.get("launch_speed"),
            "launch_angle": row.get("launch_angle"),
            "spray_angle": None,  # not available directly from statcast dataset
            "is_barrel": None,
            "is_hard_hit": is_hard_hit,
            "batted_ball_type": row.get("bb_type"),
            "hit_direction": preprocess.classify_batted_ball_direction(row.get("stand"), _safe_int(row.get("hit_location")), None),
            "loc_high_mid_low": None,  # filled in preprocess_pitch
            "loc_in_mid_out": None,  # filled in preprocess_pitch
            "loc_region": None,  # filled in preprocess_pitch
            "pa_outcome": row.get("events"),
            "is_hr": row.get("events") == "home_run",
            "is_hit": row.get("events") in HIT_EVENTS,
            "is_walk": row.get("events") in WALK_EVENTS,
            "count_str": f"{balls_ct}-{strikes_ct}" if balls_ct is not None and strikes_ct is not None else None,
            "stand": row.get("stand"),
            "hit_location": _safe_int(row.get("hit_location")),
            "plate_x": row.get("plate_x"),
            "plate_z": row.get("plate_z"),
            "description": row.get("description"),
        }
        if pitch["pitch_id"] is None:
            continue
        preprocess.preprocess_pitch(pitch)
        pitch_records.append(pitch)
    return pitch_records


def build_batches_for_date(as_of_date: date) -> Mapping[str, Iterable[Mapping]]:
    date_str = as_of_date.isoformat()
    df = statcast(start_dt=date_str, end_dt=date_str)
    if df.empty:
        logger.warning("No Statcast data for %s", as_of_date)
        return {"players": [], "teams": [], "games": [], "pitch_facts": [], "pa_facts": []}

    # pitch and PA identifiers
    df = df.copy()
    df["pitch_number_pa"] = df.groupby(["game_pk", "at_bat_number"]).cumcount() + 1
    df["game_pitch_number"] = df.groupby("game_pk").cumcount() + 1
    df["is_last_pitch"] = df.groupby(["game_pk", "at_bat_number"])["pitch_number_pa"].transform("max") == df["pitch_number_pa"]
    df["pitch_id"] = df.apply(lambda r: int(f"{int(r.game_pk)}{int(r.game_pitch_number):05d}"), axis=1)
    df["pa_id"] = df.apply(lambda r: int(f"{int(r.game_pk)}{int(r.at_bat_number):04d}"), axis=1)

    teams = _build_team_records(df)
    games = _build_game_records(df)
    players = _aggregate_players(df)
    pa_facts = _build_pa_records(df)
    pitch_facts = _build_pitch_records(df)
    return {
        "players": players,
        "teams": teams,
        "games": games,
        "pitch_facts": pitch_facts,
        "pa_facts": pa_facts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily Statcast backfill")
    parser.add_argument("--date", required=True, help="Single date (YYYY-MM-DD) to backfill")
    args = parser.parse_args()
    run_date = date.fromisoformat(args.date)

    batch = build_batches_for_date(run_date)
    with SessionLocal() as session:
        loader = StatcastLoader(session)
        loader.load_all(**batch)
    logger.info("Backfill complete for %s", run_date)


if __name__ == "__main__":
    main()
