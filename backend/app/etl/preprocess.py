"""Preprocessing helpers to derive directional/location fields before load."""
from __future__ import annotations

from typing import MutableMapping, Optional, Tuple

import numpy as np
import pandas as pd


def sanitize_value(v):
    """
    Convert pandas / numpy scalar objects to native Python types
    so SQLAlchemy can insert them into Postgres.
    """
    if v is None or pd.isna(v):
        return None
    if isinstance(v, (np.floating, np.float64, np.float32)):
        return float(v)
    if isinstance(v, (np.integer, np.int64, np.int32)):
        return int(v)
    return v

VERTICAL_PITCH_BOUNDS = (1.5, 2.5, 3.5)  # rough strike-zone thirds
HORIZONTAL_PITCH_BOUNDS = (-0.6, 0.0, 0.6)  # inside/mid/out buckets for plate_x


def bucket_pitch_vertical(z: Optional[float]) -> Optional[str]:
    import pandas as pd

    if z is None or pd.isna(z):
        return None
    z_float = float(z)
    if z_float < VERTICAL_PITCH_BOUNDS[0]:
        return "low"
    elif z_float < VERTICAL_PITCH_BOUNDS[1]:
        return "middle"
    else:
        return "high"


def bucket_pitch_horizontal(x: Optional[float]) -> Optional[str]:
    import pandas as pd

    if x is None or pd.isna(x):
        return None
    x_float = float(x)
    if x_float < HORIZONTAL_PITCH_BOUNDS[0]:
        return "inside"
    elif x_float < HORIZONTAL_PITCH_BOUNDS[1]:
        return "middle"
    else:
        return "outside"


def resolve_region(vertical: Optional[str], horizontal: Optional[str]) -> Optional[str]:
    if not vertical or not horizontal:
        return None
    return f"{vertical}_{horizontal}"


def derive_count_str(balls: Optional[int], strikes: Optional[int]) -> Optional[str]:
    if balls is None or strikes is None:
        return None
    return f"{balls}-{strikes}"


def classify_batted_ball_direction(
    stand: Optional[str], hit_location: Optional[int], spray_angle: Optional[float] = None
) -> Optional[str]:
    """
    Approximate batted-ball direction using hit_location (fielder positioning).
    hit_location values: 7=LF,8=CF,9=RF; 5/6 ~ left side, 3/4 ~ right side.
    """
    if hit_location is None:
        return None
    right_side = {3, 4, 9}
    left_side = {5, 6, 7}
    if hit_location == 8:
        return "center"
    if stand == "L":
        if hit_location in right_side:
            return "pull"
        if hit_location in left_side:
            return "oppo"
    else:  # default/right-handed
        if hit_location in left_side:
            return "pull"
        if hit_location in right_side:
            return "oppo"
    return "center"


def classify_pitch_region(plate_x: Optional[float], plate_z: Optional[float]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    v_bucket = bucket_pitch_vertical(plate_z)
    h_bucket = bucket_pitch_horizontal(plate_x)
    return v_bucket, h_bucket, resolve_region(v_bucket, h_bucket)


def classify_swing_or_take(description: Optional[str]) -> Optional[str]:
    if not description:
        return None
    desc = description.lower()
    swing_terms = {
        "swinging_strike",
        "foul",
        "foul_tip",
        "hit_into_play",
        "hit_into_play_no_out",
        "hit_into_play_score",
        "foul_bunt",
        "missed_bunt",
        "foul_pitchout",
    }
    if any(term in desc for term in swing_terms):
        return "swing"
    return "take"


def bucket_launch_angle(launch_angle: Optional[float]) -> Optional[str]:
    import pandas as pd

    if launch_angle is None or pd.isna(launch_angle):
        return None
    la = float(launch_angle)
    if la < 0:
        return "grounder"
    if la < 25:
        return "line_drive"
    if la < 50:
        return "fly_ball"
    return "popup"


def bucket_exit_velocity(ev: Optional[float]) -> Optional[str]:
    import pandas as pd

    if ev is None or pd.isna(ev):
        return None
    ev_float = float(ev)
    if ev_float >= 95:
        return "hard"
    if ev_float >= 80:
        return "medium"
    return "soft"


def categorize_pitch_type(pitch_type: Optional[str]) -> Optional[str]:
    if not pitch_type:
        return None
    fastballs = {"FF", "FT", "SI", "FC", "FA", "FS"}
    breaking = {"SL", "CU", "KC", "SC", "SV", "KN"}
    offspeed = {"CH", "FO", "EP"}
    if pitch_type in fastballs:
        return "fastball"
    if pitch_type in breaking:
        return "breaking"
    if pitch_type in offspeed:
        return "offspeed"
    return "other"


def preprocess_pitch(record: MutableMapping) -> MutableMapping:
    """Compute derived fields for a pitch record (mutates input mapping)."""
    v_bucket, h_bucket, region = classify_pitch_region(record.get("plate_x"), record.get("plate_z"))
    record["loc_high_mid_low"] = v_bucket
    record["loc_in_mid_out"] = h_bucket
    record["loc_region"] = region
    record["pitch_region"] = region
    record["count_str"] = record.get("count_str") or derive_count_str(
        record.get("count_balls_before"), record.get("count_strikes_before")
    )
    record["hit_direction"] = record.get("hit_direction") or classify_batted_ball_direction(
        record.get("stand"), record.get("hit_location"), record.get("spray_angle")
    )
    record["swing_or_take"] = classify_swing_or_take(record.get("result_pitch") or record.get("description"))
    record["launch_angle_region"] = bucket_launch_angle(record.get("launch_angle"))
    record["exit_velocity_bucket"] = bucket_exit_velocity(record.get("launch_speed"))
    record["pitch_type_category"] = categorize_pitch_type(record.get("pitch_type"))
    record["launch_speed"] = sanitize_value(record.get("launch_speed"))
    record["launch_angle"] = sanitize_value(record.get("launch_angle"))
    record["xwoba"] = sanitize_value(record.get("xwoba"))
    record["plate_x"] = sanitize_value(record.get("plate_x"))
    record["plate_z"] = sanitize_value(record.get("plate_z"))
    return record
