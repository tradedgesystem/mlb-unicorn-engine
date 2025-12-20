"""Generate and validate the static JSON "data product" for the Next.js site.

Safety rule: generate -> validate -> only then write/commit.

This tool fetches data from the backend API (local or production), materializes the
required JSON artifacts, validates them, then publishes them into:

- unicorn-website/public/data/latest/...
- unicorn-website/public/data/snapshots/YYYY-MM-DD/...
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

import requests
from sqlalchemy import case, func, select
from pybaseball import batting_stats_bref, pitching_stats_bref

from backend.app.tools.wrc_plus import build_plate_appearances, load_constants, load_statcast_data

from backend.app.db import models
from backend.app.db.session import SessionLocal


DEFAULT_BASE_URL = os.getenv("UNICORN_API_BASE_URL", "http://localhost:8000")
DEFAULT_DATA_ROOT = Path("unicorn-website/public/data")

DEFAULT_HTTP_TIMEOUT = float(os.getenv("DATA_PRODUCT_HTTP_TIMEOUT", "60"))
DEFAULT_HTTP_RETRIES = int(os.getenv("DATA_PRODUCT_HTTP_RETRIES", "3"))
DEFAULT_HTTP_BACKOFF = float(os.getenv("DATA_PRODUCT_HTTP_BACKOFF", "0.8"))
DEFAULT_WORKERS = int(os.getenv("DATA_PRODUCT_WORKERS", "12"))
DEFAULT_KEEP_DAYS = int(os.getenv("DATA_PRODUCT_KEEP_DAYS", "7"))

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_BATTING_STAT_SPECS: list[tuple[str, str, str]] = [
    ("avg", "AVG", "dec3"),
    ("slg", "SLG", "dec3"),
    ("ops", "OPS", "dec3"),
    ("obp", "OBP", "dec3"),
    ("iso", "ISO", "dec3"),
    ("woba", "wOBA", "dec3"),
    ("babip", "BABIP", "dec3"),
    ("h", "H", "int"),
    ("doubles", "2B", "int"),
    ("triples", "3B", "int"),
    ("hr", "HR", "int"),
    ("k", "SO", "int"),
    ("bb", "BB", "int"),
]

_PITCHING_STAT_SPECS: list[tuple[str, str, str]] = [
    ("era", "ERA", "dec2"),
    ("fip", "FIP", "dec2"),
    ("ip", "IP", "dec1"),
    ("h", "H", "int"),
    ("bb", "BB", "int"),
    ("hr", "HR", "int"),
    ("whip", "WHIP", "dec2"),
    ("babip", "BABIP", "dec3"),
]

_STATCAST_HIT_EVENTS = {"single", "double", "triple", "home_run"}
_STATCAST_DOUBLE_EVENTS = {"double"}
_STATCAST_TRIPLE_EVENTS = {"triple"}
_STATCAST_HR_EVENTS = {"home_run"}
_STATCAST_BB_EVENTS = {"walk", "intent_walk"}
_STATCAST_HBP_EVENTS = {"hit_by_pitch"}
_STATCAST_SF_EVENTS = {"sac_fly", "sac_fly_double_play"}
_STATCAST_SH_EVENTS = {"sac_bunt", "sac_bunt_double_play"}
_STATCAST_SO_EVENTS = {"strikeout", "strikeout_double_play"}
_STATCAST_CI_EVENTS = {"catcher_interf"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_number(value: Any, kind: str) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except TypeError:
        pass
    try:
        num = float(value)
    except Exception:
        return None
    if kind == "int":
        return int(round(num))
    return float(num)


def _normalize_ip(value: Any) -> float | None:
    raw = _coerce_number(value, "dec1")
    if raw is None:
        return None
    whole = int(raw)
    frac = round(raw - whole, 1)
    if frac == 0.1:
        return whole + (1 / 3)
    if frac == 0.2:
        return whole + (2 / 3)
    return float(raw)


def _extract_stat_row(row: Mapping[str, Any], specs: Sequence[tuple[str, str, str]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, source, kind in specs:
        payload[key] = _coerce_number(row.get(source), kind)
    return payload


def _fetch_basic_batting_stats(
    season: int,
    league_rates: dict[str, float] | None,
    date_range: tuple[str, str] | None = None,
) -> dict[int, dict[str, Any]]:
    if date_range:
        return _fetch_statcast_batting_stats(season, date_range[0], date_range[1])
    return _fetch_bref_batting_stats(season, league_rates)


def _fetch_basic_pitching_stats(season: int) -> dict[int, dict[str, Any]]:
    return _fetch_bref_pitching_stats(season)


def _statcast_date_range_from_db() -> tuple[str, str] | None:
    try:
        with SessionLocal() as session:
            min_date = session.execute(select(func.min(models.Game.game_date))).scalar_one_or_none()
            max_date = session.execute(select(func.max(models.Game.game_date))).scalar_one_or_none()
            if not min_date or not max_date:
                return None
            return (min_date.isoformat(), max_date.isoformat())
    except Exception:
        return None


def _statcast_date_range_from_constants(season: int) -> tuple[str, str] | None:
    output_dir = Path("data/outputs")
    candidates = sorted(
        output_dir.glob(f"wrc_plus_constants_{season}_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None
    payload = load_constants(candidates[0])
    cfg = payload.get("config") or {}
    start_date = cfg.get("start_date")
    end_date = cfg.get("end_date")
    if start_date and end_date:
        return (start_date, end_date)
    return None


def _statcast_date_range(season: int) -> tuple[str, str] | None:
    date_range = _statcast_date_range_from_db()
    if date_range:
        return date_range
    return _statcast_date_range_from_constants(season)



def _fetch_statcast_batting_stats(
    season: int,
    start_date: str,
    end_date: str,
) -> dict[int, dict[str, Any]]:
    cache_dir = Path("data/cache/wrc_plus")
    raw = load_statcast_data(start_date, end_date, cache_dir, allow_fetch=True)
    pa_df = build_plate_appearances(raw, game_type="R")

    events = pa_df["events"].fillna("")
    pa_df = pa_df.assign(
        is_single=events.eq("single"),
        is_double=events.isin(_STATCAST_DOUBLE_EVENTS),
        is_triple=events.isin(_STATCAST_TRIPLE_EVENTS),
        is_hr=events.isin(_STATCAST_HR_EVENTS),
        is_bb=events.isin(_STATCAST_BB_EVENTS),
        is_hbp=events.isin(_STATCAST_HBP_EVENTS),
        is_sf=events.isin(_STATCAST_SF_EVENTS),
        is_sh=events.isin(_STATCAST_SH_EVENTS),
        is_so=events.isin(_STATCAST_SO_EVENTS),
        is_ci=events.isin(_STATCAST_CI_EVENTS),
    )
    grouped = pa_df.groupby("batter", sort=False)
    counts = grouped.agg(
        PA=("events", "size"),
        singles=("is_single", "sum"),
        doubles=("is_double", "sum"),
        triples=("is_triple", "sum"),
        hr=("is_hr", "sum"),
        bb=("is_bb", "sum"),
        hbp=("is_hbp", "sum"),
        sf=("is_sf", "sum"),
        sh=("is_sh", "sum"),
        so=("is_so", "sum"),
        ci=("is_ci", "sum"),
    ).reset_index()
    counts["h"] = counts["singles"] + counts["doubles"] + counts["triples"] + counts["hr"]
    counts["ab"] = (
        counts["PA"]
        - counts["bb"]
        - counts["hbp"]
        - counts["sf"]
        - counts["sh"]
        - counts["ci"]
    )
    counts["tb"] = (
        counts["singles"]
        + 2 * counts["doubles"]
        + 3 * counts["triples"]
        + 4 * counts["hr"]
    )
    counts["avg"] = counts["h"] / counts["ab"].where(counts["ab"] > 0)
    counts["obp"] = (
        (counts["h"] + counts["bb"] + counts["hbp"])
        / (
            counts["ab"]
            + counts["bb"]
            + counts["hbp"]
            + counts["sf"]
        ).where(
            (counts["ab"] + counts["bb"] + counts["hbp"] + counts["sf"]) > 0
        )
    )
    counts["slg"] = counts["tb"] / counts["ab"].where(counts["ab"] > 0)
    counts["ops"] = counts["obp"] + counts["slg"]
    counts["iso"] = counts["slg"] - counts["avg"]
    babip_denom = (
        counts["ab"] - counts["so"] - counts["hr"] + counts["sf"]
    ).where((counts["ab"] - counts["so"] - counts["hr"] + counts["sf"]) > 0)
    counts["babip"] = (counts["h"] - counts["hr"]) / babip_denom

    woba_num = (
        0.69 * counts["bb"]
        + 0.72 * counts["hbp"]
        + 0.89 * counts["singles"]
        + 1.27 * counts["doubles"]
        + 1.62 * counts["triples"]
        + 2.1 * counts["hr"]
    )
    woba_denom = counts["ab"] + counts["bb"] + counts["hbp"] + counts["sf"]
    counts["woba"] = woba_num / woba_denom.where(woba_denom > 0)

    league_ab = counts["ab"].sum()
    league_h = counts["h"].sum()
    league_bb = counts["bb"].sum()
    league_hbp = counts["hbp"].sum()
    league_sf = counts["sf"].sum()
    league_tb = counts["tb"].sum()
    league_woba = woba_num.sum() / woba_denom.sum() if woba_denom.sum() > 0 else None
    lg_obp = (
        (league_h + league_bb + league_hbp)
        / (league_ab + league_bb + league_hbp + league_sf)
        if (league_ab + league_bb + league_hbp + league_sf) > 0
        else None
    )
    lg_slg = league_tb / league_ab if league_ab > 0 else None

    # OPS+ and wRC+ removed from output.

    stats: dict[int, dict[str, Any]] = {}
    for _, row in counts.iterrows():
        pid = int(row["batter"])
        stats[pid] = {
            "avg": _coerce_number(row["avg"], "dec3"),
            "slg": _coerce_number(row["slg"], "dec3"),
            "ops": _coerce_number(row["ops"], "dec3"),
            "obp": _coerce_number(row["obp"], "dec3"),
            "iso": _coerce_number(row["iso"], "dec3"),
            "woba": _coerce_number(row["woba"], "dec3"),
            "babip": _coerce_number(row["babip"], "dec3"),
            "h": _coerce_number(row["h"], "int"),
            "doubles": _coerce_number(row["doubles"], "int"),
            "triples": _coerce_number(row["triples"], "int"),
            "hr": _coerce_number(row["hr"], "int"),
            "k": _coerce_number(row["so"], "int"),
            "bb": _coerce_number(row["bb"], "int"),
        }
    return stats


def _league_rates_from_db() -> dict[str, float] | None:
    try:
        with SessionLocal() as session:
            min_date = session.execute(select(func.min(models.Game.game_date))).scalar_one_or_none()
            max_date = session.execute(select(func.max(models.Game.game_date))).scalar_one_or_none()
            if not min_date or not max_date:
                return None
            rows = session.execute(
                select(
                    func.count().label("pa"),
                    func.sum(case((models.PlateAppearance.result == "single", 1), else_=0)).label("singles"),
                    func.sum(case((models.PlateAppearance.result == "double", 1), else_=0)).label("doubles"),
                    func.sum(case((models.PlateAppearance.result == "triple", 1), else_=0)).label("triples"),
                    func.sum(case((models.PlateAppearance.result == "home_run", 1), else_=0)).label("hr"),
                    func.sum(case((models.PlateAppearance.result == "walk", 1), else_=0)).label("bb"),
                    func.sum(case((models.PlateAppearance.result == "intent_walk", 1), else_=0)).label("ibb"),
                    func.sum(case((models.PlateAppearance.result == "hit_by_pitch", 1), else_=0)).label("hbp"),
                    func.sum(case((models.PlateAppearance.result == "sac_fly", 1), else_=0)).label("sf"),
                    func.sum(case((models.PlateAppearance.result == "sac_bunt", 1), else_=0)).label("sac_bunt"),
                    func.sum(case((models.PlateAppearance.result == "strikeout", 1), else_=0)).label("so"),
                )
                .select_from(models.PlateAppearance)
                .join(models.Game, models.Game.game_id == models.PlateAppearance.game_id)
                .where(models.Game.game_date >= min_date, models.Game.game_date <= max_date)
            ).first()
            if not rows:
                return None
            pa = int(rows.pa or 0)
            bb = int(rows.bb or 0) + int(rows.ibb or 0)
            hbp = int(rows.hbp or 0)
            sf = int(rows.sf or 0)
            sac_bunt = int(rows.sac_bunt or 0)
            ab = pa - bb - hbp - sf - sac_bunt
            if ab <= 0:
                return None
            singles = int(rows.singles or 0)
            doubles = int(rows.doubles or 0)
            triples = int(rows.triples or 0)
            hr = int(rows.hr or 0)
            h = singles + doubles + triples + hr
            obp_denom = ab + bb + hbp + sf
            obp = (h + bb + hbp) / obp_denom if obp_denom > 0 else None
            slg = (singles + 2 * doubles + 3 * triples + 4 * hr) / ab if ab > 0 else None
            woba = None
            woba_denom = ab + bb + hbp + sf
            if woba_denom > 0:
                woba_num = (
                    0.69 * bb
                    + 0.72 * hbp
                    + 0.89 * singles
                    + 1.27 * doubles
                    + 1.62 * triples
                    + 2.1 * hr
                )
                woba = woba_num / woba_denom
            rates = {}
            if obp is not None:
                rates["obp"] = obp
            if slg is not None:
                rates["slg"] = slg
            if woba is not None:
                rates["woba"] = woba
            return rates or None
    except Exception as exc:  # noqa: BLE001
        print(f"league rates from db failed: {exc}")
        return None


def _fetch_bref_batting_stats(season: int, league_rates: dict[str, float] | None) -> dict[int, dict[str, Any]]:
    try:
        df = batting_stats_bref(season)
    except Exception as exc:  # noqa: BLE001
        print(f"baseball-reference batting fetch failed for {season}: {exc}")
        return {}
    total_ab = 0
    total_h = 0
    total_2b = 0
    total_3b = 0
    total_hr = 0
    total_bb = 0
    total_hbp = 0
    total_sf = 0
    total_obp_num = 0.0
    total_slg_num = 0.0
    total_pa_for_rate = 0.0
    for _, row in df.iterrows():
        ab = _coerce_number(row.get("AB"), "int") or 0
        h = _coerce_number(row.get("H"), "int") or 0
        doubles = _coerce_number(row.get("2B"), "int") or 0
        triples = _coerce_number(row.get("3B"), "int") or 0
        hr = _coerce_number(row.get("HR"), "int") or 0
        bb = _coerce_number(row.get("BB"), "int") or 0
        hbp = _coerce_number(row.get("HBP"), "int") or 0
        sf = _coerce_number(row.get("SF"), "int") or 0
        obp = _coerce_number(row.get("OBP"), "dec3")
        slg = _coerce_number(row.get("SLG"), "dec3")
        total_ab += int(ab)
        total_h += int(h)
        total_2b += int(doubles)
        total_3b += int(triples)
        total_hr += int(hr)
        total_bb += int(bb)
        total_hbp += int(hbp)
        total_sf += int(sf)
        if obp is not None:
            total_obp_num += float(obp) * float(ab + bb + hbp + sf)
        if slg is not None:
            total_slg_num += float(slg) * float(ab)
        total_pa_for_rate += float(ab + bb + hbp + sf)

    lg_obp = None
    lg_slg = None
    lg_woba = None
    if league_rates:
        lg_obp = league_rates.get("obp")
        lg_slg = league_rates.get("slg")
        lg_woba = league_rates.get("woba")
    if lg_obp is None or lg_slg is None or lg_woba is None:
        lg_obp = (total_obp_num / total_pa_for_rate) if total_pa_for_rate > 0 else None
        lg_slg = (total_slg_num / total_ab) if total_ab > 0 else None
        woba_denom_lg = total_ab + total_bb + total_hbp + total_sf
        if woba_denom_lg > 0:
            singles = total_h - total_2b - total_3b - total_hr
            woba_num_lg = (
                0.69 * total_bb
                + 0.72 * total_hbp
                + 0.89 * singles
                + 1.27 * total_2b
                + 1.62 * total_3b
                + 2.1 * total_hr
            )
            lg_woba = woba_num_lg / woba_denom_lg

    stats: dict[int, dict[str, Any]] = {}
    for _, row in df.iterrows():
        player_id = row.get("mlbID")
        if player_id is None:
            continue
        try:
            pid = int(player_id)
        except Exception:
            continue
        avg = _coerce_number(row.get("BA"), "dec3")
        slg = _coerce_number(row.get("SLG"), "dec3")
        obp = _coerce_number(row.get("OBP"), "dec3")
        ops = _coerce_number(row.get("OPS"), "dec3")
        h = _coerce_number(row.get("H"), "int")
        ab = _coerce_number(row.get("AB"), "int")
        hr = _coerce_number(row.get("HR"), "int")
        so = _coerce_number(row.get("SO"), "int")
        sf = _coerce_number(row.get("SF"), "int")
        iso = None if avg is None or slg is None else float(slg) - float(avg)
        denom = None
        if ab is not None and so is not None and hr is not None and sf is not None:
            denom_val = int(ab) - int(so) - int(hr) + int(sf)
            if denom_val > 0:
                denom = denom_val
        babip = None
        if denom:
            babip = (int(h or 0) - int(hr or 0)) / float(denom)
        doubles = _coerce_number(row.get("2B"), "int")
        triples = _coerce_number(row.get("3B"), "int")
        bb = _coerce_number(row.get("BB"), "int")
        hbp = _coerce_number(row.get("HBP"), "int")
        singles = None
        if h is not None and doubles is not None and triples is not None and hr is not None:
            singles = int(h) - int(doubles) - int(triples) - int(hr)
        woba = None
        woba_denom = None
        if ab is not None and bb is not None and hbp is not None and sf is not None:
            woba_denom = int(ab) + int(bb) + int(hbp) + int(sf)
        if woba_denom and singles is not None:
            woba_num = (
                0.69 * int(bb)
                + 0.72 * int(hbp)
                + 0.89 * int(singles)
                + 1.27 * int(doubles or 0)
                + 1.62 * int(triples or 0)
                + 2.1 * int(hr or 0)
            )
            woba = woba_num / float(woba_denom)
        stats[pid] = {
            "avg": avg,
            "slg": slg,
            "ops": ops,
            "obp": obp,
            "iso": iso,
            "woba": woba,
            "babip": babip,
            "h": h,
            "doubles": doubles,
            "triples": triples,
            "hr": hr,
            "k": _coerce_number(row.get("SO"), "int"),
            "bb": bb,
        }
    return stats


def _fetch_bref_pitching_stats(season: int) -> dict[int, dict[str, Any]]:
    try:
        df = pitching_stats_bref(season)
    except Exception as exc:  # noqa: BLE001
        print(f"baseball-reference pitching fetch failed for {season}: {exc}")
        return {}
    stats: dict[int, dict[str, Any]] = {}
    total_ip = 0.0
    total_er = 0.0
    total_hr = 0.0
    total_bb = 0.0
    total_hbp = 0.0
    total_so = 0.0
    for _, row in df.iterrows():
        ip_val = _normalize_ip(row.get("IP"))
        if ip_val:
            total_ip += ip_val
            total_er += float(_coerce_number(row.get("ER"), "int") or 0)
            total_hr += float(_coerce_number(row.get("HR"), "int") or 0)
            total_bb += float(_coerce_number(row.get("BB"), "int") or 0)
            total_hbp += float(_coerce_number(row.get("HBP"), "int") or 0)
            total_so += float(_coerce_number(row.get("SO"), "int") or 0)

    fip_constant = None
    if total_ip > 0:
        lg_era = (9.0 * total_er) / total_ip if total_er > 0 else 0.0
        fip_constant = lg_era - (13 * total_hr + 3 * (total_bb + total_hbp) - 2 * total_so) / total_ip

    for _, row in df.iterrows():
        player_id = row.get("mlbID")
        if player_id is None:
            continue
        try:
            pid = int(player_id)
        except Exception:
            continue
        ip_val = _normalize_ip(row.get("IP"))
        so = _coerce_number(row.get("SO"), "int")
        bb = _coerce_number(row.get("BB"), "int")
        hbp = _coerce_number(row.get("HBP"), "int")
        hr = _coerce_number(row.get("HR"), "int")
        fip = None
        if ip_val and fip_constant is not None:
            fip = ((13 * float(hr or 0) + 3 * float((bb or 0) + (hbp or 0)) - 2 * float(so or 0)) / ip_val) + fip_constant
        ab = _coerce_number(row.get("AB"), "int")
        h = _coerce_number(row.get("H"), "int")
        sf = _coerce_number(row.get("SF"), "int")
        babip = None
        if ab is not None and so is not None and hr is not None and sf is not None:
            denom_val = int(ab) - int(so) - int(hr) + int(sf)
            if denom_val > 0:
                babip = (int(h or 0) - int(hr or 0)) / float(denom_val)
        stats[pid] = {
            "era": _coerce_number(row.get("ERA"), "dec2"),
            "fip": fip,
            "ip": _coerce_number(row.get("IP"), "dec1"),
            "h": h,
            "bb": bb,
            "hr": hr,
            "whip": _coerce_number(row.get("WHIP"), "dec2"),
            "babip": babip,
        }
    return stats


def _parse_iso_datetime(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Invalid ISO datetime (empty)")
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    return datetime.fromisoformat(cleaned)


def _ensure_date_str(value: str) -> str:
    if not isinstance(value, str) or not _DATE_RE.match(value):
        raise ValueError(f"Invalid date (expected YYYY-MM-DD): {value!r}")
    date.fromisoformat(value)
    return value


def _should_retry(exc: Exception | None, resp_status: int | None) -> bool:
    # Include common transient/proxy errors (Render/Cloudflare).
    if resp_status in {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}:
        return True
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    return False


def _request_with_retry(
    url: str,
    *,
    timeout: float = DEFAULT_HTTP_TIMEOUT,
    retries: int = DEFAULT_HTTP_RETRIES,
    backoff: float = DEFAULT_HTTP_BACKOFF,
) -> requests.Response:
    attempt = 0
    while True:
        try:
            resp = requests.get(
                url,
                timeout=timeout,
                headers={
                    "User-Agent": "mlb-unicorn-engine-data-product/1.0",
                    "Cache-Control": "no-cache",
                },
            )
            if resp.status_code >= 400:
                if resp.status_code >= 500 and _should_retry(None, resp.status_code) and attempt < retries:
                    raise requests.HTTPError(f"Retryable status {resp.status_code}", response=resp)
                resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            resp_status = getattr(getattr(exc, "response", None), "status_code", None)
            if attempt > retries or not _should_retry(exc, resp_status):
                raise
            time.sleep(backoff * (2 ** (attempt - 1)))


def _fetch_json(url: str) -> Any:
    return _request_with_retry(url).json()


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    data = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    tmp_path.write_text(data, encoding="utf-8")
    tmp_path.replace(path)


def _sorted_unique_ints(values: Iterable[int]) -> list[int]:
    return sorted({int(v) for v in values})


@dataclass(frozen=True)
class _TeamRef:
    team_id: int
    abbreviation: str


def _fetch_teams(base_url: str) -> list[_TeamRef]:
    url = f"{base_url.rstrip('/')}/api/teams"
    teams_raw = _fetch_json(url)
    teams: list[_TeamRef] = []
    for row in teams_raw:
        team_id = int(row.get("team_id"))
        abbrev = (row.get("abbrev") or "").strip()
        teams.append(_TeamRef(team_id=team_id, abbreviation=abbrev))
    teams.sort(key=lambda t: t.team_id)
    return teams


def _fetch_team_detail(base_url: str, team_id: int, *, as_of_date: str | None = None) -> Mapping[str, Any]:
    url = f"{base_url.rstrip('/')}/api/teams/{team_id}"
    if as_of_date:
        url = f"{url}?as_of_date={as_of_date}"
    return _fetch_json(url)


def _fetch_player_detail(base_url: str, player_id: int, *, as_of_date: str | None = None) -> Mapping[str, Any]:
    url = f"{base_url.rstrip('/')}/api/players/{player_id}"
    if as_of_date:
        url = f"{url}?as_of_date={as_of_date}"
    return _fetch_json(url)


def _player_roles(player_payload: Mapping[str, Any]) -> list[str]:
    roles = player_payload.get("roles")
    if isinstance(roles, list) and roles:
        return [str(r) for r in roles if str(r).strip()]
    role = (player_payload.get("role") or "").strip()
    return [role] if role else []


def _normalize_roster_player(
    player: Mapping[str, Any],
    *,
    roles_override: Sequence[str] | None = None,
) -> dict[str, Any]:
    pid = int(player.get("player_id"))
    name = (player.get("player_name") or player.get("full_name") or "").strip()
    position = (player.get("position") or "").strip()
    role = (player.get("role") or "").strip()
    roles = list(roles_override) if roles_override is not None else ([role] if role else [])
    return {
        "player_id": pid,
        "name": name,
        "position": position,
        "roles": roles,
        "href": f"/players/{pid}/",
    }


def _normalize_team_detail(team: Mapping[str, Any], player_roles_map: Mapping[int, Sequence[str]]) -> dict[str, Any]:
    team_id = int(team.get("team_id"))
    abbrev = (team.get("abbrev") or "").strip()

    groups: dict[str, dict[int, dict[str, Any]]] = {"hitters": {}, "starters": {}, "relievers": {}}

    def add_to_group(group: str, normalized: dict[str, Any]) -> None:
        pid = int(normalized["player_id"])
        groups[group][pid] = normalized

    for key in ("hitters", "starters", "relievers"):
        raw = team.get(key) or []
        for p in raw:
            pid = int(p.get("player_id"))
            roles = list(player_roles_map.get(pid) or [])
            normalized = _normalize_roster_player(p, roles_override=roles)
            add_to_group(key, normalized)

            # Two-way/multi-role players should appear in every applicable roster section.
            roles_norm = {str(r).strip().lower() for r in roles if str(r).strip()}
            if "hitter" in roles_norm:
                add_to_group("hitters", normalized)
            if "starter" in roles_norm:
                add_to_group("starters", normalized)
            if "reliever" in roles_norm:
                add_to_group("relievers", normalized)

    def finalize(group: str) -> list[dict[str, Any]]:
        items = list(groups[group].values())
        items.sort(key=lambda r: (r.get("name") or "", r.get("player_id")))
        return items

    return {
        "team_id": team_id,
        "abbreviation": abbrev,
        "hitters": finalize("hitters"),
        "starters": finalize("starters"),
        "relievers": finalize("relievers"),
    }


def generate_data_product_staged(
    *,
    base_url: str,
    snapshot_date: str,
    staged_root: Path,
    workers: int = DEFAULT_WORKERS,
) -> None:
    teams = _fetch_teams(base_url)
    if len(teams) != 30:
        raise RuntimeError(f"Expected 30 teams from {base_url}/api/teams, got {len(teams)}")

    team_details: dict[int, Mapping[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(workers, 8)) as pool:
        futures = {
            pool.submit(_fetch_team_detail, base_url, t.team_id, as_of_date=snapshot_date): t.team_id for t in teams
        }
        for fut in as_completed(futures):
            team_id = futures[fut]
            team_details[team_id] = fut.result()

    roster_player_ids: list[int] = []
    for team_id, detail in team_details.items():
        for group in ("hitters", "starters", "relievers"):
            for p in detail.get(group) or []:
                if p.get("player_id") is None:
                    continue
                roster_player_ids.append(int(p.get("player_id")))
    roster_player_ids = _sorted_unique_ints(roster_player_ids)

    # Fetch player details for all roster players.
    all_player_ids = roster_player_ids

    player_details: dict[int, Mapping[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_player_detail, base_url, pid, as_of_date=snapshot_date): pid for pid in all_player_ids
        }
        for fut in as_completed(futures):
            pid = futures[fut]
            player_details[pid] = fut.result()

    # Minimal player refs for team/index payloads.
    player_minimal: dict[int, dict[str, Any]] = {}
    for pid, payload in player_details.items():
        name = (payload.get("player_name") or "").strip()
        current_team_id = payload.get("team_id")
        roles = _player_roles(payload)
        player_minimal[pid] = {
            "player_id": pid,
            "name": name,
            "current_team_id": int(current_team_id) if current_team_id is not None else None,
            "roles": roles,
        }

    # Normalize team details using player roles (handles two-way roles).
    player_roles_map = {pid: info.get("roles") or [] for pid, info in player_minimal.items()}

    season_year = int(snapshot_date.split("-")[0])
    date_range = _statcast_date_range(season_year)
    league_rates = None if date_range else _league_rates_from_db()
    basic_batting = _fetch_basic_batting_stats(season_year, league_rates, date_range)
    basic_pitching = _fetch_basic_pitching_stats(season_year)

    # Write artifacts.
    _atomic_write_json(staged_root / "teams.json", [{"team_id": t.team_id, "abbreviation": t.abbreviation} for t in teams])

    for team_id in sorted(team_details.keys()):
        normalized = _normalize_team_detail(team_details[team_id], player_roles_map)
        _atomic_write_json(staged_root / "teams" / f"{team_id}.json", normalized)

    players_index = [
        {
            "player_id": pid,
            "name": player_minimal[pid]["name"],
            "current_team_id": player_minimal[pid]["current_team_id"],
            "roles": player_minimal[pid]["roles"],
        }
        for pid in sorted(player_minimal.keys())
    ]
    _atomic_write_json(staged_root / "players_index.json", players_index)

    # Player files.
    for pid in sorted(player_details.keys()):
        payload = dict(player_details[pid])
        # Required: current_team_id for the site "back to current team" link.
        payload["current_team_id"] = int(payload["team_id"]) if payload.get("team_id") is not None else None
        payload["name"] = (payload.get("player_name") or "").strip()
        payload["roles"] = _player_roles(payload)
        payload["basic_batting"] = basic_batting.get(pid)
        payload["basic_pitching"] = basic_pitching.get(pid)
        _atomic_write_json(staged_root / "players" / f"{pid}.json", payload)

    meta = {
        "last_updated": _utc_now_iso(),
        "snapshot_date": snapshot_date,
        "counts": {
            "teams_count": 30,
            "players_count": len(players_index),
        },
    }
    _atomic_write_json(staged_root / "meta.json", meta)


def validate_data_product_dir(root: Path) -> None:
    errors: list[str] = []

    def load_json(rel: str) -> Any:
        p = root / rel
        if not p.exists():
            errors.append(f"Missing required file: {rel}")
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Invalid JSON in {rel}: {exc}")
            return None

    meta = load_json("meta.json")
    teams = load_json("teams.json")
    players_index = load_json("players_index.json")

    if isinstance(meta, dict):
        try:
            _parse_iso_datetime(meta.get("last_updated"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"meta.json last_updated invalid: {exc}")
        try:
            snapshot_date = _ensure_date_str(meta.get("snapshot_date"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"meta.json snapshot_date invalid: {exc}")
            snapshot_date = None
        counts = meta.get("counts")
        if not isinstance(counts, dict):
            errors.append("meta.json counts missing or invalid")

    if isinstance(teams, list):
        if len(teams) != 30:
            errors.append(f"teams.json must have exactly 30 teams (got {len(teams)})")
        for i, t in enumerate(teams):
            if not isinstance(t, dict):
                errors.append(f"teams.json[{i}] must be an object")
                continue
            if t.get("team_id") is None or t.get("abbreviation") is None:
                errors.append(f"teams.json[{i}] must include team_id and abbreviation")

    team_player_ids: list[int] = []
    team_ids: list[int] = []
    if isinstance(teams, list):
        for t in teams:
            if isinstance(t, dict) and t.get("team_id") is not None:
                team_ids.append(int(t.get("team_id")))
    for tid in team_ids:
        team_payload = load_json(f"teams/{tid}.json")
        if not isinstance(team_payload, dict):
            continue
        for group in ("hitters", "starters", "relievers"):
            roster = team_payload.get(group)
            if not isinstance(roster, list):
                errors.append(f"teams/{tid}.json {group} must be a list")
                continue
            for p in roster:
                if not isinstance(p, dict) or p.get("player_id") is None:
                    errors.append(f"teams/{tid}.json {group} entries must include player_id")
                    continue
                team_player_ids.append(int(p.get("player_id")))

    referenced_player_ids = set(team_player_ids)
    for pid in sorted(referenced_player_ids):
        rel = f"players/{pid}.json"
        payload = load_json(rel)
        if not isinstance(payload, dict):
            continue
        if payload.get("current_team_id") is None:
            errors.append(f"{rel} missing current_team_id")

    # meta.json counts consistency
    if isinstance(meta, dict) and isinstance(meta.get("counts"), dict):
        counts = meta["counts"]
        if isinstance(teams, list) and int(counts.get("teams_count", -1)) != len(teams):
            errors.append("meta.json counts.teams_count inconsistent with teams.json")
        if isinstance(players_index, list) and int(counts.get("players_count", -1)) != len(players_index):
            errors.append("meta.json counts.players_count inconsistent with players_index.json")
    if errors:
        raise ValueError("Data product validation failed:\n- " + "\n- ".join(errors))


def _replace_dir_atomic(src_dir: Path, dst_dir: Path) -> None:
    """Atomically replace dst_dir with src_dir (best-effort safety)."""
    dst_dir.parent.mkdir(parents=True, exist_ok=True)
    backup = None
    if dst_dir.exists():
        backup = dst_dir.with_name(dst_dir.name + f".__backup__{int(time.time())}")
        dst_dir.replace(backup)
    try:
        src_dir.replace(dst_dir)
    except Exception:  # noqa: BLE001
        if backup and backup.exists() and not dst_dir.exists():
            backup.replace(dst_dir)
        raise
    finally:
        if backup and backup.exists():
            shutil.rmtree(backup, ignore_errors=True)


def prune_old_snapshots(*, snapshots_dir: Path, keep_days: int = DEFAULT_KEEP_DAYS) -> None:
    if keep_days <= 0:
        return
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=keep_days - 1)
    for child in snapshots_dir.iterdir():
        if not child.is_dir():
            continue
        name = child.name
        if not _DATE_RE.match(name):
            continue
        try:
            d = date.fromisoformat(name)
        except ValueError:
            continue
        if d < cutoff:
            shutil.rmtree(child, ignore_errors=True)


def publish_data_product(
    *,
    staged_root: Path,
    data_root: Path,
    snapshot_date: str,
    keep_days: int,
) -> None:
    data_root.mkdir(parents=True, exist_ok=True)
    # Copy staged output into repo (tmp dirs), then atomic renames into place.
    snapshots_dir = data_root / "snapshots"
    latest_dir = data_root / "latest"
    snapshot_dst = snapshots_dir / snapshot_date

    with tempfile.TemporaryDirectory(prefix=".publish-", dir=str(data_root)) as tmp:
        tmp_root = Path(tmp)
        snapshot_tmp = tmp_root / "snapshot"
        latest_tmp = tmp_root / "latest"
        shutil.copytree(staged_root, snapshot_tmp, dirs_exist_ok=True)
        shutil.copytree(staged_root, latest_tmp, dirs_exist_ok=True)

        # Sanity: the directories we are about to publish still validate.
        validate_data_product_dir(snapshot_tmp)
        validate_data_product_dir(latest_tmp)

        _replace_dir_atomic(snapshot_tmp, snapshot_dst)
        _replace_dir_atomic(latest_tmp, latest_dir)

    prune_old_snapshots(snapshots_dir=snapshots_dir, keep_days=keep_days)


def _default_snapshot_date() -> str:
    # Prefer "yesterday" UTC to avoid partial current-day ingestion.
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate and publish the static site JSON data product.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Backend API base URL.")
    parser.add_argument("--snapshot-date", default=_default_snapshot_date(), help="Snapshot date (YYYY-MM-DD).")
    parser.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT), help="Repo path to public data root.")
    parser.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS, help="Keep last N snapshot days.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max concurrent player fetches.")
    args = parser.parse_args(argv)

    snapshot_date = _ensure_date_str(args.snapshot_date)
    data_root = Path(args.data_root)
    base_url = args.base_url.strip()
    keep_days = int(args.keep_days)

    with tempfile.TemporaryDirectory(prefix="data-product-") as tmp:
        staged_root = Path(tmp) / "staged"
        generate_data_product_staged(
            base_url=base_url,
            snapshot_date=snapshot_date,
            staged_root=staged_root,
            workers=int(args.workers),
        )
        validate_data_product_dir(staged_root)
        publish_data_product(staged_root=staged_root, data_root=data_root, snapshot_date=snapshot_date, keep_days=keep_days)

    print(f"Data product published: snapshots/{snapshot_date} and latest/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
