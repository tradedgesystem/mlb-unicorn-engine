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


DEFAULT_BASE_URL = os.getenv("UNICORN_API_BASE_URL", "http://localhost:8000")
DEFAULT_DATA_ROOT = Path("unicorn-website/public/data")

DEFAULT_HTTP_TIMEOUT = float(os.getenv("DATA_PRODUCT_HTTP_TIMEOUT", "60"))
DEFAULT_HTTP_RETRIES = int(os.getenv("DATA_PRODUCT_HTTP_RETRIES", "3"))
DEFAULT_HTTP_BACKOFF = float(os.getenv("DATA_PRODUCT_HTTP_BACKOFF", "0.8"))
DEFAULT_WORKERS = int(os.getenv("DATA_PRODUCT_WORKERS", "12"))
DEFAULT_KEEP_DAYS = int(os.getenv("DATA_PRODUCT_KEEP_DAYS", "7"))

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
