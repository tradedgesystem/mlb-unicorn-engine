"""Utilities for resolving MLBAM people metadata (names, positions) with caching.

Uses the MLB Stats public people endpoint:
https://statsapi.mlb.com/api/v1/people?personIds=...
"""

from __future__ import annotations

from typing import Dict, Iterable, Optional, Sequence, Tuple

import requests
from sqlalchemy import select

from backend.app.core.logging import logger
from backend.app.db import models

# Cache entries: player_id -> (full_name, primary_position_abbrev)
_PEOPLE_CACHE: Dict[int, Tuple[Optional[str], Optional[str]]] = {}


def _chunked(values: Sequence[int], size: int = 200) -> Iterable[Sequence[int]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def preload_people(player_ids: Sequence[int]) -> None:
    """Warm cache for the given MLBAM ids."""
    missing = [pid for pid in player_ids if pid not in _PEOPLE_CACHE]
    if not missing:
        return
    url = "https://statsapi.mlb.com/api/v1/people"
    for chunk in _chunked(missing, size=200):
        try:
            resp = requests.get(
                url,
                params={"personIds": ",".join(str(pid) for pid in chunk)},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            people = data.get("people", []) if isinstance(data, dict) else []
            returned = set()
            for person in people:
                if not isinstance(person, dict):
                    continue
                pid = int(person.get("id"))
                returned.add(pid)
                full_name = person.get("fullName")
                pos_abbrev = (person.get("primaryPosition") or {}).get("abbreviation")
                _PEOPLE_CACHE[pid] = (full_name, pos_abbrev)
            for pid in chunk:
                if pid not in returned:
                    _PEOPLE_CACHE[pid] = (None, None)
        except Exception as exc:  # noqa: BLE001
            logger.warning("MLBAM people lookup failed for %s ids: %s", len(chunk), exc)
            for pid in chunk:
                _PEOPLE_CACHE[pid] = (None, None)


def get_full_name(player_id: int) -> Optional[str]:
    if player_id not in _PEOPLE_CACHE:
        preload_people([player_id])
    return (_PEOPLE_CACHE.get(player_id) or (None, None))[0]


def get_primary_position_abbrev(player_id: int) -> Optional[str]:
    if player_id not in _PEOPLE_CACHE:
        preload_people([player_id])
    return (_PEOPLE_CACHE.get(player_id) or (None, None))[1]


def is_placeholder_name(name: Optional[str], player_id: Optional[int] = None) -> bool:
    if name is None:
        return True
    stripped = str(name).strip()
    if stripped == "":
        return True
    if stripped.isdigit():
        return True
    if player_id is not None and stripped == str(player_id):
        return True
    return False


def refresh_player_names(session) -> int:
    """Update players.full_name and players.primary_pos from MLBAM people data.

    Returns number of rows updated (name or position).
    """
    players = session.execute(select(models.Player)).scalars().all()
    preload_people([p.player_id for p in players])
    updated = 0
    for p in players:
        full_name, pos = _PEOPLE_CACHE.get(p.player_id, (None, None))
        if is_placeholder_name(p.full_name, p.player_id) and full_name and not is_placeholder_name(full_name, p.player_id):
            p.full_name = full_name
            updated += 1
        if pos and pos != p.primary_pos:
            p.primary_pos = pos
            updated += 1
    return updated


def main() -> None:
    """CLI: refresh placeholder names in the DB."""
    from backend.app.db.session import SessionLocal

    with SessionLocal() as session:
        updated = refresh_player_names(session)
        session.commit()
    logger.info("Refreshed %s player names", updated)


if __name__ == "__main__":
    main()
