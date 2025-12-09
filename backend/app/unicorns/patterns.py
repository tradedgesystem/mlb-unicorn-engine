"""Pattern validation helpers aligned to global constraints."""
from __future__ import annotations

from typing import Iterable

from backend.db import models

ALLOWED_COUNTS = {"3-0", "0-2", "3-2"}
BANNED_TERMS = {
    "inning",
    "weather",
    "wind",
    "humidity",
    "temperature",
    "sequence",
    "after fouling",
    "after two",
    "late innings",
}


def _contains_banned_term(filters_json: object) -> bool:
    if filters_json is None:
        return False
    if isinstance(filters_json, dict):
        for key, value in filters_json.items():
            if _contains_banned_term(key) or _contains_banned_term(value):
                return True
    if isinstance(filters_json, (list, tuple, set)):
        return any(_contains_banned_term(v) for v in filters_json)
    if isinstance(filters_json, str):
        lower_val = filters_json.lower()
        return any(term in lower_val for term in BANNED_TERMS)
    return False


def _invalid_count(filters_json: object) -> bool:
    if not filters_json:
        return False
    if isinstance(filters_json, dict):
        for key, value in filters_json.items():
            if key == "count_str" and isinstance(value, str) and value not in ALLOWED_COUNTS:
                return True
            if _invalid_count(value):
                return True
    if isinstance(filters_json, (list, tuple, set)):
        return any(_invalid_count(v) for v in filters_json)
    return False


def validate_pattern(pattern: models.PatternTemplate) -> None:
    errors: list[str] = []

    if pattern.complexity_score and pattern.complexity_score > 4:
        errors.append("complexity_score exceeds 4")

    if pattern.requires_count:
        if pattern.count_value not in ALLOWED_COUNTS:
            errors.append("requires_count set but count_value is invalid")

    if pattern.count_value and pattern.count_value not in ALLOWED_COUNTS:
        errors.append("count_value not in allowed set")

    if _contains_banned_term(pattern.filters_json):
        errors.append("filters_json contains banned concept")

    if _invalid_count(pattern.filters_json):
        errors.append("filters_json uses disallowed count")

    if errors:
        raise ValueError(f"Pattern {pattern.pattern_id} invalid: {', '.join(errors)}")
