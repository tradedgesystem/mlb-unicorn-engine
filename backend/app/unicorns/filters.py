"""Builder for SQL WHERE clauses from filters_json."""
from __future__ import annotations

from typing import Dict, Tuple

SUPPORTED_OPS = {"=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "IS NULL", "IS NOT NULL"}


def _next_param(counter: int) -> str:
    return f"p_{counter}"


def build_filter_clause(filters_json: dict | None) -> Tuple[str, Dict[str, object]]:
    if not filters_json:
        return "", {}

    params: Dict[str, object] = {}
    clauses = []
    counter = 0
    conditions = filters_json.get("conditions", []) if isinstance(filters_json, dict) else []

    for condition in conditions:
        field = condition.get("field")
        op = str(condition.get("op", "=")).upper()
        value = condition.get("value")
        if not field or op not in SUPPORTED_OPS:
            continue

        if op in {"IS NULL", "IS NOT NULL"}:
            clauses.append(f"{field} {op}")
            continue

        # Translate NULL comparisons into SQL IS (NOT) NULL for correctness.
        if value is None:
            if op == "=":
                clauses.append(f"{field} IS NULL")
                continue
            if op == "!=":
                clauses.append(f"{field} IS NOT NULL")
                continue

        param_name = _next_param(counter)
        counter += 1

        if op in {"IN", "NOT IN"}:
            params[param_name] = tuple(value if isinstance(value, (list, tuple)) else [value])
            clauses.append(f"{field} {op} :{param_name}")
        else:
            params[param_name] = value
            clauses.append(f"{field} {op} :{param_name}")

    if not clauses:
        return "", {}

    return " AND " + " AND ".join(clauses), params
