"""Top 50 quality audit CLI and reusable helpers."""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import time
import sys
import requests

DEFAULT_BASE_URL = os.getenv("UNICORN_API_BASE_URL", "http://localhost:8000")
DIVERSITY_MIN_TEAMS = 10
DUPLICATE_SIM_THRESHOLD = 0.85
DUPLICATE_PAIR_LIMIT = 25
DUPLICATE_CLUSTER_LIMIT = 6
CV_MIN_THRESHOLD = 0.15
NEAR_TIE_LIMIT = 25
DOMINATE_TOP10_LIMIT = 4
DOMINATE_TOP50_LIMIT = 7
DEFAULT_HTTP_TIMEOUT = 60.0
DEFAULT_HTTP_RETRIES = 3
DEFAULT_HTTP_BACKOFF = 0.8
MIN_TOP_COUNT = 50
FAIL_ON_SHORT = os.getenv("AUDIT_FAIL_ON_INSUFFICIENT", "").lower() in {"1", "true", "yes"}


def normalize_text(text: str) -> str:
    cleaned = re.sub(r"[^\w\s]", " ", text.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def jaccard_similarity(text_a: str, text_b: str) -> float:
    tokens_a = set(normalize_text(text_a).split()) if text_a else set()
    tokens_b = set(normalize_text(text_b).split()) if text_b else set()
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def cluster_descriptions(descriptions: Sequence[str], threshold: float = DUPLICATE_SIM_THRESHOLD) -> List[List[int]]:
    n = len(descriptions)
    adjacency: List[set[int]] = [set() for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            if jaccard_similarity(descriptions[i], descriptions[j]) >= threshold:
                adjacency[i].add(j)
                adjacency[j].add(i)

    visited = [False] * n
    clusters: List[List[int]] = []
    for i in range(n):
        if visited[i]:
            continue
        stack = [i]
        component: List[int] = []
        while stack:
            node = stack.pop()
            if visited[node]:
                continue
            visited[node] = True
            component.append(node)
            stack.extend(adjacency[node] - set(component))
        if len(component) > 1:
            clusters.append(sorted(component))
    return clusters


def percentiles(values: Sequence[float], percentiles_list: Sequence[int]) -> Dict[int, float]:
    if not values:
        return {p: 0.0 for p in percentiles_list}
    sorted_vals = sorted(values)
    results: Dict[int, float] = {}
    for p in percentiles_list:
        k = (len(sorted_vals) - 1) * (p / 100)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            results[p] = float(sorted_vals[int(k)])
        else:
            d0 = sorted_vals[f] * (c - k)
            d1 = sorted_vals[c] * (k - f)
            results[p] = float(d0 + d1)
    return results


def _role_from_player(player: Mapping) -> str:
    role = (player.get("role") or "").lower()
    if role:
        return role
    primary_pos = (player.get("primary_pos") or "").upper()
    if primary_pos == "P":
        return "pitcher"
    if primary_pos == "TWP":
        return "twp"
    return "hitter" if primary_pos else "unknown"


def _is_role_mismatch(entity_type: str, role: str) -> bool:
    et = (entity_type or "").lower()
    r = (role or "").lower()
    if r == "twp":
        return False
    if et in {"batter", "hitter"} and r in {"starter", "reliever", "pitcher"}:
        return True
    if et == "pitcher" and r in {"hitter"}:
        return True
    return False


@dataclass
class Top50Entry:
    run_date: str
    rank: int
    entity_type: str
    entity_id: int
    pattern_id: str
    score: float
    description: str
    team_id: Optional[int] = None
    role: Optional[str] = None


def _should_retry(exc: Exception, resp_status: Optional[int]) -> bool:
    if resp_status in {502, 503, 504}:
        return True
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    return False


def _request_with_retry(url: str, timeout: float, retries: int, backoff: float) -> requests.Response:
    attempt = 0
    while True:
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code >= 500 and _should_retry(None, resp.status_code) and attempt < retries:
                raise requests.HTTPError(f"Retryable status {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            if attempt > retries or not _should_retry(exc, getattr(getattr(exc, "response", None), "status_code", None)):
                raise
            sleep_for = backoff * (2 ** (attempt - 1))
            time.sleep(sleep_for)


def _fetch_top50_http(run_date: date, base_url: str, timeout: float, retries: int, backoff: float) -> List[Top50Entry]:
    url = f"{base_url.rstrip('/')}/top50/{run_date.isoformat()}"
    resp = _request_with_retry(url, timeout=timeout, retries=retries, backoff=backoff)
    data = resp.json()
    results: List[Top50Entry] = []
    for row in data:
        results.append(
            Top50Entry(
                run_date=str(row.get("run_date")),
                rank=int(row.get("rank")),
                entity_type=row.get("entity_type"),
                entity_id=int(row.get("entity_id")),
                pattern_id=row.get("pattern_id"),
                score=float(row.get("score", 0.0)),
                description=row.get("description") or "",
            )
        )
    return results


def _fetch_player_http(player_id: int, base_url: str, cache: MutableMapping[int, Mapping], timeout: float, retries: int, backoff: float) -> Mapping:
    if player_id in cache:
        return cache[player_id]
    url = f"{base_url.rstrip('/')}/api/players/{player_id}"
    resp = _request_with_retry(url, timeout=timeout, retries=retries, backoff=backoff)
    data = resp.json()
    cache[player_id] = data
    return data


def _adjacent_near_ties(scores: List[float]) -> int:
    if len(scores) < 2:
        return 0
    sorted_scores = sorted(scores, reverse=True)
    count = 0
    for i in range(len(sorted_scores) - 1):
        high = sorted_scores[i]
        low = sorted_scores[i + 1]
        if high == 0:
            continue
        if (high - low) < 0.01 * high:
            count += 1
    return count


def audit_day(
    entries: List[Top50Entry],
    player_lookup: Callable[[int], Mapping],
    *,
    fail_on_short: bool = FAIL_ON_SHORT,
    min_count: int = MIN_TOP_COUNT,
) -> Tuple[dict, List[str]]:
    fail_reasons: List[str] = []
    warnings: List[str] = []
    n = len(entries)
    patterns = {e.pattern_id for e in entries}
    teams = set()
    roles = []
    role_counts: Counter[str] = Counter()
    role_mismatches: List[dict] = []
    descriptions = [e.description for e in entries]
    scores = [e.score for e in entries]

    for e in entries:
        player = player_lookup(e.entity_id)
        role = _role_from_player(player)
        role_counts[role] += 1
        roles.append(role)
        team_id = e.team_id or player.get("team_id") or player.get("current_team_id")
        if team_id:
            teams.add(team_id)
        if _is_role_mismatch(e.entity_type, role):
            role_mismatches.append(
                {
                    "player_id": e.entity_id,
                    "entity_type": e.entity_type,
                    "role": role,
                    "pattern_id": e.pattern_id,
                }
            )

    diversity = {
        "patterns_distinct": len(patterns),
        "teams_distinct": len(teams),
        "roles_distinct": len(set(roles)),
        "role_counts": dict(role_counts),
    }
    if len(set(roles)) < 2:
        fail_reasons.append("diversity_roles_insufficient")
    if len(teams) < DIVERSITY_MIN_TEAMS:
        fail_reasons.append("diversity_teams_low")
    if n < min_count:
        reason = "top50_insufficient_count"
        if fail_on_short:
            fail_reasons.append(reason)
        else:
            warnings.append(reason)

    clusters = cluster_descriptions(descriptions, threshold=DUPLICATE_SIM_THRESHOLD)
    largest_cluster = max((len(c) for c in clusters), default=1)
    duplicate_pairs = sum(1 for i in range(n) for j in range(i + 1, n) if jaccard_similarity(descriptions[i], descriptions[j]) >= DUPLICATE_SIM_THRESHOLD)
    duplicates = {
        "pairs_over_0_85": duplicate_pairs,
        "largest_cluster": largest_cluster,
        "example_clusters": [[descriptions[idx] for idx in cluster[:3]] for cluster in clusters[:3]],
    }
    if duplicate_pairs >= DUPLICATE_PAIR_LIMIT:
        fail_reasons.append("duplicates_pairs_high")
    if largest_cluster >= DUPLICATE_CLUSTER_LIMIT:
        fail_reasons.append("duplicates_cluster_large")

    stats = {}
    if scores:
        mean_val = statistics.mean(scores)
        stdev_val = statistics.pstdev(scores) if len(scores) > 1 else 0.0
        cv = (stdev_val / mean_val) if mean_val else 0.0
        percs = percentiles(scores, [90, 50, 10])
        near_ties = _adjacent_near_ties(scores)
        stats = {
            "mean": mean_val,
            "stdev": stdev_val,
            "cv": cv,
            "p90": percs[90],
            "p50": percs[50],
            "p10": percs[10],
            "adjacent_near_ties": near_ties,
        }
        if cv < CV_MIN_THRESHOLD:
            fail_reasons.append("scores_cv_low")
        if near_ties >= NEAR_TIE_LIMIT:
            fail_reasons.append("scores_near_ties_many")

    if role_mismatches:
        fail_reasons.append("role_mismatches_present")

    verdict = "FAIL" if fail_reasons else "PASS"
    day_report = {
        "n": n,
        "diversity": diversity,
        "duplicates": duplicates,
        "scores": stats,
        "role_mismatches": role_mismatches,
        "verdict": verdict,
        "fail_reasons": fail_reasons,
        "warnings": warnings,
    }
    return day_report, fail_reasons


def audit_range(
    start: date,
    end: date,
    load_top50: Callable[[date], List[Top50Entry]],
    load_player: Callable[[int], Mapping],
    *,
    fail_on_short: bool = FAIL_ON_SHORT,
    min_count: int = MIN_TOP_COUNT,
) -> dict:
    days: Dict[str, dict] = {}
    cross_counts_top50: Counter[int] = Counter()
    cross_counts_top10: Counter[int] = Counter()
    fail_any = False
    overall_fail_reasons: List[str] = []
    current = start
    while current <= end:
        entries = load_top50(current)
        day_report, fail_reasons = audit_day(entries, load_player, fail_on_short=fail_on_short, min_count=min_count)
        date_str = current.isoformat()
        days[date_str] = day_report
        if day_report["verdict"] == "FAIL":
            fail_any = True
        for e in entries:
            cross_counts_top50[e.entity_id] += 1
            if e.rank <= 10:
                cross_counts_top10[e.entity_id] += 1
        current += timedelta(days=1)

    dominators_top10 = [pid for pid, count in cross_counts_top10.items() if count >= DOMINATE_TOP10_LIMIT]
    dominators_top50 = [pid for pid, count in cross_counts_top50.items() if count >= DOMINATE_TOP50_LIMIT]
    if dominators_top10:
        fail_any = True
        overall_fail_reasons.append("cross_day_dominators_top10")
    if dominators_top50:
        fail_any = True
        overall_fail_reasons.append("cross_day_dominators_top50")

    cross_day = {
        "top50_counts": [[pid, cnt] for pid, cnt in cross_counts_top50.most_common()],
        "top10_counts": [[pid, cnt] for pid, cnt in cross_counts_top10.most_common()],
        "dominators": {"top50": dominators_top50, "top10": dominators_top10},
    }

    overall = "FAIL" if fail_any else "PASS"
    return {
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "overall_verdict": overall,
        "overall_fail_reasons": overall_fail_reasons if overall == "FAIL" else [],
        "days": days,
        "cross_day": cross_day,
    }


def _default_player_loader(base_url: str, timeout: float, retries: int, backoff: float) -> Callable[[int], Mapping]:
    cache: Dict[int, Mapping] = {}

    def _loader(pid: int) -> Mapping:
        return _fetch_player_http(pid, base_url, cache, timeout=timeout, retries=retries, backoff=backoff)

    return _loader


def _default_top50_loader(base_url: str, timeout: float, retries: int, backoff: float) -> Callable[[date], List[Top50Entry]]:
    def _loader(run_date: date) -> List[Top50Entry]:
        return _fetch_top50_http(run_date, base_url, timeout=timeout, retries=retries, backoff=backoff)

    return _loader


def _write_report(report: dict, start: date, end: date) -> Path:
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    fname = artifacts_dir / f"top50_quality_report_{start.isoformat()}_{end.isoformat()}.json"
    with fname.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return fname


def _print_summary(report: dict) -> None:
    overall = report["overall_verdict"]
    print(f"Overall: {overall}")
    for day, payload in sorted(report["days"].items()):
        reasons = payload.get("fail_reasons") or []
        warns = payload.get("warnings") or []
        tag = "FAIL" if payload.get("verdict") == "FAIL" else "PASS"
        parts = []
        if reasons:
            parts.append("fails=" + ",".join(reasons))
        if warns:
            parts.append("warns=" + ",".join(warns))
        reason_text = " ".join(parts) if parts else "ok"
        print(f"{day}: {tag} ({reason_text})")


def exit_code_from_report(report: Mapping) -> int:
    """Return 0 only when overall and all days are PASS; otherwise return 1."""
    overall_ok = report.get("overall_verdict") == "PASS"
    days = report.get("days") or {}
    all_days_ok = all((day.get("verdict") == "PASS") for day in days.values())
    return 0 if overall_ok and all_days_ok else 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Top 50 quality for a date range")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--base-url", dest="base_url", default=DEFAULT_BASE_URL, help="Base URL for the Unicorn API")
    parser.add_argument("--http-timeout", type=float, default=DEFAULT_HTTP_TIMEOUT, help="HTTP timeout in seconds (default: 60)")
    parser.add_argument("--http-retries", type=int, default=DEFAULT_HTTP_RETRIES, help="HTTP retries on timeout/5xx (default: 3)")
    parser.add_argument("--http-backoff", type=float, default=DEFAULT_HTTP_BACKOFF, help="Initial backoff seconds for retries (default: 0.8)")
    parser.add_argument(
        "--fail-on-short",
        action="store_true",
        help="Fail when fewer than MIN_TOP_COUNT rows are present (default: warn only)",
    )
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    base_url = args.base_url

    report = audit_range(
        start,
        end,
        load_top50=_default_top50_loader(base_url, timeout=args.http_timeout, retries=args.http_retries, backoff=args.http_backoff),
        load_player=_default_player_loader(base_url, timeout=args.http_timeout, retries=args.http_retries, backoff=args.http_backoff),
        fail_on_short=args.fail_on_short or FAIL_ON_SHORT,
        min_count=MIN_TOP_COUNT,
    )
    report["base_url"] = base_url
    path = _write_report(report, start, end)
    _print_summary(report)
    print(f"HTTP timeout={args.http_timeout}s retries={args.http_retries} backoff={args.http_backoff}s")
    print(f"Report written to {path}")
    sys.exit(exit_code_from_report(report))


if __name__ == "__main__":
    main()
