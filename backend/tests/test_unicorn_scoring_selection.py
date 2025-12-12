from datetime import date
from types import SimpleNamespace

from backend.app.unicorns.engine import MAX_PER_PATTERN_PER_DAY, _select_top50, apply_min_score_spacing
from backend.app.unicorns.scoring import compute_scores


class DummyPattern(SimpleNamespace):
    pass


def test_rank_spread_z_used_for_tiny_stddev():
    pattern = DummyPattern(
        order_direction="desc",
        target_sample=0,
        unicorn_weight=1.0,
        public_weight=1.0,
        category=None,
    )
    rows = [{"entity_id": i, "metric_value": 1.0, "sample_size": 10} for i in range(4)]
    scored = compute_scores(pattern, rows, lambda _: 1.0)
    z_values = [round(r.z_raw, 3) for r in scored]
    assert len(set(z_values)) == 4  # rank spread assigns unique values
    assert max(z_values) == 2.0
    assert min(z_values) == 0.0


def test_select_top50_limits_per_pattern_and_uniqueness():
    run_date = date(2025, 3, 27)
    rows = []
    # Pattern A: 7 candidates (should cap at 5)
    for idx in range(7):
        res = SimpleNamespace(
            run_date=run_date,
            entity_id=idx + 1,
            pattern_id="PAT-A",
            entity_type="player",
            metric_value=1.0,
            sample_size=10,
            score=100 - idx,
        )
        rows.append((res, f"Player{idx+1}", "TeamA", "{{player_name}} test"))

    # Pattern B: 2 candidates, one duplicate entity_id that should be skipped
    res_b1 = SimpleNamespace(
        run_date=run_date,
        entity_id=100,
        pattern_id="PAT-B",
        entity_type="player",
        metric_value=1.0,
        sample_size=5,
        score=50,
    )
    res_b2 = SimpleNamespace(
        run_date=run_date,
        entity_id=1,  # duplicate of PAT-A entity_id, should be skipped
        pattern_id="PAT-B",
        entity_type="player",
        metric_value=1.0,
        sample_size=5,
        score=40,
    )
    rows.extend(
        [
            (res_b1, "Player100", "TeamB", "{{player_name}} test"),
            (res_b2, "DupPlayer", "TeamB", "{{player_name}} test"),
        ]
    )

    top = _select_top50(rows, run_date, recent_top50={}, recent_top10={})
    pattern_a_count = sum(1 for r in top if r.pattern_id == "PAT-A")
    pattern_b_count = sum(1 for r in top if r.pattern_id == "PAT-B")
    entity_ids = [r.entity_id for r in top]

    assert pattern_a_count == MAX_PER_PATTERN_PER_DAY
    assert pattern_b_count == 1  # duplicate entity_id skipped
    assert len(entity_ids) == len(set(entity_ids))  # unique players only


def test_apply_min_score_spacing_monotonic():
    rows = [
        SimpleNamespace(score=1.0, entity_id=1, pattern_id="A"),
        SimpleNamespace(score=0.995, entity_id=2, pattern_id="A"),
        SimpleNamespace(score=0.994, entity_id=3, pattern_id="A"),
        SimpleNamespace(score=0.90, entity_id=4, pattern_id="A"),
    ]
    apply_min_score_spacing(rows, min_rel_gap=0.1)
    # order unchanged
    assert [r.entity_id for r in rows] == [1, 2, 3, 4]
    # enforce gaps >=10%
    for i in range(1, len(rows)):
        assert rows[i].score <= rows[i - 1].score * (1 - 0.1) + 1e-9


def test_cooldown_skips_recent_dominator():
    run_date = date(2025, 3, 27)
    # Candidate rows sorted by score desc.
    dom = SimpleNamespace(
        run_date=run_date,
        entity_id=1,
        pattern_id="PAT-A",
        entity_type="player",
        metric_value=10.0,
        sample_size=10,
        score=100,
    )
    alt = SimpleNamespace(
        run_date=run_date,
        entity_id=2,
        pattern_id="PAT-A",
        entity_type="player",
        metric_value=9.0,
        sample_size=10,
        score=90,
    )
    rows = [
        (dom, "Dom", "TeamX", "{{player_name}} test"),
        (alt, "Alt", "TeamX", "{{player_name}} test"),
    ]
    recent_top50 = {1: 10}  # dominator exceeds limit
    recent_top10 = {1: 10}
    top = _select_top50(rows, run_date, recent_top50, recent_top10)
    assert [r.entity_id for r in top] == [2]  # dominator skipped, next best kept


def test_short_list_when_constraints_filter_out():
    run_date = date(2025, 3, 27)
    rows = []
    # Only one candidate passes; others exceed per-pattern cap and cooldown.
    for idx in range(6):
        res = SimpleNamespace(
            run_date=run_date,
            entity_id=idx + 1,
            pattern_id="PAT-A",
            entity_type="player",
            metric_value=1.0,
            sample_size=10,
            score=100 - idx,
        )
        rows.append((res, f"P{idx+1}", "TeamA", "{{player_name}} test"))
    recent_top50 = {i: 5 for i in range(1, 6)}  # first five blocked by cooldown
    recent_top10 = {}
    top = _select_top50(rows, run_date, recent_top50, recent_top10)
    assert len(top) == 1
    assert top[0].rank == 1
    assert top[0].entity_id == 6
