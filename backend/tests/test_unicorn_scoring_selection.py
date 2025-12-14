from datetime import date
from types import SimpleNamespace

from backend.app.unicorns.engine import MAX_PER_PATTERN_PER_DAY, _select_top50, apply_min_score_spacing


class DummyPattern(SimpleNamespace):
    pass


def test_select_top50_limits_per_pattern_and_uniqueness():
    run_date = date(2025, 3, 27)
    rows = []
    # Pattern A: 7 candidates (should cap at MAX_PER_PATTERN_PER_DAY)
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

    top = _select_top50(rows, run_date)
    pattern_a_count = sum(1 for r in top if r.pattern_id == "PAT-A")
    pattern_b_count = sum(1 for r in top if r.pattern_id == "PAT-B")
    entity_ids = [r.entity_id for r in top]

    assert pattern_a_count == MAX_PER_PATTERN_PER_DAY
    assert pattern_b_count == 1
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


def test_short_list_when_pattern_cap_filters_out():
    run_date = date(2025, 3, 27)
    rows = []
    # Only one candidate passes; others exceed per-pattern cap.
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
    top = _select_top50(rows, run_date)
    assert len(top) == 1
    assert top[0].rank == 1
    assert top[0].entity_id == 1
