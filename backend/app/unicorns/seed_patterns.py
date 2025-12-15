"""Seed initial pattern_templates as defined in section 14 of the spec."""
from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
from typing import List

from backend.app.core.logging import logger
from backend.app.db import models
from backend.app.db.session import SessionLocal


SEED_PATTERNS: List[dict] = [
    {
        "pattern_id": "UNQ-H-0001",
        "name": "Most Barrels Last 50 PA",
        "description_template": "{{player_name}} has the most barrels in MLB over his last 50 PA ({{metric_value}} barrels).",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "A_BARRELS",
        "filters_json": {"conditions": [{"field": "is_barrel", "op": "=", "value": True}]},
        "order_direction": "desc",
        "metric": "count_barrels",
        "target_sample": 50,
        "min_sample": 10,
        "unicorn_weight": Decimal("1.2"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-H-0002",
        "name": "Highest Hard-Hit Rate Last 50 PA",
        "description_template": "{{player_name}} leads MLB in hard-hit rate over his last 50 PA.",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "A_BARRELS",
        "filters_json": {"conditions": []},
        "order_direction": "desc",
        "metric": "hard_hit_rate",
        "target_sample": 50,
        "min_sample": 10,
        "unicorn_weight": Decimal("1.2"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-H-0003",
        "name": "Highest xwOBA Last 50 PA",
        "description_template": "{{player_name}} tops MLB in xwOBA over his last 50 PA.",
        "entity_type": "batter",
        "base_table": "pa_facts",
        "category": "A_BARRELS",
        "filters_json": {"conditions": []},
        "order_direction": "desc",
        "metric": "xwoba_avg",
        "target_sample": 50,
        "min_sample": 10,
        "unicorn_weight": Decimal("1.2"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-H-0004",
        "name": "Lowest Chase % Last 50 PA",
        "description_template": "{{player_name}} owns the lowest chase rate over his last 50 PA.",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "A_BARRELS",
        "filters_json": {"conditions": []},
        "order_direction": "asc",
        "metric": "chase_rate",
        "target_sample": 50,
        "min_sample": 10,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.0"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-H-0005",
        "name": "Highest Contact % Last 50 PA",
        "description_template": "{{player_name}} leads MLB in contact rate over his last 50 PA.",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "A_BARRELS",
        "filters_json": {"conditions": []},
        "order_direction": "desc",
        "metric": "contact_rate",
        "target_sample": 50,
        "min_sample": 10,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.0"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-H-0010",
        "name": "Most Opposite-Field HR (Season-To-Date)",
        "description_template": "{{player_name}} has the most oppo-field HR this season.",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "B_DIRECTION",
        "filters_json": {"conditions": [
            {"field": "is_hr", "op": "=", "value": True},
            {"field": "hit_direction", "op": "=", "value": "oppo"}
        ]},
        "order_direction": "desc",
        "metric": "count_hr",
        "target_sample": 0,
        "min_sample": 1,
        "unicorn_weight": Decimal("1.2"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-H-0011",
        "name": "Most Pulled HR (Season-To-Date)",
        "description_template": "{{player_name}} has the most pulled HR this season.",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "B_DIRECTION",
        "filters_json": {"conditions": [
            {"field": "is_hr", "op": "=", "value": True},
            {"field": "hit_direction", "op": "=", "value": "pull"}
        ]},
        "order_direction": "desc",
        "metric": "count_hr",
        "target_sample": 0,
        "min_sample": 1,
        "unicorn_weight": Decimal("1.2"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-H-0012",
        "name": "Most 100+ EV Oppo Hits on Low-Away Sliders",
        "description_template": "{{player_name}} has the most 100+ EV opposite-field hits on low-away sliders.",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "B_DIRECTION",
        "filters_json": {"conditions": [
            {"field": "launch_speed", "op": ">=", "value": 100},
            {"field": "hit_direction", "op": "=", "value": "oppo"},
            {"field": "loc_region", "op": "=", "value": "low_away"},
            {"field": "pitch_type", "op": "=", "value": "SL"}
        ]},
        "order_direction": "desc",
        "metric": "count_hr",
        "metric_expr": "COUNT(*)",
        "target_sample": 0,
        "min_sample": 1,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.0"),
        "complexity_score": 4,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-H-0020",
        "name": "Most HR in 3-0 Counts",
        "description_template": "{{player_name}} leads MLB in HR hit in 3-0 counts.",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "COUNT",
        "filters_json": {"conditions": [
            {"field": "is_hr", "op": "=", "value": True},
            {"field": "count_str", "op": "=", "value": "3-0"}
        ]},
        "order_direction": "desc",
        "metric": "count_hr",
        "target_sample": 0,
        "min_sample": 1,
        "unicorn_weight": Decimal("1.2"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 2,
        "requires_count": True,
        "count_value": "3-0",
    },
    {
        "pattern_id": "UNQ-H-0021",
        "name": "Highest xwOBA in 3-2 Counts",
        "description_template": "{{player_name}} has the best xwOBA in MLB in 3-2 counts.",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "COUNT",
        "filters_json": {"conditions": [
            {"field": "count_str", "op": "=", "value": "3-2"}
        ]},
        "order_direction": "desc",
        "metric": "xwoba_avg",
        "metric_expr": "AVG((SELECT p.xwoba FROM pa_facts p WHERE p.pa_id = pitch_facts.pa_id))",
        "target_sample": 0,
        "min_sample": 5,
        "unicorn_weight": Decimal("1.2"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": True,
        "count_value": "3-2",
    },
    {
        "pattern_id": "UNQ-P-0100",
        "name": "Lowest xwOBA Allowed Over Last 3 Starts",
        "description_template": "{{player_name}} has the lowest xwOBA allowed over his last 3 starts.",
        "entity_type": "pitcher",
        "base_table": "pa_facts",
        "category": "STARTER",
        "filters_json": {"conditions": []},
        "order_direction": "asc",
        "metric": "xwoba_avg",
        "target_sample": 60,
        "min_sample": 20,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-P-0101",
        "name": "Highest Whiff % Over Last 3 Starts",
        "description_template": "{{player_name}} has the highest whiff rate over his last 3 starts.",
        "entity_type": "pitcher",
        "base_table": "pitch_facts",
        "category": "STARTER",
        "filters_json": {"conditions": []},
        "order_direction": "desc",
        "metric": "whiff_rate",
        "target_sample": 60,
        "min_sample": 20,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-P-0102",
        "name": "Lowest Hard-Hit % Allowed Over Last 3 Starts",
        "description_template": "{{player_name}} allows the lowest hard-hit rate over his last 3 starts.",
        "entity_type": "pitcher",
        "base_table": "pitch_facts",
        "category": "STARTER",
        "filters_json": {"conditions": []},
        "order_direction": "asc",
        "metric": "hard_hit_rate",
        "target_sample": 60,
        "min_sample": 20,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-P-0103",
        "name": "Most Strikeouts Over Last 3 Starts",
        "description_template": "{{player_name}} has the most strikeouts over his last 3 starts.",
        "entity_type": "pitcher",
        "base_table": "pa_facts",
        "category": "STARTER",
        "filters_json": {"conditions": []},
        "order_direction": "desc",
        "metric": "total_k_last_3_starts",
        "metric_expr": "SUM(CASE WHEN result = 'K' THEN 1 ELSE 0 END)",
        "target_sample": 60,
        "min_sample": 20,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-R-0200",
        "name": "Lowest xwOBA Allowed Over Last 5 Relief Appearances",
        "description_template": "{{player_name}} has the lowest xwOBA allowed over his last 5 relief outings.",
        "entity_type": "pitcher",
        "base_table": "pa_facts",
        "category": "RELIEVER",
        "filters_json": {"conditions": []},
        "order_direction": "asc",
        "metric": "xwoba_avg",
        "target_sample": 40,
        "min_sample": 10,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.0"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-R-0201",
        "name": "Highest Whiff % Over Last 5 Relief Appearances",
        "description_template": "{{player_name}} has the highest whiff rate over his last 5 relief outings.",
        "entity_type": "pitcher",
        "base_table": "pitch_facts",
        "category": "RELIEVER",
        "filters_json": {"conditions": []},
        "order_direction": "desc",
        "metric": "whiff_rate",
        "target_sample": 40,
        "min_sample": 10,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.0"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-R-0202",
        "name": "Lowest Hard-Hit % Allowed Over Last 5 Relief Appearances",
        "description_template": "{{player_name}} allows the lowest hard-hit rate over his last 5 relief outings.",
        "entity_type": "pitcher",
        "base_table": "pitch_facts",
        "category": "RELIEVER",
        "filters_json": {"conditions": []},
        "order_direction": "asc",
        "metric": "hard_hit_rate",
        "target_sample": 40,
        "min_sample": 10,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.0"),
        "complexity_score": 1,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-P-0300",
        "name": "Most HR Allowed After 75+ Pitches",
        "description_template": "{{player_name}} has allowed the most HR after reaching 75 pitches.",
        "entity_type": "pitcher",
        "base_table": "pitch_facts",
        "category": "FATIGUE",
        "filters_json": {"conditions": [
            {"field": "is_hr", "op": "=", "value": True},
            {"field": "pitch_number_game", "op": ">=", "value": 75}
        ]},
        "order_direction": "desc",
        "metric": "count_hr",
        "target_sample": 0,
        "min_sample": 1,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 2,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-P-0301",
        "name": "Lowest xwOBA Allowed After 70+ Pitches",
        "description_template": "{{player_name}} owns the lowest contact quality after 70 pitches.",
        "entity_type": "pitcher",
        "base_table": "pitch_facts",
        "category": "FATIGUE",
        "filters_json": {"conditions": [
            {"field": "pitch_number_game", "op": ">=", "value": 70}
        ]},
        "order_direction": "asc",
        "metric": "xwoba_avg",
        "metric_expr": "AVG((SELECT p.xwoba FROM pa_facts p WHERE p.pa_id = pitch_facts.pa_id))",
        "target_sample": 0,
        "min_sample": 10,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 2,
        "requires_count": False,
        "count_value": None,
    },
    {
        "pattern_id": "UNQ-H-0400",
        "name": "Most HR in Coors Field",
        "description_template": "{{player_name}} has the most HR in Coors Field this season.",
        "entity_type": "batter",
        "base_table": "pitch_facts",
        "category": "PARK",
        "filters_json": {"conditions": [
            {"field": "games.venue_id", "op": "=", "value": 1},
            {"field": "is_hr", "op": "=", "value": True}
        ]},
        "order_direction": "desc",
        "metric": "count_hr",
        "target_sample": 0,
        "min_sample": 1,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.2"),
        "complexity_score": 2,
        "requires_count": False,
        "count_value": None,
    },
]


# --- Core raw-count hitter stats (data-driven patterns) ---

WINDOW_LAST_50_AB = {"type": "last_n_ab", "n": 50}
WINDOW_LAST_7_DAYS = {"type": "last_n_days", "n": 7}
WINDOW_SEASON_TO_DATE_PA = {"type": "last_n_pa", "n": 10000}

FASTBALL_TYPES = ["FF", "FT", "SI", "FC", "FA", "FS"]
BREAKING_TYPES = ["SL", "CU", "KC", "SC", "SV", "KN"]
OFFSPEED_TYPES = ["CH", "FO", "EP"]

RATE_ORDER_EXPR = "metric_value::float / NULLIF(sample_size, 0)"

BBE_BASE_CONDITIONS = [
    {"field": "is_last_pitch_of_pa", "op": "=", "value": True},
    {"field": "launch_speed", "op": "IS NOT NULL"},
]

AB_LAST_PITCH_CONDITIONS = [
    {"field": "is_last_pitch_of_pa", "op": "=", "value": True},
]

BARREL_COND = "launch_speed >= 98 AND launch_angle BETWEEN 26 AND 30"


def _sum_case(cond_sql: str) -> str:
    return f"SUM(CASE WHEN {cond_sql} THEN 1 ELSE 0 END)"


def _core_hitter_pattern(
    *,
    pattern_id: str,
    name: str,
    description_template: str,
    metric_expr: str,
    order_direction: str,
    min_sample: int,
    complexity_score: int,
    window: dict | None = WINDOW_LAST_50_AB,
    base_table: str = "pitch_facts",
    base_conditions: list[dict] | None = None,
    extra_conditions: list[dict] | None = None,
    order_expr: str | None = None,
    sample_expr: str | None = None,
) -> dict:
    conditions: list[dict] = []
    if base_conditions:
        conditions.extend(base_conditions)
    if extra_conditions:
        conditions.extend(extra_conditions)

    filters_json: dict = {"conditions": conditions}
    if window:
        filters_json["window"] = window
    if order_expr:
        filters_json["order_expr"] = order_expr
    if sample_expr:
        filters_json["sample_expr"] = sample_expr

    return {
        "pattern_id": pattern_id,
        "name": name,
        "description_template": description_template,
        "entity_type": "batter",
        "base_table": base_table,
        "category": "CORE_RAW_COUNTS",
        "filters_json": filters_json,
        "order_direction": order_direction,
        "metric": "custom",
        "metric_expr": metric_expr,
        "target_sample": 0,
        "min_sample": min_sample,
        "unicorn_weight": Decimal("1.0"),
        "public_weight": Decimal("1.0"),
        "complexity_score": complexity_score,
        "requires_count": False,
        "count_value": None,
    }


def _surge_expr(*, current_cond: str, season_cond: str) -> str:
    # Δ = current_count - expected_count; expected_count = season_rate * current_BBE
    return (
        f"({_sum_case(current_cond)} - ("
        f"(SELECT {_sum_case(season_cond)}::float / NULLIF(COUNT(*), 0) "
        f" FROM pitch_facts pf2"
        f" JOIN games g2 USING (game_id)"
        f" WHERE g2.game_date <= :as_of_date"
        f"   AND pf2.batter_id = windowed.batter_id"
        f"   AND pf2.is_last_pitch_of_pa = TRUE"
        f"   AND pf2.launch_speed IS NOT NULL"
        f") * COUNT(*)))"
    )


CORE_RAW_COUNT_PATTERNS: List[dict] = [
    # Core raw counts (BBE denom; window = last 50 AB)
    _core_hitter_pattern(
        pattern_id="UNQ-H-0500",
        name="Most Barrels (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} barrels ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0501",
        name="Most 100+ EV Balls (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} balls hit 100+ mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 100"),
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0502",
        name="Most 105+ EV Balls (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} balls hit 105+ mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 105"),
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0503",
        name="Most 110+ EV Balls (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} balls hit 110+ mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 110"),
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0504",
        name="Most 95+ EV Balls (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} balls hit 95+ mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 95"),
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0505",
        name="Most 90+ EV Balls (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} balls hit 90+ mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 90"),
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0506",
        name='Most "Perfect-Perfect-ish" (Last 50 AB)',
        description_template="{{player_name}}: {{metric_value}} balls hit 100+ mph with an 8–32° launch angle ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 100 AND launch_angle BETWEEN 8 AND 32"),
        order_direction="desc",
        min_sample=10,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0507",
        name='Most "HR Window" Balls (Last 50 AB)',
        description_template="{{player_name}}: {{metric_value}} balls hit 98+ mph with a 20–35° launch angle ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 98 AND launch_angle BETWEEN 20 AND 35"),
        order_direction="desc",
        min_sample=10,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0508",
        name="Most Hard Air Balls (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} hard-hit air balls (95+ mph, 10°+ launch angle) in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 95 AND launch_angle >= 10"),
        order_direction="desc",
        min_sample=10,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0509",
        name="Most Hard Pulled Air Balls (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} pulled hard-hit air balls (95+ mph, 10–35° launch angle) in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("hit_direction = 'pull' AND launch_speed >= 95 AND launch_angle BETWEEN 10 AND 35"),
        order_direction="desc",
        min_sample=10,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),

    # Rates (still display X/Y; sort by rate via order_expr)
    _core_hitter_pattern(
        pattern_id="UNQ-H-0510",
        name="Highest Barrel% (Last 50 AB)",
        description_template="{{player_name}}: barrel rate leader with {{metric_value}} barrels in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0511",
        name="Lowest Barrel% (Last 50 AB)",
        description_template="{{player_name}}: lowest barrel rate with {{metric_value}} barrels in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="asc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0512",
        name="Highest HardHit% (Last 50 AB)",
        description_template="{{player_name}}: hard-hit rate leader with {{metric_value}} hard-hit balls (95+ mph) in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 95"),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0513",
        name="Lowest HardHit% (Last 50 AB)",
        description_template="{{player_name}}: lowest hard-hit rate with {{metric_value}} hard-hit balls (95+ mph) in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 95"),
        order_direction="asc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0514",
        name="Highest 100+ EV Rate (Last 50 AB)",
        description_template="{{player_name}}: 100+ mph rate leader with {{metric_value}} balls hit 100+ mph in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 100"),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0515",
        name="Lowest 100+ EV Rate (Last 50 AB)",
        description_template="{{player_name}}: lowest 100+ mph rate with {{metric_value}} balls hit 100+ mph in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 100"),
        order_direction="asc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),

    # Pitch-type specific damage (denom = BBE vs pitch bucket; window = last 50 AB)
    _core_hitter_pattern(
        pattern_id="UNQ-H-0520",
        name="Highest Barrel% vs 95+ Fastballs (Last 50 AB)",
        description_template="{{player_name}}: barrel rate vs 95+ mph fastballs: {{metric_value}} barrels ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=5,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[
            {"field": "pitch_type", "op": "IN", "value": FASTBALL_TYPES},
            {"field": "vel", "op": ">=", "value": 95},
        ],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0521",
        name="Most Barrels vs 95+ Fastballs (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} barrels vs 95+ mph fastballs ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        min_sample=5,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[
            {"field": "pitch_type", "op": "IN", "value": FASTBALL_TYPES},
            {"field": "vel", "op": ">=", "value": 95},
        ],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0522",
        name="Most 100+ EV vs 95+ Fastballs (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} balls hit 100+ mph vs 95+ mph fastballs ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 100"),
        order_direction="desc",
        min_sample=5,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[
            {"field": "pitch_type", "op": "IN", "value": FASTBALL_TYPES},
            {"field": "vel", "op": ">=", "value": 95},
        ],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0523",
        name="Highest Avg EV vs 95+ Fastballs (Last 50 AB)",
        description_template="{{player_name}}: average exit velocity vs 95+ mph fastballs = {{metric_value}} mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr="AVG(launch_speed)",
        order_direction="desc",
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[
            {"field": "pitch_type", "op": "IN", "value": FASTBALL_TYPES},
            {"field": "vel", "op": ">=", "value": 95},
        ],
        window=WINDOW_LAST_50_AB,
    ),

    _core_hitter_pattern(
        pattern_id="UNQ-H-0524",
        name="Highest Barrel% vs Breaking (Last 50 AB)",
        description_template="{{player_name}}: barrel rate vs breaking balls: {{metric_value}} barrels ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "pitch_type", "op": "IN", "value": BREAKING_TYPES}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0525",
        name="Most Barrels vs Breaking (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} barrels vs breaking balls ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "pitch_type", "op": "IN", "value": BREAKING_TYPES}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0526",
        name="Most 100+ EV vs Breaking (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} balls hit 100+ mph vs breaking balls ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 100"),
        order_direction="desc",
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "pitch_type", "op": "IN", "value": BREAKING_TYPES}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0527",
        name="Highest Avg EV vs Breaking (Last 50 AB)",
        description_template="{{player_name}}: average exit velocity vs breaking balls = {{metric_value}} mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr="AVG(launch_speed)",
        order_direction="desc",
        min_sample=5,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "pitch_type", "op": "IN", "value": BREAKING_TYPES}],
        window=WINDOW_LAST_50_AB,
    ),

    _core_hitter_pattern(
        pattern_id="UNQ-H-0528",
        name="Highest Barrel% vs Offspeed (Last 50 AB)",
        description_template="{{player_name}}: barrel rate vs offspeed pitches: {{metric_value}} barrels ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "pitch_type", "op": "IN", "value": OFFSPEED_TYPES}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0529",
        name="Most Barrels vs Offspeed (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} barrels vs offspeed pitches ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "pitch_type", "op": "IN", "value": OFFSPEED_TYPES}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0530",
        name="Most 100+ EV vs Offspeed (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} balls hit 100+ mph vs offspeed pitches ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 100"),
        order_direction="desc",
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "pitch_type", "op": "IN", "value": OFFSPEED_TYPES}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0531",
        name="Highest Avg EV vs Offspeed (Last 50 AB)",
        description_template="{{player_name}}: average exit velocity vs offspeed pitches = {{metric_value}} mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr="AVG(launch_speed)",
        order_direction="desc",
        min_sample=5,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "pitch_type", "op": "IN", "value": OFFSPEED_TYPES}],
        window=WINDOW_LAST_50_AB,
    ),

    # Pulled-air specialists
    _core_hitter_pattern(
        pattern_id="UNQ-H-0540",
        name="Most Pulled-Air Barrels (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} pulled-air barrels ({{sample_size}} pulled air balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        min_sample=5,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[
            {"field": "hit_direction", "op": "=", "value": "pull"},
            {"field": "launch_angle", "op": ">=", "value": 10},
        ],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0541",
        name="Highest Pulled-Air Barrel% (Last 50 AB)",
        description_template="{{player_name}}: pulled-air barrel rate leader with {{metric_value}} barrels ({{sample_size}} pulled air balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=5,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[
            {"field": "hit_direction", "op": "=", "value": "pull"},
            {"field": "launch_angle", "op": ">=", "value": 10},
        ],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0542",
        name="Most Pulled Hard-Air Balls (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} pulled hard-hit air balls (95+ mph, 10°+ launch angle) in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("hit_direction = 'pull' AND launch_speed >= 95 AND launch_angle >= 10"),
        order_direction="desc",
        min_sample=10,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0543",
        name="Highest Pulled Hard-Air Share (Last 50 AB)",
        description_template="{{player_name}}: pulled hard-air share with {{metric_value}} pulled hard-hit air balls in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("hit_direction = 'pull' AND launch_speed >= 95 AND launch_angle >= 10"),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=10,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0544",
        name='Most Pulled "HR-Window" Balls (Last 50 AB)',
        description_template="{{player_name}}: {{metric_value}} pulled balls hit 98+ mph with a 20–35° launch angle in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("hit_direction = 'pull' AND launch_speed >= 98 AND launch_angle BETWEEN 20 AND 35"),
        order_direction="desc",
        min_sample=10,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),

    # Air contact quality (air BBE denom; LA>=10 filter)
    _core_hitter_pattern(
        pattern_id="UNQ-H-0550",
        name="Most Air Barrels (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} air barrels ({{sample_size}} air balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "launch_angle", "op": ">=", "value": 10}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0551",
        name="Highest Air Barrel% (Last 50 AB)",
        description_template="{{player_name}}: air barrel rate leader with {{metric_value}} barrels ({{sample_size}} air balls, last 50 AB).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "launch_angle", "op": ">=", "value": 10}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0552",
        name="Most Hard Air Balls (Air BBE) (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} hard-hit air balls (95+ mph) in {{sample_size}} air balls (last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 95"),
        order_direction="desc",
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "launch_angle", "op": ">=", "value": 10}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0553",
        name="Highest Hard-Air% (Last 50 AB)",
        description_template="{{player_name}}: hard-hit air rate leader with {{metric_value}} hard-hit air balls (95+ mph) in {{sample_size}} air balls (last 50 AB).",
        metric_expr=_sum_case("launch_speed >= 95"),
        order_direction="desc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "launch_angle", "op": ">=", "value": 10}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0554",
        name="Highest Avg EV on Air Balls (Last 50 AB)",
        description_template="{{player_name}}: average exit velocity on air balls = {{metric_value}} mph ({{sample_size}} air balls, last 50 AB).",
        metric_expr="AVG(launch_speed)",
        order_direction="desc",
        min_sample=5,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "launch_angle", "op": ">=", "value": 10}],
        window=WINDOW_LAST_50_AB,
    ),

    # Breakout deltas (recent vs baseline; use BBE window for recent)
    _core_hitter_pattern(
        pattern_id="UNQ-H-0560",
        name="Barrels Surge (Δ vs Season Rate) (Last 50 AB)",
        description_template="{{player_name}}: ΔBarrels = {{metric_value}} ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_surge_expr(
            current_cond=BARREL_COND,
            season_cond="pf2.launch_speed >= 98 AND pf2.launch_angle BETWEEN 26 AND 30",
        ),
        order_direction="desc",
        min_sample=10,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0561",
        name="100+ EV Surge (Δ vs Season Rate) (Last 50 AB)",
        description_template="{{player_name}}: Δ100+ = {{metric_value}} ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_surge_expr(current_cond="launch_speed >= 100", season_cond="pf2.launch_speed >= 100"),
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0562",
        name="HardHit Surge (Δ vs Season Rate) (Last 50 AB)",
        description_template="{{player_name}}: ΔHardHit = {{metric_value}} ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_surge_expr(current_cond="launch_speed >= 95", season_cond="pf2.launch_speed >= 95"),
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0563",
        name="Pulled Hard-Air Surge (Δ vs Season Rate) (Last 50 AB)",
        description_template="{{player_name}}: ΔPulledHardAir = {{metric_value}} ({{sample_size}} batted balls, last 50 AB).",
        metric_expr=_surge_expr(
            current_cond="hit_direction = 'pull' AND launch_speed >= 95 AND launch_angle >= 10",
            season_cond="pf2.hit_direction = 'pull' AND pf2.launch_speed >= 95 AND pf2.launch_angle >= 10",
        ),
        order_direction="desc",
        min_sample=10,
        complexity_score=3,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),

    # Avoiding weak contact (reverse unicorns)
    _core_hitter_pattern(
        pattern_id="UNQ-H-0570",
        name="Fewest Weak Contact Balls (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} weak-contact balls (under 80 mph) in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("launch_speed < 80"),
        order_direction="asc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0571",
        name='Lowest "Soft Air" Rate (Last 50 AB)',
        description_template="{{player_name}}: {{metric_value}} soft air balls (under 90 mph) in {{sample_size}} air balls (last 50 AB).",
        metric_expr=_sum_case("launch_speed < 90"),
        order_direction="asc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=5,
        complexity_score=2,
        base_conditions=BBE_BASE_CONDITIONS,
        extra_conditions=[{"field": "launch_angle", "op": ">=", "value": 10}],
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0572",
        name="Lowest Pop-up Rate (Last 50 AB)",
        description_template="{{player_name}}: {{metric_value}} pop-ups in {{sample_size}} batted balls (last 50 AB).",
        metric_expr=_sum_case("batted_ball_type = 'popup'"),
        order_direction="asc",
        order_expr=RATE_ORDER_EXPR,
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),

    # Extreme ceiling
    _core_hitter_pattern(
        pattern_id="UNQ-H-0580",
        name="Top Max EV (Last 50 AB)",
        description_template="{{player_name}}: max exit velocity = {{metric_value}} mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr="MAX(launch_speed)",
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0581",
        name="Top 2nd-Highest EV (Last 50 AB)",
        description_template="{{player_name}}: second-highest exit velocity = {{metric_value}} mph ({{sample_size}} batted balls, last 50 AB).",
        metric_expr="(array_agg(launch_speed ORDER BY launch_speed DESC))[2]",
        order_direction="desc",
        min_sample=10,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0582",
        name="Most Consecutive Games with 95+ EV (Current Streak)",
        description_template="{{player_name}}: 95+ mph batted-ball streak = {{metric_value}} games ({{sample_size}} games tracked).",
        base_table="pa_facts",
        window=WINDOW_SEASON_TO_DATE_PA,
        base_conditions=[],
        sample_expr="COUNT(DISTINCT game_id)",
        metric_expr=dedent(
            """
            (
              SELECT COALESCE(
                array_position(flags, 0) - 1,
                array_length(flags, 1),
                0
              )
              FROM (
                SELECT array_agg(has_95 ORDER BY game_date DESC, game_id DESC) AS flags
                FROM (
                  SELECT
                    g.game_date AS game_date,
                    pa.game_id AS game_id,
                    MAX(
                      CASE
                        WHEN pf.is_last_pitch_of_pa = TRUE
                         AND pf.launch_speed >= 95
                        THEN 1
                        ELSE 0
                      END
                    ) AS has_95
                  FROM pa_facts pa
                  JOIN games g ON g.game_id = pa.game_id
                  LEFT JOIN pitch_facts pf
                    ON pf.game_id = pa.game_id
                   AND pf.batter_id = pa.batter_id
                  WHERE g.game_date <= :as_of_date
                    AND pa.batter_id = windowed.batter_id
                  GROUP BY g.game_date, pa.game_id
                ) gf
              ) flags_sub
            )
            """
        ).strip(),
        order_direction="desc",
        min_sample=5,
        complexity_score=2,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0583",
        name="Most Games with a Barrel (Last 50 AB)",
        description_template="{{player_name}}: games with at least one barrel = {{metric_value}} ({{sample_size}} games, last 50 AB).",
        metric_expr="COUNT(DISTINCT CASE WHEN launch_speed >= 98 AND launch_angle BETWEEN 26 AND 30 THEN game_id END)",
        sample_expr="COUNT(DISTINCT game_id)",
        order_direction="desc",
        min_sample=5,
        complexity_score=2,
        base_conditions=AB_LAST_PITCH_CONDITIONS,
        window=WINDOW_LAST_50_AB,
    ),
    _core_hitter_pattern(
        pattern_id="UNQ-H-0584",
        name='"3-Barrel Week" (Last 7 Days)',
        description_template="{{player_name}}: barrels in the last 7 days = {{metric_value}} ({{sample_size}} batted balls).",
        metric_expr=_sum_case(BARREL_COND),
        order_direction="desc",
        min_sample=5,
        complexity_score=1,
        base_conditions=BBE_BASE_CONDITIONS,
        window=WINDOW_LAST_7_DAYS,
    ),
]

SEED_PATTERNS.extend(CORE_RAW_COUNT_PATTERNS)


def seed() -> None:
    with SessionLocal() as session:
        for data in SEED_PATTERNS:
            existing = session.get(models.PatternTemplate, data["pattern_id"])
            if existing:
                for key, value in data.items():
                    setattr(existing, key, value)
            else:
                session.add(models.PatternTemplate(**data))
        session.commit()
    logger.info("Seeded %s patterns", len(SEED_PATTERNS))


if __name__ == "__main__":
    seed()
