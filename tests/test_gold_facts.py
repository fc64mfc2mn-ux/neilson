"""Step 12: gold facts and bridges, reconciled to silver.

Unit tests cover the helpers on fixtures; the slow tests use all of gold built
once on the real data (`real_gold_all` in conftest).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.dq import ReconciliationError
from src.common.findings import run_finding
from src.common.tables import read_table
from src.gold.build_dimensions import UNKNOWN_KEY, build_dim_date
from src.gold.build_facts import (
    business_hours_flag,
    call_lead_status,
    check_date_keys,
    date_key,
    policy_gbp,
    resolve_key,
)
from src.gold.build_lead_funnel import bridge_user_region

FINDINGS_POLICY = Path(__file__).resolve().parents[1] / "src" / "sql" / "findings_policy.sql"
REF_REGION = [
    ("UK", "Europe/London", "09:00", "20:00", 30),
    ("AU", "Australia/Sydney", "09:00", "19:00", 30),
    ("US", "America/New_York", "09:00", "21:00", 15),
]


def _regions(spark):
    return spark.createDataFrame(
        REF_REGION,
        "region_code string, timezone string, business_hours_start string, "
        "business_hours_end string, sla_first_call_minutes int",
    )


# ------------------------------------------------------------------ unit


@pytest.mark.parametrize(
    "case, region, ts_utc, expected",
    [
        ("UK Monday 09:30 BST", "UK", "2026-05-11 08:30:00", True),
        ("UK Monday 08:59 BST", "UK", "2026-05-11 07:59:00", False),
        ("UK Monday 19:59 BST", "UK", "2026-05-11 18:59:00", True),
        ("UK Monday 20:00 BST, end exclusive", "UK", "2026-05-11 19:00:00", False),
        ("UK Saturday 12:00 BST", "UK", "2026-05-16 11:00:00", False),
        ("AU Tuesday 09:30 AEST is Monday in UTC", "AU", "2026-05-11 23:30:00", True),
        ("AU Saturday 08:00 AEST is Friday in UTC", "AU", "2026-05-15 22:00:00", False),
        ("US Monday 09:00 EDT, start inclusive", "US", "2026-05-11 13:00:00", True),
        ("US Friday 20:59 EDT is Saturday in UTC", "US", "2026-05-16 00:59:00", True),
    ],
)
def test_business_hours_use_each_regions_own_timezone_weekdays_only(
    spark, case, region, ts_utc, expected
):
    df = spark.createDataFrame([(region, ts_utc)], "region_code string, ts string").withColumn(
        "ts", F.to_timestamp("ts")
    )
    out = business_hours_flag(df, "ts", _regions(spark), "flag").first()
    assert out.flag is expected, case


def test_resolve_key_maps_to_unknown_and_counts_it(spark):
    dim = spark.createDataFrame([(7, "A"), (UNKNOWN_KEY, "UNKNOWN")], "k long, code string")
    fact = spark.createDataFrame([("A",), ("B",), (None,), ("UNKNOWN",)], "code string")
    out, unknown = resolve_key(fact, "code", dim, "code", "k", unknown=True, label="t.code")
    assert out.count() == 4  # never drops a row
    assert sorted(r.k for r in out.collect()) == [UNKNOWN_KEY] * 3 + [7]
    assert unknown == 3


def test_resolve_key_strict_raises_and_never_drops_or_nulls(spark):
    dim = spark.createDataFrame([(7, "UK")], "k long, code string")
    good = spark.createDataFrame([("UK",)], "code string")
    out, unknown = resolve_key(good, "code", dim, "code", "k", unknown=False, label="t.region")
    assert [r.k for r in out.collect()] == [7] and unknown == 0
    for bad in (["XX"], [None]):
        fact = spark.createDataFrame([(v,) for v in ["UK", *bad]], "code string")
        with pytest.raises(ReconciliationError, match="t.region: 1 row"):
            resolve_key(fact, "code", dim, "code", "k", unknown=False, label="t.region")


def test_date_keys_must_exist_in_dim_date(spark):
    dim_date = build_dim_date(spark)
    ok = spark.createDataFrame([(date(2026, 4, 1), None)], "d date, e date")
    check_date_keys(ok, ["d", "e"], dim_date, "t")  # nulls are allowed
    assert ok.select(date_key("d").alias("k")).first().k == 20260401
    bad = spark.createDataFrame([(date(2027, 1, 1),)], "d date")
    with pytest.raises(ReconciliationError, match="t.d: 1 date"):
        check_date_keys(bad, ["d"], dim_date, "t")


def test_policy_gbp_uses_the_application_month_rate_and_never_invents_one(spark):
    policy = spark.createDataFrame(
        [("P1", "CAD", date(2026, 5, 20), Decimal("1000.00")),
         ("P2", "CAD", date(2026, 6, 2), Decimal("1000.00")),  # no CAD June rate
         ("P3", "GBP", date(2026, 7, 1), Decimal("500.00"))],  # no July rates at all
        "policy_id string, currency_code string, application_date date, "
        "annual_premium_local decimal(17,2)",
    )  # fmt: skip
    fx = spark.createDataFrame(
        [("2026-05", "CAD", Decimal("0.587781")), ("2026-06", "GBP", Decimal("1.000000"))],
        "rate_month string, currency_code string, rate_to_gbp decimal(18,6)",
    )
    rows = {r.policy_id: r for r in policy_gbp(policy, fx).collect()}
    assert rows["P1"].annual_premium_gbp == Decimal("587.78")
    assert rows["P1"].gbp_rate_missing is False
    for pid in ("P2", "P3"):
        assert rows[pid].annual_premium_gbp is None and rows[pid].gbp_rate_missing is True


def test_call_lead_status_separates_ghost_and_quarantined_leads(spark):
    calls = spark.createDataFrame(
        [("C1", "L1"), ("C2", "L1-DUP"), ("C3", "LD-GHOST-UK-1"), ("C4", "L-QUAR")],
        "call_id string, lead_id string",
    )
    dedup_map = spark.createDataFrame(
        [("L1", "L1"), ("L1-DUP", "L1")], "source_lead_id string, surviving_lead_id string"
    )
    raw_ids = spark.createDataFrame([("L1",), ("L1-DUP",), ("L-QUAR",)], "lead_id string")
    rows = {r.call_id: (r.surviving_lead_id, r.lead_match_status)
            for r in call_lead_status(calls, dedup_map, raw_ids).collect()}  # fmt: skip
    assert rows == {
        "C1": ("L1", "matched"),
        "C2": ("L1", "matched"),  # a call on a duplicate goes to its survivor
        "C3": (None, "lead_not_found"),
        "C4": (None, "lead_quarantined"),
    }


def test_bridge_user_region_keeps_all_and_maps_two_regions(spark):
    sec = spark.createDataFrame(
        [(" UK.Analyst@northgatelife.example", "UK", "Regional_Analyst"),
         ("anz.manager@northgatelife.example", "AU", "Regional_Analyst"),
         ("anz.manager@northgatelife.example", "US", "Regional_Analyst"),
         ("marketing.analyst@northgatelife.example", "ALL", "Marketing_Analyst_NoPII")],
        "user_email string, region_code string, role_name string",
    )  # fmt: skip
    dim_region = spark.createDataFrame([(1, "UK"), (2, "AU"), (3, "US")], "region_key long, "
                                       "region_code string")  # fmt: skip
    rows = bridge_user_region(sec, dim_region).collect()
    by_user: dict[str, set] = {}
    for r in rows:
        by_user.setdefault(r.user_principal_name, set()).add(
            (r.region_code, r.region_key, r.all_regions_flag)
        )
    assert by_user == {
        "uk.analyst@northgatelife.example": {("UK", 1, False)},
        "anz.manager@northgatelife.example": {("AU", 2, False), ("US", 3, False)},
        "marketing.analyst@northgatelife.example": {("ALL", None, True)},
    }
    # Default deny: a user with no mapping, e.g. group.finance, has no rows at all.
    assert "group.finance@northgatelife.example" not in by_user
    bad = sec.unionByName(spark.createDataFrame([("x@northgatelife.example", "IE", "R")],
                                                sec.schema))  # fmt: skip
    with pytest.raises(ReconciliationError, match="region"):
        bridge_user_region(bad, dim_region).collect()


# ------------------------------------------------------------------ real data


@pytest.fixture(scope="module")
def gold(spark, real_gold_all):
    config, results = real_gold_all
    return config, results, (lambda name: read_table(spark, config, name))


@pytest.mark.slow
def test_each_fact_reconciles_to_silver(gold):
    _, results, t = gold
    assert t("gold.fact_lead").count() == t("silver.lead").count() == 47_201
    assert t("gold.fact_call").count() == t("silver.call").count() == 62_829
    assert t("gold.fact_policy").count() == t("silver.policy").count() == 5_172
    spend, silver_spend = t("gold.fact_marketing_spend_daily"), t("silver.marketing_spend")
    assert spend.count() == silver_spend.count() == 2_001
    assert spend.groupBy("date_key", "campaign_key").count().where("count > 1").count() == 0
    gbp = lambda df: df.agg(F.sum("spend_gbp")).first()[0]  # noqa: E731
    assert gbp(spend) == gbp(silver_spend)  # GBP total reconciles exactly
    for fact in results["facts"]:
        assert fact.rows_out == fact.rows_in, fact.table


@pytest.mark.slow
def test_fact_keys_unique_and_unknowns_counted(gold):
    _, results, t = gold
    for table, key in (("gold.fact_call", "call_key"), ("gold.fact_policy", "policy_key"),
                       ("gold.fact_lead", "lead_key")):  # fmt: skip
        r = t(table).agg(F.count(F.lit(1)).alias("n"), F.countDistinct(key).alias("k"),
                         F.max(key).alias("hi")).first()  # fmt: skip
        assert r.n == r.k and r.hi < 2**53, table
    unknown = {(f.table, k): v for f in results["facts"] for k, v in f.unknown.items()}
    silver_agent_ids = t("silver.agent").select("agent_id").distinct()
    policy_ghost_agents = t("silver.policy").join(silver_agent_ids, "agent_id", "left_anti")
    assert unknown == {
        ("gold.fact_lead", "campaign_key"): 304,  # orphan campaign ids (defect 5)
        ("gold.fact_lead", "channel_key"): 0,
        ("gold.fact_call", "agent_key"): 615,  # AG9001 to AG9004 (defect 17)
        ("gold.fact_policy", "agent_key"): policy_ghost_agents.count(),
        ("gold.fact_marketing_spend_daily", "campaign_key"): 0,
        ("gold.fact_marketing_spend_daily", "channel_key"): 0,
    }
    # Region and product resolve for every row (no UNKNOWN member; strict).
    for table, cols in (("gold.fact_lead", ["region_key", "product_key"]),
                        ("gold.fact_call", ["region_key"]),
                        ("gold.fact_policy", ["region_key", "product_key"]),
                        ("gold.fact_marketing_spend_daily", ["region_key"])):  # fmt: skip
        for c in cols:
            assert t(table).where(F.col(c).isNull() | (F.col(c) < 0)).count() == 0, (table, c)


@pytest.mark.slow
def test_calls_and_policies_without_a_surviving_lead_keep_a_null_lead_key(gold):
    _, _, t = gold
    status = lambda table: {  # noqa: E731
        r.lead_match_status: r["count"]
        for r in t(table).groupBy("lead_match_status").count().collect()
    }
    calls = status("gold.fact_call")
    assert calls == {"matched": 61_690, "lead_not_found": 1_094, "lead_quarantined": 45}
    assert t("gold.fact_call").where("lead_key IS NULL").count() == 1_139
    policies = status("gold.fact_policy")
    assert policies == {"matched": 4_767, "no_lead": 404, "lead_quarantined": 1}
    p = t("gold.fact_policy")
    assert p.where("lead_key IS NULL").count() == 405
    assert p.where("is_lead_attributed").count() == 4_767
    # 35 policies name a call quarantined in silver: no call key, the policy stays.
    assert p.where("call_id IS NOT NULL AND call_key IS NULL").count() == 35
    # No CAD June rate and no July rates: 517 + 158 policies without a GBP premium (defect 12).
    assert p.where("gbp_rate_missing").count() == 675
    assert p.where("gbp_rate_missing AND annual_premium_gbp IS NOT NULL").count() == 0
    finding = run_finding(p.sparkSession, FINDINGS_POLICY, "policy_gbp_rate_missing")
    assert {(r.currency_code, r.application_month): r.policies for r in finding} == {
        ("AUD", "2026-07"): 16, ("CAD", "2026-06"): 517, ("CAD", "2026-07"): 42,
        ("GBP", "2026-07"): 63, ("USD", "2026-07"): 37,
    }  # fmt: skip


@pytest.mark.slow
def test_bridges(gold):
    _, results, t = gold
    bridge = t("gold.bridge_lead_call")
    assert bridge.count() == 62_829 - 1_139 == 61_690
    assert bridge.select("call_key").distinct().count() == 61_690  # each call once
    assert bridge.join(t("gold.dim_lead"), "lead_key", "left_anti").count() == 0
    users = t("gold.bridge_user_region")
    assert users.count() == 7
    assert users.where("all_regions_flag").count() == 1
    two = users.where("user_principal_name = 'anz.manager@northgatelife.example'")
    assert {r.region_code for r in two.collect()} == {"AU", "US"}


@pytest.mark.slow
def test_fact_lead_funnel_is_one_row_per_surviving_lead(gold):
    _, results, t = gold
    funnel = t("gold.fact_lead_funnel")
    assert funnel.count() == funnel.select("lead_key").distinct().count() == 47_201
    assert funnel.count() != t("gold.bridge_lead_call").count()  # the bridge is not the funnel
    assert funnel.join(t("gold.dim_lead"), "lead_key", "left_anti").count() == 0
    s = funnel.agg(
        F.sum("call_attempt_count").alias("calls"),
        F.sum(F.col("policy_key").isNotNull().cast("int")).alias("policies"),
        F.sum((F.col("minutes_to_first_call") < 0).cast("int")).alias("negative"),
    ).first()
    assert s.calls == 61_690  # every bridged call counted once
    assert s.policies == 4_767  # every lead-attributed policy linked once
    assert s.negative == 0  # calls before their lead were quarantined in silver
