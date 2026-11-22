"""`fact_lead_funnel` row count equals distinct surviving `lead_key` count,
on a fixture with a lead carrying 4 calls and 1 policy."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from src.gold.build_lead_funnel import FUNNEL_COLUMNS, bridge_lead_call, lead_funnel

T0 = "2026-05-11 10:00:00"  # a Monday, UTC


def _ts(spark, rows, schema, ts_cols):
    df = spark.createDataFrame(rows, schema)
    for c in ts_cols:
        df = df.withColumn(c, F.to_timestamp(c))  # UTC strings, not host-local datetimes
    return df


def _fixture(spark):
    fact_lead = _ts(
        spark,
        [
            (1, T0, 100, 10, 20, 30, True),
            (2, T0, 100, 10, 20, 30, False),
            (3, T0, 100, -1, -1, 30, True),
        ],
        "lead_key long, lead_created_ts_utc string, region_key long, campaign_key long, "
        "channel_key long, product_key long, lead_in_business_hours_flag boolean",
        ["lead_created_ts_utc"],
    )
    dim_lead = spark.createDataFrame(
        [(1, True, 2), (2, None, 1), (3, False, 1)],
        "lead_key long, consent_phone_current_flag boolean, dedup_cluster_size int",
    )
    fact_call = _ts(
        spark,
        [
            # Lead 1: four calls. The fourth comes after the policy application.
            (501, 1, 11, "2026-05-11 10:10:00", "NO_ANSWER", False),
            (502, 1, 12, "2026-05-11 11:00:00", "CONNECTED", True),
            (503, 1, 13, "2026-05-12 09:00:00", "QUOTED", True),
            (504, 1, 14, "2026-05-16 09:00:00", "CALLBACK", False),
            # Lead 3: one call 40 minutes after creation (SLA is 30).
            (505, 3, -1, "2026-05-11 10:40:00", "VOICEMAIL", False),
        ],
        "call_key long, lead_key long, agent_key long, call_start_ts_utc string, "
        "disposition string, is_connected boolean",
        ["call_start_ts_utc"],
    )
    fact_policy = spark.createDataFrame(
        [
            (900, 1, date(2026, 5, 13), 20260601, Decimal("612.00")),
            (901, None, date(2026, 5, 13), 20260601, Decimal("100.00")),  # no lead
        ],
        "policy_key long, lead_key long, application_date date, policy_start_date_key int, "
        "annual_premium_gbp decimal(14,2)",
    )
    dim_region = spark.createDataFrame([(100, 30)], "region_key long, sla_first_call_minutes int")
    return fact_lead, dim_lead, fact_call, fact_policy, dim_region


def test_lead_funnel_row_count_equals_distinct_lead_key(spark):
    fact_lead, dim_lead, fact_call, fact_policy, dim_region = _fixture(spark)
    funnel = lead_funnel(fact_lead, dim_lead, fact_call, fact_policy, dim_region)
    assert funnel.count() == funnel.select("lead_key").distinct().count() == 3
    # The bridge is call grain, not the funnel: 5 rows for 2 leads with calls.
    bridge = bridge_lead_call(fact_call)
    assert bridge.count() == 5 and bridge.count() != funnel.count()
    assert tuple(funnel.columns[:21]) == FUNNEL_COLUMNS[:21]


def test_lead_funnel_carries_the_lead_to_call_to_policy_path(spark):
    rows = {r.lead_key: r for r in lead_funnel(*_fixture(spark)).collect()}
    one, two, three = rows[1], rows[2], rows[3]
    assert (one.first_call_agent_key, one.last_call_agent_key) == (11, 13)  # last before applying
    assert (one.call_attempt_count, one.connected_call_count) == (4, 2)
    assert (one.minutes_to_first_call, one.within_first_call_sla_flag) == (Decimal("10.00"), True)
    assert (one.quoted_flag, one.application_flag) == (True, True)
    assert (one.policy_key, one.policy_start_date_key, one.days_lead_to_policy) == (
        900, 20260601, 2
    )  # fmt: skip
    assert one.annual_premium_gbp == Decimal("612.00")
    assert (one.consent_phone_current_flag, one.dedup_cluster_size) == (True, 2)
    # No calls, no policy: counts are zero, keys and timings null, flags false.
    assert (two.call_attempt_count, two.connected_call_count) == (0, 0)
    assert (two.first_call_ts_utc, two.first_call_agent_key, two.last_call_agent_key) == (
        None, None, None
    )  # fmt: skip
    assert (two.within_first_call_sla_flag, two.quoted_flag, two.application_flag) == (
        False, False, False
    )  # fmt: skip
    assert (two.policy_key, two.days_lead_to_policy) == (None, None)
    # Called outside the SLA; the UNKNOWN campaign and agent keys pass through.
    assert (three.minutes_to_first_call, three.within_first_call_sla_flag) == (
        Decimal("40.00"), False
    )  # fmt: skip
    assert (three.campaign_key, three.first_call_agent_key) == (-1, -1)
    assert one.lead_in_business_hours_flag is True and two.lead_in_business_hours_flag is False


def test_bridge_lead_call_orders_calls_per_lead_and_drops_unresolved_leads(spark):
    _, _, fact_call, _, _ = _fixture(spark)
    ghost = _ts(
        spark,
        [(506, None, 11, "2026-05-11 12:00:00", "NO_ANSWER", False)],
        "call_key long, lead_key long, agent_key long, call_start_ts_utc string, "
        "disposition string, is_connected boolean",
        ["call_start_ts_utc"],
    )
    bridge = bridge_lead_call(fact_call.unionByName(ghost))
    seq = {r.call_key: (r.lead_key, r.call_sequence) for r in bridge.collect()}
    assert seq == {501: (1, 1), 502: (1, 2), 503: (1, 3), 504: (1, 4), 505: (3, 1)}
