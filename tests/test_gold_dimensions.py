"""Step 10: gold dimensions (dim_date, dim_region, dim_channel, dim_campaign,
dim_product, dim_agent as SCD2). dim_lead is step 11."""

from __future__ import annotations

import hashlib
import shutil
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.dq import ReconciliationError
from src.common.tables import read_table, table_path
from src.gold.build_dimensions import (
    UNKNOWN_KEY,
    build_dim_date,
    build_dimensions,
    dim_agent,
    dim_campaign,
    dim_channel,
    key_coverage,
    resolve_agent,
    surrogate_key,
)

AGENT_COLS = (
    "agent_id string, agent_name string, region_code string, team string, "
    "seniority_band string, active_flag boolean, source_row long"
)


# ------------------------------------------------------------------ unit


def test_surrogate_keys_are_deterministic_and_never_the_unknown_key(spark):
    df = spark.createDataFrame([("UK",), ("CA",), ("UK",)], "code string")
    once = [r.k for r in df.select(surrogate_key("code").alias("k")).collect()]
    again = [r.k for r in df.select(surrogate_key("code").alias("k")).collect()]
    assert once == again  # same input, same key, every run
    assert once[0] == once[2] and once[0] != once[1]
    assert UNKNOWN_KEY == -1 and UNKNOWN_KEY not in once
    assert all(k > 0 for k in once)


def test_surrogate_keys_are_52_bit_and_reproducible_outside_spark(spark):
    """Below 2^53, so Power BI (IEEE double) holds every key exactly.

    The key is the first 13 hex digits (52 bits) of SHA-256 of the "|"-joined
    natural key, so anyone can recompute it without Spark.
    """
    natural = [("UK",), ("AG0008|2026-04-01",), ("CMP-UK-001",), ("x" * 200,)]
    df = spark.createDataFrame(natural, "code string")
    got = [r.k for r in df.select(surrogate_key("code").alias("k")).collect()]
    want = [int(hashlib.sha256(v.encode()).hexdigest()[:13], 16) for (v,) in natural]
    assert got == want
    many = spark.range(20_000).select(surrogate_key(F.col("id").cast("string")).alias("k"))
    stats = many.agg(F.max("k").alias("hi"), F.min("k").alias("lo"),
                     F.countDistinct("k").alias("n")).first()  # fmt: skip
    assert stats.lo >= 0 and stats.hi < 2**53 and stats.n == 20_000
    assert int(float(stats.hi)) == stats.hi  # survives a round trip through a double


def test_dim_date_is_a_full_contiguous_2026_calendar(spark):
    d = build_dim_date(spark)
    rows = {r.date: r for r in d.collect()}
    assert len(rows) == 365 and min(rows) == date(2026, 1, 1) and max(rows) == date(2026, 12, 31)
    assert d.select("date_key").distinct().count() == 365
    r = rows[date(2026, 4, 18)]  # a Saturday
    assert (r.date_key, r.year, r.quarter, r.fiscal_quarter) == (20260418, 2026, 2, "Q2 2026")
    assert (r.year_month, r.month, r.month_name, r.month_start_date) == (
        "2026-04", 4, "April", date(2026, 4, 1)
    )  # fmt: skip
    assert (r.day_of_week, r.day_name, r.is_weekday) == (6, "Saturday", False)
    assert rows[date(2026, 4, 20)].is_weekday is True
    assert r.is_reporting_period is True and rows[date(2026, 7, 1)].is_reporting_period is False


def test_dim_agent_scd2_windows_follow_source_row_order(spark):
    silver = spark.createDataFrame(
        [
            ("A1", "Ann", "CA", "CA-Team-Beta", "Trainee", True, 8),
            ("A1", "Ann", "CA", "CA-Team-Beta", "Core", True, 43),
            ("B1", "Bob", "UK", "UK-Team-Alpha", "Core", True, 2),
        ],
        AGENT_COLS,
    )
    rows = {(r.agent_id, r.seniority_band): r for r in dim_agent(silver).collect()}
    early, late, single = rows[("A1", "Trainee")], rows[("A1", "Core")], rows[("B1", "Core")]
    # Assumption 8: the earlier row covers 2026-01-01 to 2026-03-31, the later row is
    # current from 2026-04-01 (start of the reporting period). valid_to is inclusive.
    assert (early.valid_from, early.valid_to, early.is_current) == (
        date(2026, 1, 1), date(2026, 3, 31), False
    )  # fmt: skip
    assert (late.valid_from, late.valid_to, late.is_current) == (
        date(2026, 4, 1), date(9999, 12, 31), True
    )  # fmt: skip
    assert (single.valid_from, single.valid_to, single.is_current) == (
        date(2026, 1, 1), date(9999, 12, 31), True
    )  # fmt: skip
    unknown = rows[("UNKNOWN", None)]
    assert (unknown.agent_key, unknown.is_current) == (UNKNOWN_KEY, True)
    # Data minimisation: employee names stay in silver (brief tier 2).
    assert "agent_name" not in dim_agent(silver).columns
    assert len({r.agent_key for r in rows.values()}) == 4


def test_dim_agent_has_no_rule_for_three_versions(spark):
    silver = spark.createDataFrame(
        [("A1", "Ann", "CA", "T", s, True, n) for n, s in ((1, "a"), (2, "b"), (3, "c"))],
        AGENT_COLS,
    )
    with pytest.raises(ReconciliationError, match="3 versions"):
        dim_agent(silver)


def test_a_call_resolves_to_the_agent_version_valid_at_the_call_time(spark):
    silver = spark.createDataFrame(
        [
            ("A1", "Ann", "CA", "CA-Team-Beta", "Trainee", True, 8),
            ("A1", "Ann", "CA", "CA-Team-Beta", "Core", True, 43),
        ],
        AGENT_COLS,
    )
    dim = dim_agent(silver)
    keys = {r.seniority_band: r.agent_key for r in dim.where("agent_id = 'A1'").collect()}
    # Timestamps built from UTC strings in Spark: a Python datetime would be read in the
    # host's local zone (BST) and move across midnight.
    calls = spark.createDataFrame(
        [
            ("march", "A1", "2026-03-15 10:00:00"),
            ("last_minute", "A1", "2026-03-31 23:59:00"),
            ("first_minute", "A1", "2026-04-01 00:00:00"),
            ("may", "A1", "2026-05-01 09:00:00"),
            ("ghost", "AG9001", "2026-05-01 09:00:00"),
        ],
        "call_id string, agent_id string, ts string",
    ).withColumn("call_start_ts_utc", F.to_timestamp("ts"))
    out = {r.call_id: r.agent_key for r in resolve_agent(calls, dim).collect()}
    assert out == {
        "march": keys["Trainee"],
        "last_minute": keys["Trainee"],
        "first_minute": keys["Core"],
        "may": keys["Core"],
        "ghost": UNKNOWN_KEY,
    }


def test_channel_and_campaign_dimensions_carry_an_unknown_member(spark):
    channels = spark.createDataFrame(
        [("ORGANIC", "Organic", "Organic", False, 0)],
        "channel_code string, channel_name string, channel_group string, paid_flag boolean, "
        "attribution_window_days int",
    )
    ch = {r.channel_code: r for r in dim_channel(channels).collect()}
    assert ch["UNKNOWN"].channel_key == UNKNOWN_KEY and ch["ORGANIC"].channel_key > 0
    campaigns = spark.createDataFrame(
        [
            (
                "CMP-1",
                "Spring",
                "TV",
                "UK",
                "CI",
                "Leads",
                date(2026, 4, 1),
                date(2026, 6, 30),
                False,
            )
        ],
        "campaign_id string, campaign_name string, channel_code string, region_code string, "
        "product_code string, objective string, start_date date, end_date date, "
        "end_before_start boolean",
    )
    cp = {r.campaign_id: r for r in dim_campaign(campaigns).collect()}
    assert cp["UNKNOWN"].campaign_key == UNKNOWN_KEY and cp["CMP-1"].campaign_key > 0


def test_key_coverage_counts_unmatched_and_nulls(spark):
    fact = spark.createDataFrame([("a",), ("b",), (None,), ("z",)], "code string")
    dim = spark.createDataFrame([("a",), ("b",)], "code string")
    c = key_coverage(fact, "code", dim, "code")
    assert (c.rows, c.matched, c.unmatched, c.null) == (4, 2, 1, 1)


# ------------------------------------------------------------------ real data


@pytest.fixture(scope="module")
def real_gold(spark, real_leads):
    """All silver built once more under its own run id, then the dimensions."""
    from src.silver.calls import build_calls
    from src.silver.policy import build_policy
    from src.silver.spend import build_spend

    config, _, _ = real_leads
    config = {**config, "run_id": "test-gold"}
    build_calls(spark, config)
    build_policy(spark, config)
    build_spend(spark, config)
    return config, {r.table: r for r in build_dimensions(spark, config)}


@pytest.mark.slow
def test_each_dimension_reconciles_to_its_silver_source(spark, real_gold):
    config, results = real_gold
    # table: (silver rows in, gold rows out, UNKNOWN members added)
    expect = {
        "gold.dim_date": (365, 365, 0),  # generated calendar, no silver source
        "gold.dim_region": (4, 4, 0),
        "gold.dim_channel": (8, 9, 1),
        "gold.dim_campaign": (24, 25, 1),
        "gold.dim_product": (3, 3, 0),  # distinct codes across lead, campaign, policy
        "gold.dim_agent": (45, 46, 1),  # 42 agents, 3 with two versions
    }
    for table, (rows_in, rows_out, unknown) in expect.items():
        r = results[table]
        assert (r.rows_in, r.rows_out, r.unknown_members) == (rows_in, rows_out, unknown), table
        assert r.rows_out == r.rows_in + r.unknown_members
        assert read_table(spark, config, table).count() == rows_out


# Natural key(s) behind each surrogate key.
DIM_KEYS = {
    "gold.dim_date": ("date_key", ("date",)),
    "gold.dim_region": ("region_key", ("region_code",)),
    "gold.dim_channel": ("channel_key", ("channel_code",)),
    "gold.dim_campaign": ("campaign_key", ("campaign_id",)),
    "gold.dim_product": ("product_key", ("product_code",)),
    "gold.dim_agent": ("agent_key", ("agent_id", "valid_from")),
}


def _key_rows(spark, config) -> dict[str, list[tuple]]:
    """Per dimension, (natural key..., surrogate key) sorted by natural key."""
    out = {}
    for table, (key, natural) in DIM_KEYS.items():
        rows = read_table(spark, config, table).select(*natural, key).collect()
        out[table] = sorted(tuple(str(v) for v in r) for r in rows)
    return out


@pytest.mark.slow
@pytest.mark.parametrize("table", sorted(DIM_KEYS))
def test_no_surrogate_key_collisions(spark, real_gold, table):
    """One key per natural key and one natural key per key, every key below 2^53."""
    config, _ = real_gold
    key, natural = DIM_KEYS[table]
    d = read_table(spark, config, table)
    r = d.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(key).alias("keys"),
        F.countDistinct(*natural).alias("naturals"),
        F.max(key).alias("hi"),
    ).first()
    assert r.rows == r.keys == r.naturals, table
    assert r.hi < 2**53, table


@pytest.mark.slow
def test_surrogate_keys_identical_when_rebuilt_from_empty_gold(spark, real_gold):
    """Build the dimensions twice from an empty gold layer: same keys, row for row."""
    config, _ = real_gold
    gold_root = Path(table_path(config, "gold.dim_date")).parent
    builds = []
    for n in (1, 2):
        shutil.rmtree(gold_root, ignore_errors=True)
        assert not gold_root.exists()
        build_dimensions(spark, {**config, "run_id": f"test-gold-empty-{n}"})
        builds.append(_key_rows(spark, config))
    # And once more over the existing tables (a plain rerun).
    build_dimensions(spark, {**config, "run_id": "test-gold-rerun"})
    builds.append(_key_rows(spark, config))
    for table in DIM_KEYS:
        assert builds[0][table] == builds[1][table] == builds[2][table], table
        assert len(builds[0][table]) > 0, table


@pytest.mark.slow
def test_dim_agent_scd2_properties(spark, real_gold):
    config, _ = real_gold
    a = read_table(spark, config, "gold.dim_agent").where("agent_id != 'UNKNOWN'")
    rows = sorted(a.collect(), key=lambda r: (r.agent_id, r.valid_from))
    by_agent: dict[str, list] = {}
    for r in rows:
        by_agent.setdefault(r.agent_id, []).append(r)
    assert len(by_agent) == 42
    assert sum(len(v) == 2 for v in by_agent.values()) == 3
    for agent, versions in by_agent.items():
        assert sum(v.is_current for v in versions) == 1, agent  # exactly one current row
        assert versions[-1].is_current and versions[-1].valid_to == date(9999, 12, 31), agent
        for prev, nxt in zip(versions, versions[1:], strict=False):
            # Contiguous and never overlapping: the next window starts the day after.
            assert (nxt.valid_from - prev.valid_to).days == 1, agent
    assert {
        (r.agent_id, r.seniority_band, r.is_current)
        for r in a.where("agent_id IN ('AG0008', 'AG0023', 'AG0037')").collect()
    } == {
        ("AG0008", "Trainee", False), ("AG0008", "Core", True),
        ("AG0023", "Core", False), ("AG0023", "Senior", True),
        ("AG0037", "Trainee", False), ("AG0037", "Core", True),
    }  # fmt: skip
    # Employee personal and HR fields stay out of gold (data minimisation).
    assert not {"agent_name", "agent_email", "manager_name", "hire_date", "fte"} & set(a.columns)


@pytest.mark.slow
def test_every_silver_call_resolves_to_one_agent_version(spark, real_gold):
    config, _ = real_gold
    calls = read_table(spark, config, "silver.call")
    dim = read_table(spark, config, "gold.dim_agent")
    resolved = resolve_agent(calls, dim)
    assert resolved.count() == calls.count() == 62_829  # no call lost or doubled
    assert resolved.where(F.col("agent_key") == UNKNOWN_KEY).count() == 615  # AG9001 to AG9004
    assert resolved.where("agent_key IS NULL").count() == 0
    # Every call is on or after 2026-04-01, so versioned agents resolve to the current row.
    current = dim.where("is_current").select("agent_key")
    assert resolved.join(current, "agent_key", "left_anti").count() == 0


@pytest.mark.slow
def test_future_fact_foreign_keys_resolve(spark, real_gold):
    """Key coverage against silver. Unmatched rows map to UNKNOWN and are counted here."""
    config, _ = real_gold
    t = lambda name: read_table(spark, config, name)  # noqa: E731
    lead, call, policy, spend = (
        t("silver.lead"),
        t("silver.call"),
        t("silver.policy"),
        t("silver.marketing_spend"),
    )
    dates = t("gold.dim_date")
    lead_ids = t("silver.lead_dedup_map").select(F.col("source_lead_id").alias("lead_id"))
    cov = {
        # Leads: orphan campaign ids are already UNKNOWN in silver (304, defect 5).
        "lead.campaign": key_coverage(lead, "campaign_id", t("gold.dim_campaign"), "campaign_id"),
        "lead.channel": key_coverage(lead, "channel_code", t("gold.dim_channel"), "channel_code"),
        "lead.region": key_coverage(lead, "region_code", t("gold.dim_region"), "region_code"),
        "lead.date": key_coverage(
            lead.withColumn("d", F.to_date("created_at_utc")), "d", dates, "date"
        ),  # fmt: skip
        "call.agent": key_coverage(call, "agent_id", t("gold.dim_agent"), "agent_id"),
        # Calls: 1,094 lead ids in no lead file + 45 on leads quarantined at step 7.
        "call.lead": key_coverage(call, "lead_id", lead_ids, "lead_id"),
        "call.date": key_coverage(
            call.withColumn("d", F.to_date("call_start_ts_utc")), "d", dates, "date"
        ),  # fmt: skip
        "policy.product": key_coverage(
            policy, "product_code", t("gold.dim_product"), "product_code"
        ),  # fmt: skip
        "policy.region": key_coverage(policy, "region_code", t("gold.dim_region"), "region_code"),
        # Policies: 404 null leads keep a NULL key (assumption 11); 1 names a lead
        # quarantined at step 7 as unidentifiable, so it maps to UNKNOWN.
        "policy.lead": key_coverage(policy, "lead_id", lead_ids, "lead_id"),
        "policy.date": key_coverage(policy, "application_date", dates, "date"),
        "spend.campaign": key_coverage(
            spend, "campaign_id", t("gold.dim_campaign"), "campaign_id"
        ),  # fmt: skip
        "spend.date": key_coverage(spend, "spend_date", dates, "date"),
    }
    got = {k: (c.rows, c.unmatched, c.null) for k, c in cov.items()}
    assert got == {
        "lead.campaign": (47_201, 0, 0),
        "lead.channel": (47_201, 0, 0),
        "lead.region": (47_201, 0, 0),
        "lead.date": (47_201, 0, 0),
        "call.agent": (62_829, 615, 0),  # AG9001 to AG9004: UNKNOWN agent (defect 17)
        "call.lead": (62_829, 1_139, 0),
        "call.date": (62_829, 0, 0),
        "policy.product": (5_172, 0, 0),
        "policy.region": (5_172, 0, 0),
        "policy.lead": (5_172, 1, 404),
        "policy.date": (5_172, 0, 0),
        "spend.campaign": (2_001, 0, 0),
        "spend.date": (2_001, 0, 0),
    }
