"""Step 6: silver reference data and dimensions (channel, region, campaign, FX, agent)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.dq import assert_reconciled, quarantined_count, read_dq_exceptions
from src.common.findings import run_finding
from src.common.tables import read_table
from src.silver.reference import (
    UNKNOWN_CAMPAIGN_ID,
    build_reference,
    conform_agents,
    conform_campaign_id,
    conform_campaigns,
    missing_fx_rates,
    normalise_channel,
)

CHANNELS = [
    "PAID_SEARCH", "PAID_SOCIAL", "DISPLAY", "AFFILIATE",
    "AGGREGATOR", "DIRECT_MAIL", "TV", "ORGANIC",
]  # fmt: skip
CAMPAIGN_COLS = (
    "campaign_id string, campaign_name string, channel_code string, region_code string, "
    "product_code string, objective string, start_date string, end_date string, "
    "landing_page_url string"
)
AGENT_COLS = (
    "agent_id string, agent_name string, agent_email string, region_code string, team string, "
    "seniority_band string, hire_date string, fte string, active_flag string, "
    "manager_name string, _row_hash string"
)


# ------------------------------------------------------------------ unit: channel labels


def test_every_campaign_label_variant_maps_to_a_ref_channel(spark):
    variants = [
        "AFFILIATE", "AGGREGATOR", "Affiliate", "Aggregator", "DIRECT_MAIL", "DISPLAY",
        "DISPLAY ", "ORGANIC", "PAID_SEARCH", "PAID_SOCIAL", "PaidSearch", "PaidSocial",
        "SOCIAL_PAID", "TV", "aggregator", "display", "paid_search",
    ]  # fmt: skip
    df = spark.createDataFrame([(v,) for v in variants], "raw string")
    out = {
        r.raw: r.code
        for r in df.select("raw", normalise_channel(F.col("raw"), CHANNELS).alias("code")).collect()
    }
    assert set(out.values()) == set(CHANNELS)
    assert out["SOCIAL_PAID"] == "PAID_SOCIAL"
    assert out["DISPLAY "] == "DISPLAY"
    assert out["PaidSearch"] == "PAID_SEARCH"


def test_unknown_channel_label_is_null_not_guessed(spark):
    df = spark.createDataFrame([("RADIO",), (None,)], "raw string")
    assert [
        r.c for r in df.select(normalise_channel(F.col("raw"), CHANNELS).alias("c")).collect()
    ] == [None, None]


# ------------------------------------------------------------------ unit: campaigns


def test_conform_campaigns_normalises_and_flags_bad_dates(spark):
    raw = spark.createDataFrame(
        [
            ("C1", "n1", "PaidSearch", "UK", "CI", "Acq", "2026-04-01", "2026-06-30", "u"),
            ("C2", "n2", "TV", "US", "CI", "Acq", "2026-04-01", "2026-03-22", "u"),
        ],
        CAMPAIGN_COLS,
    )
    campaigns, label_fixes, bad_dates = conform_campaigns(raw, CHANNELS)
    rows = {r.campaign_id: r for r in campaigns.collect()}
    assert rows["C1"].channel_code == "PAID_SEARCH"
    assert rows["C1"].channel_code_raw == "PaidSearch"
    assert rows["C1"].start_date == date(2026, 4, 1)
    assert [r.campaign_id for r in label_fixes.collect()] == ["C1"]
    assert [r.campaign_id for r in bad_dates.collect()] == ["C2"]
    assert rows["C2"].end_before_start is True  # flagged, kept


def test_orphan_campaign_ids_map_to_unknown(spark):
    campaigns = spark.createDataFrame([("CMP-1041",)], "campaign_id string")
    leads = spark.createDataFrame(
        [("L1", "CMP-1041"), ("L2", "CMP-9999"), ("L3", "CMP-1099"), ("L4", None)],
        "lead_id string, campaign_id string",
    )
    out = {r.lead_id: r for r in conform_campaign_id(leads, campaigns).collect()}
    assert out["L1"].campaign_id == "CMP-1041"
    assert out["L2"].campaign_id == UNKNOWN_CAMPAIGN_ID
    assert out["L3"].campaign_id == UNKNOWN_CAMPAIGN_ID
    assert out["L4"].campaign_id == UNKNOWN_CAMPAIGN_ID
    assert out["L2"].campaign_id_raw == "CMP-9999"  # raw value kept for lineage
    assert out["L2"].is_orphan_campaign and not out["L1"].is_orphan_campaign


# ------------------------------------------------------------------ unit: FX


def test_missing_fx_rate_is_detected_not_nulled(spark):
    fx = spark.createDataFrame(
        [("2026-04", "CAD", "0.578"), ("2026-05", "CAD", "0.58"), ("2026-04", "GBP", "1.0")],
        "rate_month string, currency_code string, rate_to_gbp string",
    )
    missing = missing_fx_rates(fx, ["CAD", "GBP"], ["2026-04", "2026-05"])
    assert sorted(r.fx_key for r in missing.collect()) == ["2026-05|GBP"]


# ------------------------------------------------------------------ unit: agents


def test_conform_agents_keeps_versions_and_fixes_region_and_team(spark):
    raw = spark.createDataFrame(
        [
            ("A1", "n", "e", "CA", "CA-Team-Beta", "Trainee", "2020-07-09", "0.80", "1", "m", "h1"),
            ("A1", "n", "e", "CA", "UK-Team-Alpha", "Core", "2020-07-09", "0.80", "1", "m", "h2"),
            ("A2", "n", "e", None, "UK-Team-Beta", "Core", "2018-09-22", "1.00", "0", "m", "h3"),
            ("A3", "n", "e", "US", "US-Team-Alpha", "Senior", "2018-07-15", "1.00", "1", "m", "h4"),
        ],
        AGENT_COLS,
    )
    agents, region_fixes, team_fixes, versions = conform_agents(raw, ["UK", "CA", "AU", "US"])
    rows = agents.collect()
    assert len(rows) == 4  # both versions of A1 kept

    a1 = sorted((r.seniority_band, r.team, r.team_raw) for r in rows if r.agent_id == "A1")
    assert a1 == [
        ("Core", "CA-Team-Beta", "UK-Team-Alpha"),
        ("Trainee", "CA-Team-Beta", "CA-Team-Beta"),
    ]
    a2 = next(r for r in rows if r.agent_id == "A2")
    assert (a2.region_code, a2.region_code_raw) == ("UK", None)
    assert a2.active_flag is False and float(a2.fte) == 1.0

    assert [r.agent_id for r in region_fixes.collect()] == ["A2"]
    assert [r.agent_id for r in team_fixes.collect()] == ["A1"]
    assert sorted(r.agent_id for r in versions.collect()) == ["A1", "A1"]


# ------------------------------------------------------------------ real data


@pytest.fixture(scope="module")
def real_reference(spark, real_bronze):
    # Own run id, so other modules building silver on the same bronze never mix exceptions.
    config = {**real_bronze.config, "run_id": "test-silver-reference"}
    results = {r.table: r for r in build_reference(spark, config)}
    return config, results


def _reconciles(spark, config, result):
    out = read_table(spark, config, result.table).count()
    q = quarantined_count(spark, config, result.source_table)
    assert (result.rows_out, result.rows_quarantined) == (out, q)
    assert_reconciled(result.table, result.rows_in, out, q)


@pytest.mark.slow
@pytest.mark.parametrize(
    ("table", "rows_in", "rows_out"),
    [
        ("silver.ref_channel", 8, 8),
        ("silver.ref_region", 4, 4),
        ("silver.campaign", 24, 24),
        ("silver.fx_rate", 11, 11),
        ("silver.agent", 45, 45),
    ],
)
def test_real_tables_reconcile(spark, real_reference, table, rows_in, rows_out):
    config, results = real_reference
    result = results[table]
    assert (result.rows_in, result.rows_out) == (rows_in, rows_out)
    _reconciles(spark, config, result)


def _exceptions(spark, config, check):
    return read_dq_exceptions(spark, config).where(F.col("check_name") == check)


@pytest.mark.slow
def test_real_campaign_channels_all_conform(spark, real_reference):
    config, _ = real_reference
    campaigns = read_table(spark, config, "silver.campaign")
    channels = {r.channel_code for r in read_table(spark, config, "silver.ref_channel").collect()}
    assert {r.channel_code for r in campaigns.collect()} <= channels
    assert campaigns.where("end_before_start").count() == 2
    assert _exceptions(spark, config, "campaign_end_before_start").count() == 2


@pytest.mark.slow
def test_real_orphan_campaigns_flagged(spark, real_reference):
    config, _ = real_reference
    rows = _exceptions(spark, config, "campaign_orphan_id").collect()
    assert len(rows) == 310 + 3
    assert {r.detail for r in rows} == {"CMP-9999", "CMP-1099"}
    assert {(r.action, r.defect_id, r.source_table) for r in rows} == {
        ("flagged", 5, "bronze.leads_raw")
    }


@pytest.mark.slow
def test_real_missing_cad_june_is_an_error_not_a_null(spark, real_reference):
    config, _ = real_reference
    rows = _exceptions(spark, config, "fx_rate_missing").collect()
    assert [(r.key_value, r.severity, r.defect_id) for r in rows] == [("2026-06|CAD", "error", 12)]
    fx = read_table(spark, config, "silver.fx_rate")
    assert fx.where(F.col("rate_to_gbp").isNull()).count() == 0
    assert fx.where("rate_month = '2026-06' AND currency_code = 'CAD'").count() == 0


@pytest.mark.slow
def test_real_agents_keep_both_versions_and_have_regions(spark, real_reference):
    config, _ = real_reference
    agents = read_table(spark, config, "silver.agent")
    versions = {
        r.agent_id: r["count"]
        for r in agents.groupBy("agent_id").count().where("count > 1").collect()
    }
    assert versions == {"AG0008": 2, "AG0023": 2, "AG0037": 2}
    assert agents.where(F.col("region_code").isNull()).count() == 0
    assert agents.where(F.substring("team", 1, 2) != F.col("region_code")).count() == 0
    assert _exceptions(spark, config, "agent_null_region").count() == 2
    team_fixes = _exceptions(spark, config, "agent_team_region_mismatch").collect()
    assert {r.key_value for r in team_fixes} == {"AG0008", "AG0037"}
    assert _exceptions(spark, config, "agent_duplicate_version").count() == 6


# ------------------------------------------------------------------ evidence (findings SQL)

FINDINGS = Path(__file__).resolve().parents[1] / "src" / "sql" / "findings_reference.sql"


@pytest.mark.slow
def test_evidence_null_region_agents_call_only_in_uk(spark, real_reference):
    rows = run_finding(spark, FINDINGS, "null_region_agent_call_regions")
    assert sorted((r.agent_id, r.call_region, r.calls) for r in rows) == [
        ("AG0007", "UK", 1302),
        ("AG0035", "UK", 1206),
    ]


@pytest.mark.slow
def test_evidence_team_prefix_matches_region(spark, real_reference):
    row = run_finding(spark, FINDINGS, "team_prefix_region_agreement")[0]
    assert (row.matching, row.with_region) == (41, 43)


@pytest.mark.slow
def test_evidence_lead_vs_campaign_channel(spark, real_reference):
    row = run_finding(spark, FINDINGS, "lead_campaign_channel_disagreement")[0]
    assert row.leads_with_known_campaign == 47_687
    assert row.disagree_raw == 23_644
    assert row.disagree_normalised == 600
