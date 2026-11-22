"""Step 7: silver.lead, silver.lead_dedup_map and silver.consent_current."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.dq import read_dq_exceptions
from src.common.findings import run_finding
from src.common.tables import read_table
from src.silver.leads import (
    implausible_dob,
    null_implausible_dob,
    primary_key_conflicts,
    unidentifiable,
)
from src.silver.resolve_consent import resolve_consent

FINDINGS = Path(__file__).resolve().parents[1] / "src" / "sql" / "findings_leads.sql"


# ------------------------------------------------------------------ unit


def test_primary_key_conflicts_take_every_row_of_the_id(spark):
    df = spark.createDataFrame(
        [("L1", "a", "h1"), ("L1", "b", "h2"), ("L2", "c", "h3")],
        "lead_id string, campaign_id string, _row_hash string",
    )
    assert sorted(r.campaign_id for r in primary_key_conflicts(df).collect()) == ["a", "b"]


def test_unidentifiable_needs_both_keys_missing(spark):
    df = spark.createDataFrame(
        [("L1", None, None), ("L2", " ", ""), ("L3", "e@x", None), ("L4", None, "07")],
        "lead_id string, email string, phone string",
    )
    assert sorted(r.lead_id for r in unidentifiable(df).collect()) == ["L1", "L2"]


def test_implausible_dob_is_age_under_18_or_over_100_at_creation(spark):
    df = spark.createDataFrame(
        [
            ("old", date(1905, 1, 1)),
            ("child", date(2020, 3, 1)),
            ("future", date(2027, 1, 1)),
            ("eighteen", date(2008, 3, 1)),
            ("hundred", date(1926, 3, 1)),
            ("none", None),
        ],
        "lead_id string, date_of_birth date",
    ).withColumn("created_at_utc", F.lit(datetime(2026, 4, 1)))
    assert sorted(r.lead_id for r in implausible_dob(df).collect()) == ["child", "future", "old"]


def test_minor_dob_is_nulled_and_the_lead_kept(spark):
    created = datetime(2026, 4, 1, 12, 0)
    df = spark.createDataFrame(
        [
            ("minor_17", date(2008, 6, 1), created),  # turns 18 two months after creation
            ("adult_18_today", date(2008, 4, 1), created),  # 18th birthday on creation day
            ("adult_40", date(1986, 1, 15), created),
        ],
        "lead_id string, date_of_birth date, created_at_utc timestamp",
    )
    out = {r.lead_id: r.date_of_birth for r in null_implausible_dob(df).collect()}
    assert set(out) == {"minor_17", "adult_18_today", "adult_40"}  # no lead dropped
    assert out["minor_17"] is None  # a minor's DOB is nulled, not the lead
    assert out["adult_18_today"] == date(2008, 4, 1)
    assert out["adult_40"] == date(1986, 1, 15)


def test_resolve_consent_latest_event_and_phone_flag(spark):
    events = spark.createDataFrame(
        [
            ("E1", "L1", datetime(2026, 4, 1), "GRANTED", ["EMAIL", "PHONE"]),
            ("E2", "L1", datetime(2026, 4, 5), "WITHDRAWN", ["EMAIL"]),  # email only
            ("E3", "L2", datetime(2026, 4, 1), "GRANTED", ["PHONE"]),
            ("E4", "L2", datetime(2026, 4, 2), "WITHDRAWN", ["PHONE"]),
            ("E5", "L2", datetime(2026, 4, 3), "RE_GRANTED", ["PHONE"]),
            ("E6", "L3", datetime(2026, 4, 1), "GRANTED", ["EMAIL"]),
        ],
        "event_id string, lead_id string, event_ts_utc timestamp, event_type string, "
        "channels array<string>",
    )
    rows = {r.lead_id: r for r in resolve_consent(events).collect()}
    assert (rows["L1"].current_event_type, rows["L1"].consent_marketing_current) == (
        "WITHDRAWN",
        False,
    )
    assert rows["L1"].consent_phone_current_flag is True  # the withdrawal was email only
    assert (rows["L2"].consent_marketing_current, rows["L2"].consent_phone_current_flag) == (
        True,
        True,
    )
    assert rows["L3"].consent_phone_current_flag is None  # never asked about phone
    assert {r.lead_id: r.event_count for r in rows.values()} == {"L1": 2, "L2": 3, "L3": 1}


# ------------------------------------------------------------------ real data


def _checks(spark, config, name):
    return read_dq_exceptions(spark, config).where(F.col("check_name") == name)


@pytest.mark.slow
def test_lead_ids_reconcile(spark, real_leads):
    config, rec, _ = real_leads
    # Distinct lead_id: 47,997 = 47,201 surviving + 758 mapped duplicates
    #                            + 35 unidentifiable + 3 primary-key conflicts.
    assert (rec.distinct_ids_in, rec.survivors, rec.mapped_duplicates) == (47_997, 47_201, 758)
    assert (rec.unidentifiable_ids, rec.pk_conflict_ids) == (35, 3)
    assert rec.distinct_ids_in == (
        rec.survivors + rec.mapped_duplicates + rec.unidentifiable_ids + rec.pk_conflict_ids
    )
    # Rows: 48,000 = 47,959 map rows + 41 quarantined (35 unidentifiable + 6 PK rows,
    # because a primary-key conflict quarantines both rows of each of the 3 ids).
    assert (rec.rows_in, rec.map_rows, rec.rows_quarantined) == (48_000, 47_959, 41)
    assert rec.rows_in == rec.map_rows + rec.rows_quarantined

    assert read_table(spark, config, "silver.lead").count() == 47_201
    dedup_map = read_table(spark, config, "silver.lead_dedup_map")
    assert dedup_map.count() == 47_959
    assert dedup_map.where("source_lead_id != surviving_lead_id").count() == 758
    assert set(dedup_map.columns) >= {
        "source_lead_id", "surviving_lead_id", "match_rule", "cluster_size"
    }  # fmt: skip


@pytest.mark.slow
def test_quarantine_and_flag_counts(spark, real_leads):
    config, _, _ = real_leads
    pk = _checks(spark, config, "lead_pk_conflict").collect()
    assert len(pk) == 6 and len({r.key_value for r in pk}) == 3
    assert {(r.severity, r.action, r.defect_id) for r in pk} == {("error", "quarantined", 2)}

    unid = _checks(spark, config, "lead_unidentifiable").collect()
    assert len(unid) == 35
    assert {(r.severity, r.action, r.defect_id) for r in unid} == {("error", "quarantined", 4)}

    dups = _checks(spark, config, "lead_near_duplicate")
    assert dups.count() == 758 and dups.where("action != 'flagged'").count() == 0

    assert _checks(spark, config, "lead_dob_implausible").count() == 60
    assert _checks(spark, config, "lead_outside_period").count() == 10
    assert _checks(spark, config, "lead_campaign_channel_mismatch").count() == 600


@pytest.mark.slow
def test_silver_lead_content(spark, real_leads):
    config, _, _ = real_leads
    leads = read_table(spark, config, "silver.lead")
    assert leads.groupBy("lead_id").count().where("count > 1").count() == 0
    # Defect 6: the 10 July leads stay in silver, flagged, never quarantined. All 10 are
    # near-duplicates of late-June leads (within 48h), so they sit in lead_dedup_map
    # mapped to June survivors; none survives into silver.lead.
    assert leads.where(F.col("created_at_utc") >= "2026-07-01").count() == 0
    july = run_finding(spark, FINDINGS, "july_leads")[0]
    assert (july.july_leads, july.in_dedup_map, july.mapped_to_june) == (10, 10, 10)
    # Defect 7: no implausible DOB survives; the leads themselves do.
    assert implausible_dob(leads).count() == 0
    # Defect 5: orphan campaign ids are UNKNOWN on the lead.
    assert (
        leads.where("campaign_id = 'UNKNOWN'").count()
        == leads.where("campaign_id_raw IN ('CMP-9999', 'CMP-1099')").count()
    )
    # Defect 24: channel_code is the campaign's channel wherever the campaign is known.
    campaigns = read_table(spark, config, "silver.campaign").select(
        "campaign_id", F.col("channel_code").alias("campaign_channel")
    )
    joined = leads.join(campaigns, "campaign_id")
    assert joined.where(F.col("channel_code") != F.col("campaign_channel")).count() == 0


@pytest.mark.slow
def test_consent_current(spark, real_leads):
    config, _, consent = real_leads
    assert (consent.rows_in, consent.rows_out, consent.rows_quarantined) == (2_900, 2_900, 0)
    current = read_table(spark, config, "silver.consent_current")
    assert current.count() == 2_900
    assert current.where("current_event_type = 'WITHDRAWN'").count() == 299
    withdrawn = _checks(spark, config, "consent_latest_withdrawn").collect()
    still_y = _checks(spark, config, "consent_withdrawn_marketing_y").collect()
    assert len(withdrawn) == 299 and len(still_y) == 284
    assert {(r.action, r.defect_id) for r in withdrawn + still_y} == {("flagged", 25)}


# ------------------------------------------------------------------ evidence (findings SQL)


@pytest.mark.slow
def test_finding_lead_id_reconciliation(spark, real_leads):
    r = run_finding(spark, FINDINGS, "lead_id_reconciliation")[0]
    assert (r.distinct_ids, r.pk_conflict_ids, r.unidentifiable_ids) == (47_997, 3, 35)
    assert (r.surviving, r.mapped_duplicates) == (47_201, 758)


@pytest.mark.slow
def test_finding_source_flag_vs_rule(spark, real_leads):
    r = run_finding(spark, FINDINGS, "source_flag_vs_rule")[0]
    assert r.source_flagged == 480
    assert r.flag_matches_rule == 478  # same survivor
    assert r.flag_reversed == 1  # the "duplicate" was created first, so it survives
    assert r.flag_no_shared_key == 1  # email-only vs phone-only: the rule cannot link them
    assert r.rule_only == 758 - 478 - 1  # duplicates the source never flagged


@pytest.mark.slow
def test_finding_dob_groups(spark, real_leads):
    rows = {r.dob_group: r.leads for r in run_finding(spark, FINDINGS, "dob_implausible_groups")}
    assert rows == {"age_over_100": 20, "age_under_18": 20, "born_after_creation": 20}
    r = run_finding(spark, FINDINGS, "born_2008_ages")[0]
    assert (r.leads, r.min_age, r.max_age) == (252, 18, 18)


@pytest.mark.slow
def test_finding_dob_register_bands(spark, real_leads):
    # The register's ~312 (20 before 1920, 292 after 2008) against the rule's 60 nulled.
    rows = {r.band: (r.leads, r.nulled) for r in run_finding(spark, FINDINGS, "dob_register_bands")}
    assert rows == {
        "1_before_1920": (20, 20),
        "2_2008_to_today": (272, 20),  # 252 born 2008, aged 18: kept; 20 born 2020: nulled
        "3_future": (20, 20),
    }
    assert sum(n for _, n in rows.values()) == 60


@pytest.mark.slow
def test_finding_withdrawn_consent(spark, real_leads):
    r = run_finding(spark, FINDINGS, "withdrawn_consent_still_y")[0]
    assert (r.latest_withdrawn, r.lead_still_y) == (299, 284)
