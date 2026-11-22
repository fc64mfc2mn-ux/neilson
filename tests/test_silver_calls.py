"""Step 8: silver.call (defects 13 to 17 and 26)."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.dq import CHECK_DEFECTS, ReconciliationError, read_dq_exceptions
from src.common.findings import run_finding
from src.common.tables import read_table
from src.silver.calls import (
    build_calls,
    calls_after_do_not_call,
    classify_quarantine,
    exact_duplicate_copies,
    lead_match,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FINDINGS = REPO_ROOT / "src" / "sql" / "findings_calls.sql"
T0 = datetime(2026, 5, 10, 9, 0)


# ------------------------------------------------------------------ unit


def test_exact_duplicate_copies_drop_all_but_the_first_copy(spark):
    raw = spark.createDataFrame(
        [("C1", "f", "h1", 1), ("C1", "f", "h1", 7), ("C2", "f", "h2", 2), ("C1", "g", "h9", 1)],
        "call_id string, _source_file string, _row_hash string, _source_row long",
    )
    removed = exact_duplicate_copies(raw).collect()
    assert [(r.call_id, r._source_file, r._source_row) for r in removed] == [("C1", "f", 7)]


def test_conflicting_duplicate_call_id_has_no_rule_and_fails(spark):
    raw = spark.createDataFrame(
        [("C1", "f", "h1", 1), ("C1", "f", "h2", 2)],
        "call_id string, _source_file string, _row_hash string, _source_row long",
    )
    with pytest.raises(ReconciliationError, match="different content"):
        exact_duplicate_copies(raw).count()


def test_quarantine_rules_and_their_precedence(spark):
    # Each row states which rule removes it. A row breaking two rules is removed by
    # the first in order: unparseable timestamp, end before start, start before lead.
    h = lambda n: T0 + timedelta(hours=n)  # noqa: E731
    calls = spark.createDataFrame(
        [
            ("ok", "L1", h(1), h(2)),
            ("no_ts", "L1", None, h(2)),
            ("end_first", "L1", h(2), h(1)),
            ("end_first_and_before_lead", "L1", h(-2), h(-3)),
            ("before_lead", "L1", h(-1), h(0)),
            ("at_creation", "L1", h(0), h(1)),
            ("ghost_lead", "LX", h(-5), h(-4)),
        ],
        "call_id string, lead_id string, call_start_ts_utc timestamp, call_end_ts_utc timestamp",
    )
    leads = spark.createDataFrame([("L1", T0)], "lead_id string, lead_created_at_utc timestamp")
    rules = {r.call_id: r.quarantine_rule for r in classify_quarantine(calls, leads).collect()}
    assert rules == {
        "ok": None,
        "no_ts": "call_timestamp_unparseable",
        "end_first": "call_end_before_start",
        "end_first_and_before_lead": "call_end_before_start",
        "before_lead": "call_start_before_lead",
        "at_creation": None,
        "ghost_lead": None,  # no lead to compare with: flagged as not found, not quarantined
    }


def test_lead_match_found_quarantined_or_not_found(spark):
    calls = spark.createDataFrame([("C1", "L1"), ("C2", "L2"), ("C3", "LD-GHOST-UK-1")],
                                  "call_id string, lead_id string")  # fmt: skip
    leads = spark.createDataFrame([("L1", T0), ("L2", T0)],
                                  "lead_id string, lead_created_at_utc timestamp")  # fmt: skip
    dedup_map = spark.createDataFrame(
        [("L1", "L1")], "source_lead_id string, surviving_lead_id string"
    )
    out = {r.call_id: r.lead_match for r in lead_match(calls, leads, dedup_map).collect()}
    assert out == {"C1": "found", "C2": "lead_quarantined", "C3": "not_found"}


def test_calls_after_do_not_call_are_flagged_not_the_dnc_itself(spark):
    h = lambda n: T0 + timedelta(hours=n)  # noqa: E731
    calls = spark.createDataFrame(
        [
            ("a1", "L1", h(1), "NO_ANSWER"),
            ("a2", "L1", h(2), "DO_NOT_CALL"),
            ("a3", "L1", h(3), "CALLBACK"),
            ("a4", "L1", h(4), "DO_NOT_CALL"),
            ("b1", "L2", h(1), "CONNECTED"),
        ],
        "call_id string, lead_id string, call_start_ts_utc timestamp, disposition string",
    )
    out = calls_after_do_not_call(calls).collect()
    assert sorted(r.call_id for r in out) == ["a3", "a4"]
    assert {r.first_do_not_call_ts_utc for r in out} == {h(2)}


def test_new_checks_are_mapped_to_registered_defects():
    assert {
        "call_exact_duplicate": 13,
        "call_timestamp_unparseable": 14,
        "call_au_residual_offset": 15,
        "call_end_before_start": 16,
        "call_start_before_lead": 16,
        "call_lead_not_found": 17,
        "call_lead_quarantined": 17,
        "call_agent_not_found": 17,
        "call_after_do_not_call": 26,
    }.items() <= CHECK_DEFECTS.items()


def test_config_converts_au_from_sydney_and_keeps_the_rest_utc():
    zones = load_config(str(REPO_ROOT / "config" / "config.yaml"))["calls"]["source_timezone"]
    assert zones == {
        "UK": "UTC",
        "AU": "Australia/Sydney",
        "CA": "UTC",
        "US": "UTC",
    }, "Call timezones changed: update assumption 5 and the AU gap test together"


# ------------------------------------------------------------------ real data


@pytest.fixture(scope="module")
def real_calls(spark, real_leads):
    config, _, _ = real_leads
    return config, build_calls(spark, config)


def _checks(spark, config, name):
    return read_dq_exceptions(spark, config).where(F.col("check_name") == name)


@pytest.mark.slow
def test_call_rows_reconcile_rule_by_rule(spark, real_calls):
    config, rec = real_calls
    # Rows: 64,000 = 41,000 UK/AU + 23,000 CA/US.
    assert rec.rows_in_by_file == {"calls_uk_au.csv": 41_000, "calls_ca_us.csv": 23_000}
    # Defect 13: 540 second copies of byte-identical rows (346 UK/AU + 194 CA/US).
    assert rec.removed_by_file["call_exact_duplicate"] == {
        "calls_uk_au.csv": 346, "calls_ca_us.csv": 194
    }  # fmt: skip
    # Defect 16: 380 end before start (256 + 124; the register's 262 is UK/AU raw rows,
    # duplicates included), then 251 start before their lead: 245 as stored, plus 6 AU
    # calls that move before their lead once AU is converted from Sydney time.
    assert rec.removed_by_file["call_end_before_start"] == {
        "calls_uk_au.csv": 256, "calls_ca_us.csv": 124
    }  # fmt: skip
    assert rec.removed_by_file["call_start_before_lead"] == {
        "calls_uk_au.csv": 182, "calls_ca_us.csv": 69
    }  # fmt: skip
    assert rec.removed_by_file.get("call_timestamp_unparseable", {}) == {}
    # 64,000 = 62,829 + 540 + 380 + 251.
    assert (rec.rows_in, rec.rows_out, rec.rows_quarantined) == (64_000, 62_829, 1_171)
    assert rec.rows_in == rec.rows_out + rec.rows_quarantined
    # By call id: 63,460 distinct = 62,829 out + 380 + 251 (duplicates share an id).
    assert rec.distinct_ids_in == 63_460 == rec.rows_out + 380 + 251
    assert read_table(spark, config, "silver.call").count() == 62_829


@pytest.mark.slow
def test_call_quarantine_and_flag_counts(spark, real_calls):
    config, _ = real_calls
    expect = {
        # check: (rows, severity, action)
        "call_exact_duplicate": (540, "info", "quarantined"),
        "call_end_before_start": (380, "error", "quarantined"),
        "call_start_before_lead": (251, "warning", "quarantined"),
        # Flags are raised on rows kept in silver: 1,100 ghost-lead calls less 6 that end
        # before they start; 620 ghost-agent calls less 4 + 1 quarantined.
        "call_lead_not_found": (1_094, "info", "flagged"),
        "call_agent_not_found": (615, "info", "flagged"),
        "call_au_residual_offset": (14_762, "warning", "flagged"),
        "call_lead_quarantined": (45, "info", "flagged"),
        "call_after_do_not_call": (6_311, "warning", "flagged"),
    }
    for check, (rows, severity, action) in expect.items():
        got = _checks(spark, config, check)
        assert got.count() == rows, check
        assert {(r.severity, r.action) for r in got.select("severity", "action").distinct()
                .collect()} == {(severity, action)}, check  # fmt: skip
    agents = _checks(spark, config, "call_agent_not_found")
    assert {r.detail for r in agents.select("detail").distinct().collect()} == {
        "AG9001", "AG9002", "AG9003", "AG9004"
    }  # fmt: skip


@pytest.mark.slow
def test_silver_call_content(spark, real_calls):
    config, _ = real_calls
    calls = read_table(spark, config, "silver.call").cache()
    assert calls.groupBy("call_id").count().where("count > 1").count() == 0
    assert calls.where("call_end_ts_utc < call_start_ts_utc").count() == 0
    assert calls.where("call_start_ts_utc IS NULL OR duration_seconds < 0").count() == 0
    assert dict(calls.groupBy("region_code").count().collect()) == {
        "UK": 25_454, "AU": 14_762, "CA": 12_794, "US": 9_819
    }  # fmt: skip
    # CA/US keep their own SLA and wrap-up values.
    assert calls.where("region_code = 'CA' AND answered_within_sla_flag IS NOT NULL").count() > 0
    assert calls.where("region_code = 'US' AND wrap_up_code IS NULL").count() == 0
    # Flags are columns too, so gold can use them without reading dq_exception.
    assert calls.where("after_do_not_call").count() == 6_311
    assert calls.where("residual_offset_flag").count() == 14_762
    assert calls.where("residual_offset_flag AND region_code != 'AU'").count() == 0
    calls.unpersist()


@pytest.mark.slow
def test_timezone_hypotheses_on_bronze(spark, real_calls):
    """Assumption 5 evidence, on bronze times as stored."""
    gap = {r.region_code: r for r in run_finding(spark, FINDINGS, "first_call_gap_by_hypothesis")}
    # UK, CA and US behave as UTC: first call 22 to 45 minutes after the lead.
    assert round(gap["UK"].median_min_as_utc) == 22
    assert round(gap["CA"].median_min_as_utc) == 45 and round(gap["US"].median_min_as_utc) == 40
    # As local time they would be 38 minutes early (UK) or 4.7 hours late (CA, US).
    assert round(gap["UK"].median_min_as_local) == -38
    assert round(gap["CA"].median_min_as_local) == 285
    # AU: 18.5 hours as stored, 8.5 hours as Sydney local time.
    assert round(gap["AU"].median_min_as_utc) == 1_111
    assert round(gap["AU"].median_min_as_local) == 509
    # Which side is shifted: application date = linked call date for every UK, CA and US
    # policy as stored; for AU only after moving the call back 10 h (not 9, not 11).
    fit = {
        (r.region_code, r.offset_hours): (r.matching, r.policies)
        for r in run_finding(spark, FINDINGS, "policy_date_vs_call_date_by_offset")
    }
    for region, n in (("UK", 1_716), ("CA", 1_058), ("US", 810)):
        assert fit[(region, 0)] == (n, n)
    assert fit[("AU", 0)] == (248, 426)
    assert fit[("AU", 10)] == (426, 426)
    assert fit[("AU", 9)][0] < 426 and fit[("AU", 11)][0] < 426
    # AU lead ingestion lag equals every other region's: the leads are not shifted.
    lead_side = {r.region_code: r for r in run_finding(spark, FINDINGS, "lead_internal_timing")}
    assert {round(r.median_ingest_lag_min, 1) for r in lead_side.values()} <= {2.6, 2.7}


@pytest.mark.slow
def test_policy_date_equals_call_date_in_every_region(spark, real_calls):
    """Permanent check (assumption 5): with each region's configured zone, a policy's
    application date equals its linked call's date."""
    rows = {r.region_code: r for r in run_finding(spark, FINDINGS, "policy_date_vs_call_date")}
    # Distinct calls before quarantine, converted as configured: AU 426 of 426.
    assert {k: (r.matching, r.policies) for k, r in rows.items()} == {
        "UK": (1_716, 1_716), "AU": (426, 426), "CA": (1_058, 1_058), "US": (810, 810)
    }  # fmt: skip
    # On silver.call (4 AU, 21 UK, 4 CA and 6 US linked calls are quarantined).
    assert {k: (r.matching_silver, r.policies_silver) for k, r in rows.items()} == {
        "UK": (1_695, 1_695), "AU": (422, 422), "CA": (1_054, 1_054), "US": (804, 804)
    }  # fmt: skip


@pytest.mark.slow
def test_bronze_vs_silver_shows_the_au_shift_only(spark, real_calls):
    rows = {
        (r.region_code, r.shift_hours): r
        for r in run_finding(spark, FINDINGS, "call_shift_bronze_vs_silver")
    }
    # AU moves back 11 h before Sydney DST ended (5 April 2026, 03:00 local), 10 h after.
    assert {k: r.calls for k, r in rows.items() if k[0] == "AU"} == {
        ("AU", 11): 351, ("AU", 10): 14_411
    }  # fmt: skip
    assert str(rows[("AU", 11)].last_stored_start) < "2026-04-05 03:00"
    assert str(rows[("AU", 10)].first_stored_start) >= "2026-04-05 02:00"
    # UK, CA and US are unchanged.
    assert {k for k in rows if k[0] != "AU"} == {("UK", 0), ("CA", 0), ("US", 0)}


@pytest.mark.slow
def test_au_residual_first_call_gap_stays_open_and_flagged(spark, real_calls):
    """After conversion, AU's first call still comes about 8.5 h after the lead, against
    22 to 45 minutes elsewhere. An open observation: flagged, not corrected."""
    config, _ = real_calls
    gap = {r.region_code: r for r in run_finding(spark, FINDINGS, "first_call_gap_in_silver")}
    assert round(gap["AU"].median_min) == 512
    assert all(gap[x].median_min < 60 for x in ("UK", "CA", "US"))
    flagged = _checks(spark, config, "call_au_residual_offset")
    assert flagged.count() == 14_762 == gap["AU"].calls


@pytest.mark.slow
def test_finding_register_vs_distinct(spark, real_calls):
    r = run_finding(spark, FINDINGS, "register_vs_distinct")[0]
    assert (r.end_before_start_raw_uk_au, r.end_before_start_raw, r.end_before_start) == (
        262, 388, 380
    )  # fmt: skip
    assert (r.before_lead_raw, r.before_lead) == (248, 245)  # as stored
    assert r.before_lead_converted == 251  # AU from Sydney time: what silver quarantines
    assert (r.ghost_agent_raw, r.ghost_agent) == (624, 620)
    assert (r.ghost_lead_ids, r.ghost_lead_raw, r.ghost_lead) == (1_099, 1_111, 1_100)


@pytest.mark.slow
def test_finding_ghost_leads(spark, real_calls):
    """Defect 17 decision (assumption 20): ghost-lead calls are kept, UNKNOWN lead."""
    r = run_finding(spark, FINDINGS, "ghost_lead_evidence")[0]
    # 1,125 call lead ids have no lead in silver: 26 are leads quarantined at step 7 as
    # unidentifiable; none is a PK conflict or outside the period. The other 1,099 are
    # in no lead file at all and every one follows the pattern LD-GHOST-<region>-<n>.
    assert (r.unmatched_ids, r.match_unidentifiable) == (1_125, 26)
    assert (r.match_pk_conflict, r.match_outside_period) == (0, 0)
    assert r.ghost_ids == r.ghost_pattern_ids == 1_099
    assert r.prefix_matches_call_region == r.ghost_calls == 1_100
    assert r.linked_policies == 0  # no policy hangs off a ghost lead or its calls


@pytest.mark.slow
def test_finding_start_before_lead_gap(spark, real_calls):
    r = run_finding(spark, FINDINGS, "start_before_lead_gap")[0]
    # 251 calls start between 2 and 24 hours before their lead (AU converted).
    assert r.calls == 251
    assert round(r.min_minutes) == 120 and round(r.max_minutes) == 1_434
    assert 1_410 < r.median_minutes < 1_416


@pytest.mark.slow
def test_finding_calls_after_do_not_call(spark, real_calls):
    rows = {r.region_code: r for r in run_finding(spark, FINDINGS, "calls_after_do_not_call")}
    uk_au = [rows["UK"], rows["AU"]]
    # The register's 4,461 calls on 2,897 leads: UK and AU, distinct calls.
    assert sum(r.calls_distinct for r in uk_au) == 4_461
    assert sum(r.leads_distinct for r in uk_au) == 2_897
    # Not in the register: the same pattern in CA and US.
    assert sum(rows[x].calls_distinct for x in ("CA", "US")) == 1_966
    assert sum(rows[x].leads_distinct for x in ("CA", "US")) == 1_389
    assert sum(r.calls_silver for r in rows.values()) == 6_311


@pytest.mark.slow
def test_finding_do_not_call_reconciliation(spark, real_calls):
    """6,427 distinct calls before quarantine vs 6,311 flagged in silver."""
    r = run_finding(spark, FINDINGS, "do_not_call_reconciliation")[0]
    assert (r.flagged_distinct, r.flagged_silver) == (6_427, 6_311)
    # 116 = 38 flagged calls quarantined themselves (all end before start)
    #     + 63 kept calls whose lead has no DO_NOT_CALL call left in silver
    #     + 15 kept calls whose remaining DO_NOT_CALL call comes after them.
    assert (r.self_quarantined, r.anchor_gone, r.anchor_later) == (38, 63, 15)
    assert r.flagged_distinct - r.flagged_silver == 38 + 63 + 15
    # Leads, all four regions combined.
    assert (r.leads_distinct, r.leads_silver) == (4_286, 4_223)


@pytest.mark.slow
def test_finding_au_dst_overlap_calls(spark, real_calls):
    rows = {r.call_id: r for r in run_finding(spark, FINDINGS, "au_dst_overlap_calls")}
    assert set(rows) == {"CL-00038707", "CL-00039376"}
    # Stored duration equals duration_seconds for both: start and end share an offset.
    assert {k: (r.stored_diff_seconds, r.duration_seconds) for k, r in rows.items()} == {
        "CL-00038707": (1_352, 1_352), "CL-00039376": (76, 76)
    }  # fmt: skip
