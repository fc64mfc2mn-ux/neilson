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
        "call_au_timestamp_unverified": 15,
        "call_end_before_start": 16,
        "call_start_before_lead": 16,
        "call_lead_not_found": 17,
        "call_lead_quarantined": 17,
        "call_agent_not_found": 17,
        "call_after_do_not_call": 26,
    }.items() <= CHECK_DEFECTS.items()


def test_config_keeps_every_region_as_utc():
    zones = load_config(str(REPO_ROOT / "config" / "config.yaml"))["calls"]["source_timezone"]
    assert zones == {
        "UK": "UTC",
        "AU": "UTC",
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
    # duplicates included), then 245 start before their lead (the register's "about 0.4%").
    assert rec.removed_by_file["call_end_before_start"] == {
        "calls_uk_au.csv": 256, "calls_ca_us.csv": 124
    }  # fmt: skip
    assert rec.removed_by_file["call_start_before_lead"] == {
        "calls_uk_au.csv": 176, "calls_ca_us.csv": 69
    }  # fmt: skip
    assert rec.removed_by_file.get("call_timestamp_unparseable", {}) == {}
    # 64,000 = 62,835 + 540 + 380 + 245. CLAUDE.md expects 62,830 from a rounded 250.
    assert (rec.rows_in, rec.rows_out, rec.rows_quarantined) == (64_000, 62_835, 1_165)
    assert rec.rows_in == rec.rows_out + rec.rows_quarantined
    # By call id: 63,460 distinct = 62,835 out + 380 + 245 (duplicates share an id).
    assert rec.distinct_ids_in == 63_460 == rec.rows_out + 380 + 245
    assert read_table(spark, config, "silver.call").count() == 62_835


@pytest.mark.slow
def test_call_quarantine_and_flag_counts(spark, real_calls):
    config, _ = real_calls
    expect = {
        # check: (rows, severity, action)
        "call_exact_duplicate": (540, "info", "quarantined"),
        "call_end_before_start": (380, "error", "quarantined"),
        "call_start_before_lead": (245, "warning", "quarantined"),
        # Flags are raised on rows kept in silver: 1,100 ghost-lead calls less 6 that end
        # before they start; 620 ghost-agent calls less 4 + 1 quarantined.
        "call_lead_not_found": (1_094, "info", "flagged"),
        "call_agent_not_found": (615, "info", "flagged"),
        "call_au_timestamp_unverified": (14_768, "warning", "flagged"),
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
        "UK": 25_454, "AU": 14_768, "CA": 12_794, "US": 9_819
    }  # fmt: skip
    # CA/US keep their own SLA and wrap-up values.
    assert calls.where("region_code = 'CA' AND answered_within_sla_flag IS NOT NULL").count() > 0
    assert calls.where("region_code = 'US' AND wrap_up_code IS NULL").count() == 0
    # Flags are columns too, so gold can use them without reading dq_exception.
    assert calls.where("after_do_not_call").count() == 6_311
    assert calls.where("start_ts_offset_unverified").count() == 14_768
    assert calls.where("start_ts_offset_unverified AND region_code != 'AU'").count() == 0
    calls.unpersist()


@pytest.mark.slow
def test_au_first_call_gap_is_unexplained_and_au_is_kept_as_stored(spark, real_calls):
    """Assumption 5. Fails if anyone converts AU or the evidence changes."""
    config, _ = real_calls
    gap = {r.region_code: r for r in run_finding(spark, FINDINGS, "first_call_gap_by_hypothesis")}
    # UK, CA and US behave as UTC: first call 22 to 45 minutes after the lead.
    assert round(gap["UK"].median_min_as_utc) == 22
    assert round(gap["CA"].median_min_as_utc) == 45 and round(gap["US"].median_min_as_utc) == 40
    # As local time they would be 38 minutes early (UK) or 4.7 hours late (CA, US).
    assert round(gap["UK"].median_min_as_local) == -38
    assert round(gap["CA"].median_min_as_local) == 285
    # AU: 18.5 hours as UTC, still 8.5 hours as Sydney local time. Neither fits.
    assert round(gap["AU"].median_min_as_utc) == 1_111
    assert round(gap["AU"].median_min_as_local) == 509
    # silver.call keeps AU as stored: no AU start differs from its bronze value, and the
    # first-call median stays near the as-UTC figure (1,113 vs 1,111: silver has already
    # dropped quarantined calls). A Sydney conversion would put it near 510.
    au = run_finding(spark, FINDINGS, "au_first_call_gap_in_silver")[0]
    message = (
        "silver.call AU timestamps no longer match the stored values: AU has been "
        "converted. Update assumption 5, defect 15 and this test together."
    )
    assert (au.au_rows_changed, au.au_rows) == (0, 14_768), message
    assert round(au.median_min) == 1_113, message


@pytest.mark.slow
def test_au_calls_not_leads_carry_a_ten_hour_offset(spark, real_calls):
    """Which side is shifted: policy dates fit AU calls moved back 10 h; leads look fine."""
    rows = run_finding(spark, FINDINGS, "policy_date_vs_call_date_by_offset")
    fit = {(r.region_code, r.offset_hours): (r.matching, r.policies) for r in rows}
    for region, n in (("UK", 1_716), ("CA", 1_058), ("US", 810)):
        assert fit[(region, 0)] == (n, n)  # application date = call date, as UTC
    assert fit[("AU", 0)] == (248, 426)
    assert fit[("AU", 10)] == (426, 426)  # only a 10 h shift makes every AU policy fit
    assert fit[("AU", 9)][0] < 426 and fit[("AU", 11)][0] < 426
    lead_side = {r.region_code: r for r in run_finding(spark, FINDINGS, "lead_internal_timing")}
    # AU lead ingestion lag equals every other region: no shift on the lead side.
    assert {round(r.median_ingest_lag_min, 1) for r in lead_side.values()} <= {2.6, 2.7}


@pytest.mark.slow
def test_finding_register_vs_distinct(spark, real_calls):
    r = run_finding(spark, FINDINGS, "register_vs_distinct")[0]
    assert (r.end_before_start_raw_uk_au, r.end_before_start_raw, r.end_before_start) == (
        262, 388, 380
    )  # fmt: skip
    assert (r.before_lead_raw, r.before_lead) == (248, 245)
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
    # 245 calls start between 40 minutes and 24 hours before their lead.
    assert r.calls == 245
    assert round(r.min_minutes) == 40 and round(r.max_minutes) == 1_434
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
