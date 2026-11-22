"""Step 9: silver.policy (defects 19 to 23)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.dq import CHECK_DEFECTS, read_dq_exceptions
from src.common.findings import run_finding
from src.common.tables import read_table
from src.silver.policy import (
    build_policy,
    derive_annual_premium,
    merge_conflicting,
    split_duplicates,
)

FINDINGS = Path(__file__).resolve().parents[1] / "src" / "sql" / "findings_policy.sql"
COLS = "policy_id string, lead_id string, agent_id string, annual_premium_local string, " \
    "monthly_premium_local string, _row_hash string, _source_row long"  # fmt: skip


# ------------------------------------------------------------------ unit


def test_split_duplicates_exact_copies_and_conflicting_rows(spark):
    raw = spark.createDataFrame(
        [
            ("P1", "L1", "A1", "120", "10", "h1", 1),
            ("P1", "L1", "A1", "120", "10", "h1", 9),  # exact copy: removed
            ("P2", "L2", "A2", "240", "20", "h2", 2),
            ("P2", None, "A3", "240", "20", "h3", 8),  # conflicting
            ("P3", "L3", "A3", "360", "30", "h4", 3),
        ],
        COLS,
    )
    exact, conflicting = split_duplicates(raw)
    assert [(r.policy_id, r._source_row) for r in exact.collect()] == [("P1", 9)]
    assert sorted((r.policy_id, r._source_row) for r in conflicting.collect()) == [
        ("P2", 2), ("P2", 8)
    ]  # fmt: skip


def test_merge_conflicting_earliest_row_wins_and_nulls_are_filled(spark):
    rows = spark.createDataFrame(
        [
            ("P1", None, "A1", "1186.9", "98.91", "h1", 10),
            ("P1", "L1", "A2", "1427.33", "98.91", "h2", 50),
        ],
        COLS,
    )
    merged, removed = merge_conflicting(rows, ["lead_id", "agent_id", "annual_premium_local",
                                               "monthly_premium_local"])  # fmt: skip
    m = merged.collect()
    assert len(m) == 1
    # Conflict (both non-null): earliest source row wins. Null: filled from the other row.
    assert (m[0].agent_id, m[0].annual_premium_local, m[0].lead_id) == ("A1", "1186.9", "L1")
    assert m[0]._source_row == 10
    r = removed.collect()
    assert [x._source_row for x in r] == [50]
    assert r[0].detail == (
        "lead_id: NULL vs L1 (filled); agent_id: A1 vs A2 (kept A1); "
        "annual_premium_local: 1186.9 vs 1427.33 (kept 1186.9)"
    )


def test_derive_annual_premium_only_where_it_is_null(spark):
    df = spark.createDataFrame(
        [("P1", None, 31.53), ("P2", 400.0, 32.75), ("P3", None, None)],
        "policy_id string, annual_premium_local double, monthly_premium_local double",
    )
    out = {r.policy_id: r for r in derive_annual_premium(df).collect()}
    assert (round(out["P1"].annual_premium_local, 2), out["P1"].annual_premium_derived) == (
        378.36, True
    )  # fmt: skip
    assert (out["P2"].annual_premium_local, out["P2"].annual_premium_derived) == (400.0, False)
    assert (out["P3"].annual_premium_local, out["P3"].annual_premium_derived) == (None, False)


def test_policy_checks_are_mapped():
    assert {
        "policy_exact_duplicate": 19,
        "policy_conflicting_duplicate": 19,
        "policy_lead_missing": 20,
        "policy_annual_premium_derived": 21,
        "policy_premium_above_12x_monthly": 21,
    }.items() <= CHECK_DEFECTS.items()


# ------------------------------------------------------------------ real data


@pytest.fixture(scope="module")
def real_policy(spark, real_reference):
    config, _ = real_reference
    return config, build_policy(spark, config)


def _checks(spark, config, name):
    return read_dq_exceptions(spark, config).where(F.col("check_name") == name)


@pytest.mark.slow
def test_policy_rows_reconcile(spark, real_policy):
    config, rec = real_policy
    # 5,200 rows = 5,172 policies + 12 exact copies + 16 conflicting copies merged away.
    assert (rec.rows_in, rec.distinct_ids_in) == (5_200, 5_172)
    assert (rec.exact_copies, rec.conflicting_removed) == (12, 16)
    assert (rec.rows_out, rec.rows_quarantined) == (5_172, 28)
    assert rec.rows_in == rec.rows_out + rec.rows_quarantined
    policies = read_table(spark, config, "silver.policy")
    assert policies.count() == policies.select("policy_id").distinct().count() == 5_172
    assert policies.where("merged_from_duplicates").count() == 16


@pytest.mark.slow
def test_policy_dq_counts(spark, real_policy):
    config, _ = real_policy
    expect = {
        "policy_exact_duplicate": (12, "info", "quarantined"),
        "policy_conflicting_duplicate": (16, "warning", "quarantined"),
        # 410 raw null lead_id: 4 sit on copies merged away (406), 2 more are filled by
        # coalescing from the other copy (404).
        "policy_lead_missing": (404, "info", "flagged"),
        # 620 raw null annual premium: 1 on an exact copy, 1 on a merged-away copy, 1
        # filled by coalescing: 617 derived as monthly x 12.
        "policy_annual_premium_derived": (617, "info", "corrected"),
        # Supplied annual premium above 12 x monthly (by more than 0.01 per month):
        # flagged for the PolicyCore owner, values unchanged (docs/OPEN_QUESTIONS.md).
        "policy_premium_above_12x_monthly": (187, "warning", "flagged"),
    }
    for check, (rows, severity, action) in expect.items():
        got = _checks(spark, config, check)
        assert got.count() == rows, check
        assert {(r.severity, r.action) for r in got.select("severity", "action").distinct()
                .collect()} == {(severity, action)}, check  # fmt: skip
    details = [r.detail for r in _checks(spark, config, "policy_conflicting_duplicate").collect()]
    assert all(d and ("(filled)" in d or "(kept" in d) for d in details)


@pytest.mark.slow
def test_silver_policy_content(spark, real_policy):
    config, _ = real_policy
    p = read_table(spark, config, "silver.policy").cache()
    assert p.where("annual_premium_local IS NULL").count() == 0
    assert p.where("annual_premium_derived").count() == 617
    assert p.where("lead_id IS NULL").count() == 404  # kept, flagged
    # Statuses kept raw: no filter at silver; which ones count is a measure decision.
    assert dict(p.groupBy("policy_status").count().collect()) == {
        "INFORCE": 4_155, "LAPSED": 309, "NTU": 288, "CANCELLED": 420
    }  # fmt: skip
    assert dict(p.dtypes)["application_date"] == "date"
    assert dict(p.dtypes)["annual_premium_local"].startswith("decimal")
    p.unpersist()


@pytest.mark.slow
def test_finding_policy_conflict_fields(spark, real_policy):
    rows = {r.field: r for r in run_finding(spark, FINDINGS, "conflicting_duplicate_fields")}
    # Policies whose two copies differ on each field, null-vs-value or value-vs-value.
    assert {k: (r.null_fill, r.value_conflict) for k, r in rows.items()} == {
        "lead_id": (6, 0), "call_id": (11, 0), "agent_id": (0, 1),
        "annual_premium_local": (2, 3)
    }  # fmt: skip


@pytest.mark.slow
def test_finding_survivorship_evidence(spark, real_policy):
    """Earliest copy wins value conflicts: it matches the independent evidence."""
    r = run_finding(spark, FINDINGS, "survivorship_evidence")[0]
    # Agent conflict: the linked call's agent is the earliest copy's agent.
    assert (r.agent_conflicts, r.agent_earliest_matches_call) == (1, 1)
    # Premium conflicts: earliest copy = monthly x 12 in 2 of 3; the later copy in none.
    assert (r.premium_conflicts, r.premium_earliest_x12, r.premium_later_x12) == (3, 2, 0)


@pytest.mark.slow
def test_finding_annual_to_monthly_ratio(spark, real_policy):
    r = run_finding(spark, FINDINGS, "annual_to_monthly_ratio")[0]
    # On policies that arrived with both premiums, annual = monthly x 12 (to 0.01) for
    # 96% of them; the rest sit between 12.6 and 15 (a finding, not changed).
    assert r.with_both == 4_555
    assert r.at_12 == 4_368  # 95.9%; the other 187 sit at 12.6 to 15
    assert round(r.median_ratio, 2) == 12.0
    assert r.min_ratio > 11.99 and r.max_ratio < 15.0
    assert r.min_other > 12.6


@pytest.mark.slow
def test_finding_declined_yet_inforce(spark, real_policy):
    r = run_finding(spark, FINDINGS, "declined_yet_inforce")[0]
    assert (r.raw_rows, r.policies) == (267, 265)


@pytest.mark.slow
def test_finding_lead_missing_trace(spark, real_policy):
    r = run_finding(spark, FINDINGS, "lead_missing_trace")[0]
    # 410 raw -> 406 once duplicate copies are removed -> 404 after coalescing fills 2.
    assert (r.raw_null, r.after_removing_copies, r.silver_null) == (410, 406, 404)


@pytest.mark.slow
def test_premium_flag_changes_no_value(spark, real_policy):
    config, _ = real_policy
    p = read_table(spark, config, "silver.policy")
    flagged = _checks(spark, config, "policy_premium_above_12x_monthly").select(
        F.col("key_value").alias("policy_id")
    )
    rows = p.join(flagged, "policy_id")
    assert rows.count() == 187
    assert rows.where("annual_premium_derived").count() == 0
    # Still the supplied value: above 12 x monthly on every flagged row.
    assert rows.where("annual_premium_local <= monthly_premium_local * 12.01").count() == 0


@pytest.mark.slow
def test_finding_premium_above_12x_monthly(spark, real_policy):
    rows = run_finding(spark, FINDINGS, "premium_above_12x_monthly")
    assert sum(r.policies for r in rows) == 187
    assert min(r.min_ratio for r in rows) > 12.6 and max(r.max_ratio for r in rows) < 15
