"""Step 16: the Power BI model's measures, mirrored in SQL.

Power BI cannot run here, so the tests check three things: the status rule behind
Lead to Policy Conversion Rate (assumption 3), the expected_* queries in
src/sql/findings_powerbi.sql on small fixtures (denominators, grain, nulls), and that
powerbi/measures.dax encodes the same logic. Real-data tests are marked `slow`.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from src.powerbi_expected import expected_csv, expected_rows, run_query

FIXTURE = "global_temp."
REPO = Path(__file__).resolve().parents[1]
EXPECTED = REPO / "powerbi" / "expected_values.csv"


def _views(spark, **tables):
    """Register fixture DataFrames as global temp views, the fixture `gold` schema."""
    for name, (rows, schema) in tables.items():
        spark.createDataFrame(rows, schema).createOrReplaceGlobalTempView(name)


REGIONS = ([(1, "UK"), (2, "AU")], "region_key long, region_code string")


def test_status_rule_counts_linked_policies_and_compares_three_rules(spark):
    _views(
        spark,
        dim_region=REGIONS,
        fact_lead=(
            [(10, 1), (11, 1), (12, 1), (13, 1), (20, 2)],
            "lead_key long, region_key long",
        ),
        fact_policy=(
            [
                (10, True, "INFORCE"),
                (11, True, "NTU"),
                (12, True, "CANCELLED"),
                (13, True, "LAPSED"),
                (None, False, "INFORCE"),  # no surviving lead: never counted
            ],
            "lead_key long, is_lead_attributed boolean, policy_status string",
        ),
    )
    rows = {r.region_code: r for r in run_query(spark, "conversion_by_status_rule", FIXTURE)}
    uk = rows["UK"]
    assert (uk.leads, uk.policies_all, uk.policies_excl_ntu, uk.policies_inforce_only) == (
        4, 4, 3, 1,
    )  # fmt: skip
    assert float(uk.rate_excl_ntu_pct) == 75.0
    assert float(uk.stricter_rule_drop_pp) == 50.0
    assert rows["ALL"].leads == 5
    assert "AU" not in rows  # a region with no linked policy has no rate row


# ---------------------------------------------------------------------------
# Expected values on fixtures: denominators, grain, nulls
# ---------------------------------------------------------------------------

D_APR, D_MAY = 20260410, 20260512


def _model_fixture(spark):
    """Two regions, April and May. Lead keys: UK 1-4 (April), AU 5 (May)."""
    _views(
        spark,
        dim_region=REGIONS,
        dim_date=(
            [(D_APR, "2026-04"), (D_MAY, "2026-05")],
            "date_key int, year_month string",
        ),
        fact_lead=(
            [(k, D_APR) for k in (1, 2, 3, 4)] + [(5, D_MAY)],
            "lead_key long, lead_created_date_key int",
        ),
        fact_lead_funnel=(
            [
                (1, 1, Decimal("10"), 1),
                (2, 1, Decimal("20"), 0),
                (3, 1, Decimal("40"), 0),
                (4, 1, None, 0),
                (5, 2, Decimal("7"), 1),
            ],
            "lead_key long, region_key long, minutes_to_first_call decimal(12,2), "
            "connected_call_count int",
        ),  # fmt: skip
        fact_policy=(
            [
                (1, 1, True, "INFORCE"),  # counted in April (lead month)
                (2, 1, True, "CANCELLED"),  # counted: it was sold
                (3, 1, True, "NTU"),  # never counted
                (None, 1, False, "INFORCE"),  # no surviving lead: never counted
            ],
            "lead_key long, region_key long, is_lead_attributed boolean, " "policy_status string",
        ),  # fmt: skip
        fact_call=(
            [
                (1, D_APR, True, 100, 20),
                (1, D_APR, True, 300, 0),
                (1, D_APR, False, 999, 99),  # not connected: out of both sides
                (2, D_MAY, True, 50, 10),
            ],
            "region_key long, call_date_key int, is_connected boolean, "
            "talk_time_seconds int, hold_time_seconds int",
        ),
        fact_marketing_spend_daily=(
            [(D_APR, 1, Decimal("100")), (D_APR, 2, Decimal("20"))],  # no spend in May
            "date_key int, region_key long, spend_gbp decimal(14,2)",
        ),
    )


def _expected(spark):
    _model_fixture(spark)
    return {(m, s, v): x for m, s, v, x in expected_rows(spark, FIXTURE)}


def test_leads_are_distinct_surviving_leads_by_lead_month_and_region(spark):
    e = _expected(spark)
    assert e[("Leads (Deduplicated)", "all", "ALL")] == "5.000000"
    assert e[("Leads (Deduplicated)", "region", "UK")] == "4.000000"
    assert e[("Leads (Deduplicated)", "month", "2026-05")] == "1.000000"


def test_conversion_counts_linked_non_ntu_policies_over_leads_and_blank_without_any(spark):
    e = _expected(spark)
    m = "Lead to Policy Conversion Rate"
    assert e[(m, "all", "ALL")] == "0.400000"  # 2 policies (INFORCE, CANCELLED) / 5
    assert e[(m, "region", "UK")] == "0.500000"
    assert e[(m, "month", "2026-04")] == "0.500000"
    assert e[(m, "region", "AU")] == ""  # leads but no policy: BLANK, not 0
    assert e[(m, "month", "2026-05")] == ""


def test_cost_per_lead_is_blank_where_there_is_no_spend(spark):
    e = _expected(spark)
    m = "Cost Per Lead (GBP)"
    assert e[(m, "all", "ALL")] == "24.000000"  # 120 / 5
    assert e[(m, "month", "2026-04")] == "30.000000"  # 120 / 4
    assert e[(m, "month", "2026-05")] == ""
    assert e[("Marketing Spend (GBP)", "region", "AU")] == "20.000000"


def test_aht_uses_connected_calls_on_both_sides(spark):
    e = _expected(spark)
    m = "Average Handle Time (Seconds)"
    assert e[(m, "all", "ALL")] == "160.000000"  # (450 + 30) / 3
    assert e[(m, "month", "2026-04")] == "210.000000"  # 420 / 2


def test_median_ignores_never_called_leads_and_averages_the_middle_two(spark):
    e = _expected(spark)
    m = "Time to First Call (Median Minutes)"
    assert e[(m, "region", "UK")] == "20.000000"  # 10, 20, 40; lead 4 never called
    assert e[(m, "all", "ALL")] == "15.000000"  # 7, 10, 20, 40 -> (10 + 20) / 2


def test_expected_csv_is_byte_identical_on_a_second_run(spark):
    _model_fixture(spark)
    first = expected_csv(spark, FIXTURE)
    assert first == expected_csv(spark, FIXTURE)
    assert first.startswith("measure,slice,slice_value,value\nMarketing Spend (GBP),all,ALL,")


# ---------------------------------------------------------------------------
# Real data
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def gold(spark, real_gold_all):
    return real_gold_all


@pytest.mark.slow
def test_real_status_rule_effect_overall_and_by_region(spark, gold):
    """Quoted in docs/ASSUMPTIONS.md (3) and docs/KPI_DECISION_LOG.md."""
    rows = {r.region_code: r for r in run_query(spark, "conversion_by_status_rule")}
    got = {
        k: (r.leads, r.policies_all, r.policies_excl_ntu, r.policies_inforce_only,
            float(r.rate_excl_ntu_pct), float(r.rate_inforce_only_pct),
            float(r.stricter_rule_drop_pp), float(r.stricter_rule_relative_drop_pct))
        for k, r in rows.items()
    }  # fmt: skip
    assert got == {
        "ALL": (47201, 4767, 4499, 3828, 9.53, 8.11, 1.42, 14.9),
        "AU": (8955, 503, 475, 402, 5.30, 4.49, 0.82, 15.4),
        "CA": (11317, 1245, 1182, 1024, 10.44, 9.05, 1.40, 13.4),
        "UK": (18902, 2059, 1935, 1627, 10.24, 8.61, 1.63, 15.9),
        "US": (8027, 960, 907, 775, 11.30, 9.65, 1.64, 14.6),
    }


@pytest.mark.slow
def test_real_each_surviving_lead_has_at_most_one_policy_in_its_own_region(spark, gold):
    """Why the measure's 4,499 policies equal the legacy port's 4,499 converted leads,
    and why the model can take a policy's region from fact_policy: if either changes,
    the KPI log's explanation and MODEL_DESIGN.md must be revisited."""
    row = spark.sql(
        """
        SELECT COUNT(*) AS policies,
               COUNT(DISTINCT p.lead_key) AS leads,
               COUNT_IF(p.region_key <> l.region_key) AS region_mismatch
        FROM gold.fact_policy p JOIN gold.fact_lead l ON l.lead_key = p.lead_key
        WHERE p.is_lead_attributed
        """
    ).first()
    assert (row.policies, row.leads, row.region_mismatch) == (4767, 4767, 0)


@pytest.mark.slow
def test_real_expected_values_file_is_reproducible(spark, gold):
    """powerbi/expected_values.csv equals a fresh run of the queries on gold."""
    assert expected_csv(spark) == EXPECTED.read_text(encoding="utf-8")


@pytest.mark.slow
def test_real_premium_coverage_and_aht_scope(spark, gold):
    """Quoted in measures.dax (Average Annual Premium) and MODEL_DESIGN.md."""
    cov = run_query(spark, "premium_gbp_coverage")[0]
    assert (cov.policies_counted, cov.with_gbp_premium, float(cov.coverage_pct)) == (
        4884, 4240, 86.8,
    )  # fmt: skip
    aht = run_query(spark, "aht_numerator_scope")[0]
    assert (aht.connected_calls, aht.unconnected_with_talk) == (19126, 11736)
    assert (float(aht.aht_connected_only), float(aht.aht_all_calls_numerator)) == (
        517.6, 642.6,
    )  # fmt: skip
