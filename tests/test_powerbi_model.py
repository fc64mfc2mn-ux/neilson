"""Step 16: the Power BI model's measures, mirrored in SQL.

Power BI cannot run here, so the tests check three things: the status rule behind
Lead to Policy Conversion Rate (assumption 3), the expected_* queries in
src/sql/findings_powerbi.sql on small fixtures (denominators, grain, nulls), and that
powerbi/measures.dax encodes the same logic. Real-data tests are marked `slow`.
"""

from __future__ import annotations

import pytest

from src.powerbi_expected import run_query

FIXTURE = "global_temp."


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
