"""Step 18: Connected Call to Policy Conversion Rate and its bridge from Lead to Policy.

Fixture tests check the rule (policies by start date over connected calls by call date,
business-hours surviving leads on both sides, NTU and no-lead policies out). The bridge
tests check that each step changes one thing and that the steps telescope exactly. Slow
tests recompute the figures from the real gold tables in pandas, independently of the SQL.
"""

from __future__ import annotations

import csv
import io
from decimal import Decimal
from pathlib import Path

import pytest

from src.kpi_reconciliation import STEP_NAMES, bridge_csv, bridge_rows
from src.powerbi_expected import MEASURES, _value, expected_rows, run_query

FIXTURE = "global_temp."
REPO = Path(__file__).resolve().parents[1]
MEASURE = "Connected Call to Policy Conversion Rate"
D_APR, D_MAY, D_JUL = 20260410, 20260512, 20260715


def _views(spark, **tables):
    for name, (rows, schema) in tables.items():
        spark.createDataFrame(rows, schema).createOrReplaceGlobalTempView(name)


def _fixture(spark):
    """Leads 1-3 UK (April; 3 outside business hours), 4-5 AU (May; 5 outside)."""
    _views(
        spark,
        dim_region=([(1, "UK"), (2, "AU")], "region_key long, region_code string"),
        dim_date=(
            [(D_APR, 2026, 2, "2026-04"), (D_MAY, 2026, 2, "2026-05"), (D_JUL, 2026, 3, "2026-07")],
            "date_key int, year int, quarter int, year_month string",
        ),
        fact_lead_funnel=(
            [
                (1, 1, D_APR, True),
                (2, 1, D_APR, True),
                (3, 1, D_APR, False),
                (4, 2, D_MAY, True),
                (5, 2, D_MAY, False),
            ],
            "lead_key long, region_key long, lead_created_date_key int, "
            "lead_in_business_hours_flag boolean",
        ),  # fmt: skip
        fact_policy=(
            [
                (1, 1, True, "INFORCE", D_APR, D_MAY),  # counted, May
                (2, 1, True, "CANCELLED", D_APR, D_JUL),  # counted (it was sold), July
                (1, 1, True, "NTU", D_APR, D_MAY),  # never counted
                (3, 1, True, "INFORCE", D_APR, D_MAY),  # lead outside business hours
                (None, 1, False, "INFORCE", None, D_MAY),  # no surviving lead
            ],
            "lead_key long, region_key long, is_lead_attributed boolean, policy_status string, "
            "lead_created_date_key int, policy_start_date_key int",
        ),
        fact_call=(
            [
                (1, 1, D_APR, True),
                (1, 1, D_MAY, True),
                (2, 1, D_APR, False),
                (3, 1, D_APR, True),  # lead outside business hours
                (4, 2, D_MAY, True),
                (2, 1, D_JUL, True),
            ],
            "lead_key long, region_key long, call_date_key int, is_connected boolean",
        ),  # fmt: skip
    )


def _expected(spark):
    _fixture(spark)
    rows = run_query(spark, "expected_connected_call_to_policy_conversion_rate", FIXTURE)
    return {(MEASURE, r.slice, r.slice_value): _value(r.value) for r in rows}


def test_rate_is_policies_by_start_date_over_connected_calls_by_call_date(spark):
    e = _expected(spark)
    assert e[(MEASURE, "all", "ALL")] == "0.500000"  # 2 policies / 4 connected calls
    assert e[(MEASURE, "region", "UK")] == "0.666667"  # 2 / 3
    assert e[(MEASURE, "month", "2026-05")] == "0.500000"  # 1 policy started / 2 calls
    assert e[(MEASURE, "month", "2026-07")] == "1.000000"  # 1 / 1
    assert e[(MEASURE, "quarter", "2026-Q2")] == "0.333333"  # 1 / 3
    assert e[(MEASURE, "quarter", "2026-Q3")] == "1.000000"


def test_blank_not_zero_where_calls_exist_but_no_policy(spark):
    e = _expected(spark)
    assert e[(MEASURE, "region", "AU")] == ""
    assert e[(MEASURE, "month", "2026-04")] == ""


def test_measure_is_in_the_expected_values_file_list():
    assert (MEASURE, "expected_connected_call_to_policy_conversion_rate") in MEASURES


def _bridge(spark):
    _fixture(spark)
    return bridge_rows(spark, FIXTURE)


def test_bridge_counts_change_one_thing_per_step_on_the_fixture(spark):
    rows = {(r["period"], r["step"]): r for r in _bridge(spark)}
    got = [
        (rows[("Q2 2026", s)]["numerator"], rows[("Q2 2026", s)]["denominator"]) for s in range(7)
    ]
    assert got == [(3, 5), (2, 5), (2, 6), (2, 5), (2, 4), (1, 3), (1, 3)]
    got = [
        (rows[("All dates", s)]["numerator"], rows[("All dates", s)]["denominator"])
        for s in range(7)
    ]
    assert got == [(3, 5), (3, 5), (3, 6), (3, 5), (3, 5), (2, 4), (2, 4)]
    assert len(STEP_NAMES) == 7


def test_bridge_steps_sum_exactly_and_end_on_the_measure(spark):
    rows = _bridge(spark)
    e = _expected(spark)
    for period, value in (("Q2 2026", e[(MEASURE, "quarter", "2026-Q2")]),
                          ("All dates", e[(MEASURE, "all", "ALL")])):  # fmt: skip
        mine = [r for r in rows if r["period"] == period]
        assert Decimal(mine[-1]["rate_pct"]) / 100 == Decimal(value)
        total = sum((Decimal(r["delta_pp"]) for r in mine[1:]), Decimal(0))
        assert Decimal(mine[0]["rate_pct"]) + total == Decimal(mine[-1]["rate_pct"])  # no residual


def test_bridge_csv_is_byte_identical_on_a_second_run(spark):
    _fixture(spark)
    first = bridge_csv(bridge_rows(spark, FIXTURE))
    assert first == bridge_csv(bridge_rows(spark, FIXTURE))
    assert first.splitlines()[0].startswith("period,step,step_name,")


# ---------------------------------------------------------------------------
# Real data: independent pandas recomputation
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def gold(spark, real_gold_all):
    import pandas as pd

    t = {n: spark.table(f"gold.{n}").toPandas() for n in
         ("fact_policy", "fact_call", "fact_lead_funnel", "dim_date", "dim_region")}  # fmt: skip
    return t, pd


def _pandas_values(t):
    """Connected Call to Policy Conversion Rate by slice, from first principles."""
    import pandas as pd

    f, p, c, d, r = (t[k] for k in ("fact_lead_funnel", "fact_policy", "fact_call",
                                    "dim_date", "dim_region"))  # fmt: skip
    bh = set(f.loc[f.lead_in_business_hours_flag, "lead_key"])
    d = d.assign(q=d.year.astype(str) + "-Q" + d.quarter.astype(str))
    pol = p[p.is_lead_attributed & (p.policy_status.fillna("") != "NTU") & p.lead_key.isin(bh)]
    dq, rq = d[["date_key", "year_month", "q"]], r[["region_key", "region_code"]]
    pol = pol.merge(dq, left_on="policy_start_date_key", right_on="date_key")
    pol = pol.merge(rq, on="region_key")
    cal = c[c.is_connected & c.lead_key.isin(bh)]
    cal = cal.merge(dq, left_on="call_date_key", right_on="date_key")
    cal = cal.merge(rq, on="region_key")
    out = {("all", "ALL"): (len(pol), len(cal))}
    for sl, col in (("region", "region_code"), ("month", "year_month"), ("quarter", "q")):
        n, m = pol.groupby(col).size(), cal.groupby(col).size()
        for k in m.index.union(n.index):
            out[(sl, k)] = (int(n.get(k, 0)), int(m.get(k, 0)))
    return out, pd


@pytest.mark.slow
def test_real_headline_counts_and_values_match_an_independent_pandas_run(spark, gold):
    t, _ = gold
    pandas_values, _ = _pandas_values(t)
    sql = {(s, v): x for m, s, v, x in expected_rows(spark) if m == MEASURE}
    assert pandas_values[("all", "ALL")] == (1760, 7329)
    assert pandas_values[("quarter", "2026-Q2")] == (1324, 7135)
    for key, (n, m) in pandas_values.items():
        want = (
            ""
            if n == 0 or m == 0
            else str((Decimal(n) / Decimal(m)).quantize(Decimal("0.000001"), "ROUND_HALF_UP"))
        )
        assert sql[key] == want, key
    assert set(sql) == set(pandas_values)


@pytest.mark.slow
def test_real_calls_and_policies_share_their_lead_region(spark, gold):
    """The DAX filters each fact on its own region_key; the SQL and the lead set agree
    only because region never differs between a lead, its calls and its policies."""
    t, _ = gold
    f, c, p = t["fact_lead_funnel"], t["fact_call"], t["fact_policy"]
    reg = f.set_index("lead_key").region_key
    cl, pl = c[c.lead_key.notna()], p[p.lead_key.notna()]
    assert (cl.region_key.values == reg.reindex(cl.lead_key).values).all()
    assert (pl.region_key.values == reg.reindex(pl.lead_key).values).all()


@pytest.mark.slow
def test_real_bridge_matches_pandas_and_ends_on_the_measure(spark, gold):
    t, _ = gold
    f, p, c, d = (t[k] for k in ("fact_lead_funnel", "fact_policy", "fact_call", "dim_date"))
    q2 = set(d.loc[(d.year == 2026) & (d.quarter == 2), "date_key"])
    sold = p[p.is_lead_attributed & (p.policy_status.fillna("") != "NTU")]
    bh = set(f.loc[f.lead_in_business_hours_flag, "lead_key"])
    leads_q2 = set(f.loc[f.lead_created_date_key.isin(q2), "lead_key"])
    calls = c[c.lead_key.notna()]
    conn = calls[calls.is_connected]
    want = [
        (int(sold.lead_created_date_key.isin(q2).sum()), len(leads_q2)),
        (int(sold.policy_start_date_key.isin(q2).sum()), len(leads_q2)),
        (int(sold.policy_start_date_key.isin(q2).sum()), int(calls.lead_key.isin(leads_q2).sum())),
        (int(sold.policy_start_date_key.isin(q2).sum()), int(conn.lead_key.isin(leads_q2).sum())),
        (int(sold.policy_start_date_key.isin(q2).sum()), int(conn.call_date_key.isin(q2).sum())),
    ]
    last = (int((sold.policy_start_date_key.isin(q2) & sold.lead_key.isin(bh)).sum()),
            int((conn.call_date_key.isin(q2) & conn.lead_key.isin(bh)).sum()))  # fmt: skip
    want += [last, last]
    rows = [r for r in bridge_rows(spark) if r["period"] == "Q2 2026"]
    assert [(r["numerator"], r["denominator"]) for r in rows] == want
    assert last == (1324, 7135)
    total = sum((Decimal(r["delta_pp"]) for r in rows[1:]), Decimal(0))
    assert Decimal(rows[0]["rate_pct"]) + total == Decimal(rows[-1]["rate_pct"])


@pytest.mark.slow
def test_real_committed_outputs_are_reproducible(spark, gold):
    committed = (REPO / "powerbi" / "kpi_reconciliation.csv").read_text(encoding="utf-8")
    assert bridge_csv(bridge_rows(spark)) == committed
    assert list(csv.reader(io.StringIO(committed)))[0][0] == "period"


@pytest.mark.slow
def test_real_variants_quoted_in_the_reconciliation(spark, gold):
    """Quoted in docs/KPI_RECONCILIATION.md: no variant gives the indicative 20.5%."""
    v = run_query(spark, "kpi_connected_call_variants")[0]
    assert (v.policies_all, v.policies_linked) == (3688, 3392)
    assert (v.connected_all, v.connected_linked) == (18607, 18338)
