"""The ported query and the naive translation differ by a stated row-count
difference and a stated CPL scaling factor, matching the behavioural
differences you found and listed in your submission.

Naive: src/sql/legacy_sp_marketing_lead_summary_naive.sql, a literal Spark SQL
translation of data/legacy/sp_MarketingLeadSummary.sql run on the raw (bronze)
data. Ported: src/sql/port_sp_marketing_lead_summary.sql on gold. GETDATE() is
the `as_of` parameter (default 2026-06-30 00:00:00). Differences: docs/LEGACY_PORT.md.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.findings import run_finding
from src.legacy_port import DEFAULT_AS_OF, naive_summary, ported_summary

FINDINGS = Path(__file__).resolve().parents[1] / "src" / "sql" / "findings_legacy.sql"
LEADS = "LeadID string, RegionCode string, ChannelCode string, LeadStatus string, " \
        "CreatedAt timestamp"  # fmt: skip
SPEND = "SpendDate date, RegionCode string, ChannelCode string, SpendLocal decimal(18,2)"


def _raw(spark, leads=(), calls=(), policies=(), spend=(), regions=("UK",)):
    """Raw-shaped sources: Leads, Calls, Policies, MarketingSpendDaily, RefRegion."""
    lead_df = spark.createDataFrame(list(leads), LEADS.replace("timestamp", "string")).withColumn(
        "CreatedAt", F.to_timestamp("CreatedAt")
    )
    return {
        "leads": lead_df,
        "calls": spark.createDataFrame(list(calls), "CallID string, LeadID string"),
        "policies": spark.createDataFrame(
            list(policies), "PolicyID string, LeadID string, PolicyStatus string"
        ),
        "spend": spark.createDataFrame(list(spend), SPEND),
        "ref_region": spark.createDataFrame([(r,) for r in regions], "RegionCode string"),
    }


def _gold(spark, spend=(), leads=(), calls=(), policies=()):
    """Gold-shaped sources. spend: (date, region, channel, gbp); leads: (key, region,
    channel, created); calls: (lead_key, call_key); policies: (lead_key, counts_as_sale)."""
    regions = sorted({x[1] for x in (*spend, *leads)})
    channels = sorted({x[2] for x in (*spend, *leads)})
    rk = {r: i + 1 for i, r in enumerate(regions)}
    ck = {c: i + 1 for i, c in enumerate(channels)}
    days = spark.sql(
        "SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS date "
        "FROM (SELECT explode(sequence(DATE'2026-01-01', DATE'2026-12-31')) AS d)"
    )
    return {
        "dim_date": days,
        "dim_region": spark.createDataFrame(
            list(rk.items()), "region_code string, region_key long"
        ).select("region_key", "region_code"),
        "dim_channel": spark.createDataFrame(
            list(ck.items()), "channel_code string, channel_key long"
        ).select("channel_key", "channel_code"),  # fmt: skip
        "fact_marketing_spend_daily": spark.createDataFrame(
            [(int(d.strftime("%Y%m%d")), rk[r], ck[c], Decimal(g)) for d, r, c, g in spend],
            "date_key int, region_key long, channel_key long, spend_gbp decimal(14,2)",
        ),
        "fact_lead": spark.createDataFrame(
            [(k, rk[r], ck[c], ts) for k, r, c, ts in leads],
            "lead_key long, region_key long, channel_key long, lead_created_ts_utc string",
        ).withColumn("lead_created_ts_utc", F.to_timestamp("lead_created_ts_utc")),
        "bridge_lead_call": spark.createDataFrame(list(calls), "lead_key long, call_key long"),
        "fact_policy": spark.createDataFrame(
            [(k, True, s) for k, s in policies],
            "lead_key long, is_lead_attributed boolean, counts_as_sale boolean",
        ),
    }


def _one(df, region="UK", channel="TV"):
    rows = df.where((F.col("RegionCode") == region) & (F.col("ChannelCode") == channel)).collect()
    assert len(rows) == 1, rows
    return rows[0]


MAY = "2026-05-10 10:00:00"


# ------------------------------------------------------------------ fixtures, one per difference


def test_fan_out_counts_join_rows_as_calls_and_converted_leads_per_call(spark):
    """One lead, three calls, one sale. Naive: TotalCalls 3, ConvertedLeads 3, rate 300%,
    CPL spend / 3. Ported: 1 lead, 3 calls, 1 converted, rate 100%, CPL spend / 1."""
    raw = _raw(spark, leads=[("L1", "UK", "TV", "NEW", MAY)],
               calls=[(f"C{i}", "L1") for i in range(3)], policies=[("P1", "L1", "INFORCE")],
               spend=[(date(2026, 5, 1), "UK", "TV", Decimal("300.00"))])  # fmt: skip
    n = _one(naive_summary(spark, raw))
    assert (n.TotalLeads, n.TotalCalls, n.ConvertedLeads) == (1, 3, 3)
    assert (n.ConversionRate, n.CPL) == (Decimal("3.000000"), Decimal("100.0000"))
    gold = _gold(spark, spend=[(date(2026, 5, 1), "UK", "TV", "300.00")],
                 leads=[(1, "UK", "TV", MAY)], calls=[(1, c) for c in range(3)],
                 policies=[(1, True)])  # fmt: skip
    p = _one(ported_summary(spark, gold))
    assert (p.TotalLeads, p.TotalCalls, p.ConvertedLeads) == (1, 3, 1)
    assert (p.ConversionRate, p.CPL) == (Decimal("1.000000"), Decimal("300.0000"))


def test_conversion_cast_overflows_above_999_and_spark_returns_null(spark):
    """CAST(ConvertedLeads AS DECIMAL(9,6)) holds at most 999.999999. SQL Server raises
    error 8115 and the procedure fails; Spark (ANSI off) returns NULL silently."""
    raw = _raw(spark, leads=[("L1", "UK", "TV", "NEW", MAY)],
               calls=[(f"C{i}", "L1") for i in range(1000)], policies=[("P1", "L1", "INFORCE")],
               spend=[(date(2026, 5, 1), "UK", "TV", Decimal("10.00"))])  # fmt: skip
    n = _one(naive_summary(spark, raw))
    assert n.ConvertedLeads == 1000 and n.ConversionRate is None


def test_where_on_the_left_join_drops_spend_without_leads_and_null_status_leads(spark):
    """`l.LeadStatus <> 'DEAD'` in WHERE: a NULL status fails it, and a spend row with no
    lead (all lead columns NULL) is dropped, so the LEFT JOIN acts as an inner join."""
    raw = _raw(spark, leads=[("L1", "UK", "TV", None, MAY), ("L2", "UK", "TV", "NEW", MAY),
                             ("L3", "UK", "TV", "DEAD", MAY)],
               spend=[(date(2026, 5, 1), "UK", "TV", Decimal("100.00")),
                      (date(2026, 5, 1), "UK", "RADIO", Decimal("50.00"))])  # fmt: skip
    n = naive_summary(spark, raw)
    assert _one(n).TotalLeads == 1  # only L2: NULL and DEAD both removed
    assert n.where("ChannelCode = 'RADIO'").count() == 0  # spend with no lead: gone
    gold = _gold(spark, spend=[(date(2026, 5, 1), "UK", "TV", "100.00"),
                               (date(2026, 5, 1), "UK", "RADIO", "50.00")],
                 leads=[(2, "UK", "TV", MAY)])  # fmt: skip
    p = ported_summary(spark, gold)
    assert _one(p, channel="RADIO").TotalLeads == 0  # kept, with its spend


def test_channel_join_is_exact_in_spark_but_not_in_sql_server(spark):
    """SQL Server's default collation is case-insensitive and ignores trailing spaces, so
    'DISPLAY ' = 'display' = 'DISPLAY' there. A literal Spark port matches nothing."""
    raw = _raw(spark, leads=[("L1", "UK", "display", "NEW", MAY)],
               spend=[(date(2026, 5, 1), "UK", "DISPLAY ", Decimal("100.00"))])  # fmt: skip
    assert naive_summary(spark, raw).count() == 0  # no match, then the WHERE drops the row
    assert spark.sql("SELECT 'DISPLAY ' = 'display' AS eq").first().eq is False
    assert spark.sql("SELECT upper(rtrim('DISPLAY ')) = upper(rtrim('display')) AS eq") \
        .first().eq is True  # what SQL Server's collation does  # fmt: skip


def test_spend_is_local_currency_and_raw_rows_are_summed_as_they_are(spark):
    """No FX: CAD spend is reported as CAD. Duplicate and minor-unit rows are summed raw."""
    day = date(2026, 5, 1)
    raw = _raw(spark, leads=[("L1", "CA", "TV", "NEW", MAY)], regions=("CA",),
               spend=[(day, "CA", "TV", Decimal("100.00")), (day, "CA", "TV", Decimal("100.00")),
                      (date(2026, 5, 2), "CA", "TV", Decimal("10000.00"))])  # fmt: skip
    assert _one(naive_summary(spark, raw), "CA").TotalSpend == Decimal("10200.00")  # CAD
    gold = _gold(spark, spend=[(day, "CA", "TV", "58.78"), (date(2026, 5, 2), "CA", "TV", "58.78")],
                 leads=[(1, "CA", "TV", MAY)])  # gold: one copy, rescaled, in GBP  # fmt: skip
    assert _one(ported_summary(spark, gold), "CA").TotalSpendGBP == Decimal("117.56")


def test_spend_window_is_90_days_from_as_of_but_leads_are_all_time(spark):
    """Spend: SpendDate >= as_of - 90 days, no upper bound. Leads: no date filter at all.
    A time of day on as_of (GETDATE() at 07:00) drops the first day of the window."""
    raw = _raw(spark, leads=[("OLD", "UK", "TV", "NEW", "2025-12-01 10:00:00"),
                             ("NOW", "UK", "TV", "NEW", MAY)],
               spend=[(date(2026, 4, 1), "UK", "TV", Decimal("40.00")),
                      (date(2026, 6, 30), "UK", "TV", Decimal("60.00"))])  # fmt: skip
    n = _one(naive_summary(spark, raw))
    assert (n.TotalSpend, n.TotalLeads) == (Decimal("100.00"), 2)  # December lead counted
    assert DEFAULT_AS_OF == "2026-06-30 00:00:00"
    late = _one(naive_summary(spark, raw, as_of="2026-06-30 07:00:00"))
    assert late.TotalSpend == Decimal("60.00")  # 1 April fell out of the window
    gold = _gold(spark, spend=[(date(2026, 4, 1), "UK", "TV", "40.00"),
                               (date(2026, 6, 30), "UK", "TV", "60.00")],
                 leads=[(1, "UK", "TV", "2025-12-01 10:00:00"), (2, "UK", "TV", MAY)])  # fmt: skip
    p = _one(ported_summary(spark, gold))
    assert (p.TotalSpendGBP, p.TotalLeads) == (Decimal("100.00"), 1)  # same window for both


def test_duplicate_leads_and_policies_count_twice_naive_once_ported(spark):
    """Raw duplicates: the same person twice and a policy row twice. NTU never counts."""
    raw = _raw(spark, leads=[("L1", "UK", "TV", "NEW", MAY), ("L1-DUP", "UK", "TV", "NEW", MAY)],
               policies=[("P1", "L1", "INFORCE"), ("P1", "L1", "INFORCE"),
                         ("P2", "L1-DUP", "NTU")],
               spend=[(date(2026, 5, 1), "UK", "TV", Decimal("100.00"))])  # fmt: skip
    n = _one(naive_summary(spark, raw))
    assert (n.TotalLeads, n.ConvertedLeads) == (2, 2)  # 2 leads; P1 counted twice
    gold = _gold(spark, spend=[(date(2026, 5, 1), "UK", "TV", "100.00")],
                 leads=[(1, "UK", "TV", MAY)], policies=[(1, True)])  # fmt: skip
    p = _one(ported_summary(spark, gold))
    assert (p.TotalLeads, p.ConvertedLeads) == (1, 1)


def test_region_parameter_replaces_the_cursor(spark):
    """The cursor loops over RefRegion (or the one @RegionCode); the port filters once."""
    leads = [("L1", "UK", "TV", "NEW", MAY), ("L2", "CA", "TV", "NEW", MAY)]
    spend = [(date(2026, 5, 1), "UK", "TV", Decimal("1.00")),
             (date(2026, 5, 1), "CA", "TV", Decimal("1.00"))]  # fmt: skip
    raw = _raw(spark, leads=leads, spend=spend, regions=("UK", "CA"))
    assert naive_summary(spark, raw).count() == 2
    assert [r.RegionCode for r in naive_summary(spark, raw, region="CA").collect()] == ["CA"]


# ------------------------------------------------------------------ real data


@pytest.fixture(scope="module")
def legacy(spark, real_gold_all):
    from src.legacy_port import register_results

    config, _ = real_gold_all
    register_results(spark, config)
    return config


def _finding(spark, name):
    return run_finding(spark, FINDINGS, name)


@pytest.mark.slow
def test_real_row_counts_and_dropped_groups(spark, legacy):
    r = _finding(spark, "legacy_row_counts")[0]
    # 18 region x channel spend groups; the WHERE-on-LEFT-JOIN drops none on this data.
    assert (r.naive_rows, r.ported_rows, r.spend_groups, r.groups_without_leads) == (18, 18, 18, 0)


@pytest.mark.slow
def test_real_fan_out_inflates_calls_and_conversion(spark, legacy):
    r = _finding(spark, "legacy_fan_out")[0]
    assert (r.naive_total_leads, r.ported_total_leads) == (47_706, 47_201)
    assert (r.naive_total_calls, r.ported_total_calls) == (76_090, 61_690)  # join rows vs calls
    assert (r.naive_converted, r.ported_converted) == (8_001, 4_499)  # once per joined row
    # Inflated, but not above 100% on this data: the largest naive rate is 34.7%.
    assert (r.max_naive_rate_unbounded, r.max_ported_rate) == (0.347, Decimal("0.186"))
    assert r.groups_over_100_pct == 0
    # Two groups have ConvertedLeads > 999: SQL Server raises error 8115 on the
    # DECIMAL(9,6) cast (the procedure fails); the literal Spark port returns NULL.
    assert r.groups_cast_overflow == r.naive_null_rates == 2


@pytest.mark.slow
def test_real_cpl_factor_uk(spark, legacy):
    r = _finding(spark, "legacy_cpl_factor")[0]
    assert r.uk_divisor_factor == 0.621  # CPL divides by join rows: x 0.621 from that alone
    assert (r.uk_min_cpl_ratio, r.uk_max_cpl_ratio) == (Decimal("0.609"), Decimal("21.547"))


@pytest.mark.slow
def test_real_collation_inputs_are_clean_but_campaigns_are_not(spark, legacy):
    r = _finding(spark, "legacy_channel_collation")[0]
    assert (r.spend_rows_not_canonical, r.lead_rows_not_canonical) == (0, 0)
    assert r.groups_lost_to_exact_join == 0  # latent on this data
    assert r.campaign_rows_not_canonical == 9  # the variants exist upstream


@pytest.mark.slow
def test_real_spend_and_period_differences(spark, legacy):
    r = _finding(spark, "legacy_spend_and_period")[0]
    assert (r.naive_spend_rows, r.gold_spend_rows) == (2_224, 2_001)
    assert r.spend_rows_in_window_on_2026_11_25 == 0  # run on submission day: empty report
    assert r.spend_rows_when_run_at_0700 == 2_199  # GETDATE() at 07:00 drops 1 April (25)
    assert r.naive_leads_outside_window == 10  # leads are all-time
    assert (r.dead_leads, r.null_status_leads) == (0, 0)


@pytest.mark.slow
def test_real_spend_by_region_is_local_and_uncleaned(spark, legacy):
    rows = {r.RegionCode: r for r in _finding(spark, "legacy_spend_by_region")}
    assert {k: r.naive_over_gold_local for k, r in rows.items()} == {
        "AU": Decimal("16.129"), "CA": Decimal("1.380"), "UK": Decimal("13.434"),
        "US": Decimal("1.013"),
    }  # fmt: skip
    assert {k: r.currency for k, r in rows.items()} == {
        "AU": "AUD", "CA": "CAD", "UK": "GBP", "US": "USD"
    }  # fmt: skip


@pytest.mark.slow
def test_real_duplicates_the_naive_version_counts(spark, legacy):
    r = _finding(spark, "legacy_duplicates")[0]
    assert (r.raw_distinct_lead_ids, r.surviving_leads) == (47_997, 47_201)
    assert (r.raw_call_rows, r.raw_distinct_calls) == (64_000, 63_460)
    assert (r.raw_linked_policy_rows, r.raw_linked_distinct_policies) == (4_521, 4_500)
