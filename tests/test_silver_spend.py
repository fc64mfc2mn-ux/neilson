"""Step 9: silver.marketing_spend (defects 9 to 12, and 28)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.dq import CHECK_DEFECTS, read_dq_exceptions
from src.common.findings import run_finding
from src.common.tables import read_table
from src.silver.spend import (
    build_spend,
    classify_minor_units,
    clicks_exceed_impressions,
    outside_campaign_dates,
    to_gbp,
)

FINDINGS = Path(__file__).resolve().parents[1] / "src" / "sql" / "findings_spend.sql"
ROW = (
    "campaign_id string, currency_code string, impressions long, clicks double, spend_local double"
)


# ------------------------------------------------------------------ unit


def test_minor_unit_rule_rescales_fits_and_quarantines_the_ambiguous(spark):
    normal = [(f"N{i}", "GBP", 1000, 50.0, 20.0 + i) for i in range(5)]  # CPM 20 to 24
    df = spark.createDataFrame(
        normal
        + [
            ("pence", "GBP", 1000, 50.0, 2200.0),  # 100x the median CPM; /100 fits
            ("odd", "GBP", 1000, 50.0, 660.0),  # 30x; /100 = 0.3x, fits neither
            ("spike", "GBP", 1000, 50.0, 60.0),  # 2.7x: below threshold, left alone
            ("zero", "GBP", 1000, 50.0, 0.0),  # not evaluated
            ("aud", "AUD", 1000, 50.0, 22.0),  # medians are per currency
        ],
        ROW,
    )
    out = {r.campaign_id: r.minor_unit_action for r in classify_minor_units(df, 20, (0.5, 2.0))
           .collect()}  # fmt: skip
    assert out["pence"] == "rescale" and out["odd"] == "ambiguous"
    assert {out[k] for k in ("spike", "zero", "aud", "N0", "N4")} == {None}


def test_clicks_exceed_impressions(spark):
    df = spark.createDataFrame(
        [("a", "GBP", 4482, 4494.0, 3128.6), ("b", "GBP", 4482, 100.0, 90.0),
         ("c", "GBP", 4482, None, 90.0)],
        ROW,
    )  # fmt: skip
    assert [r.campaign_id for r in clicks_exceed_impressions(df).collect()] == ["a"]


def test_to_gbp_uses_the_months_rate_and_leaves_missing_rates_null(spark):
    spend = spark.createDataFrame(
        [(date(2026, 4, 3), "AUD", 100.0), (date(2026, 6, 9), "CAD", 100.0),
         (date(2026, 5, 1), "GBP", 12.34)],
        "spend_date date, currency_code string, spend_local double",
    )  # fmt: skip
    fx = spark.createDataFrame(
        [("2026-04", "AUD", 0.512), ("2026-05", "GBP", 1.0), ("2026-05", "CAD", 0.587781)],
        "rate_month string, currency_code string, rate_to_gbp double",
    )
    out = {r.currency_code: r for r in to_gbp(spend, fx).collect()}
    assert (float(out["AUD"].spend_gbp), float(out["GBP"].spend_gbp)) == (51.2, 12.34)
    assert out["CAD"].rate_to_gbp is None and out["CAD"].spend_gbp is None


def test_outside_campaign_dates(spark):
    spend = spark.createDataFrame(
        [("C1", date(2026, 4, 1)), ("C1", date(2026, 4, 15)), ("C1", date(2026, 5, 2))],
        "campaign_id string, spend_date date",
    )
    campaigns = spark.createDataFrame(
        [("C1", date(2026, 4, 2), date(2026, 4, 30))],
        "campaign_id string, start_date date, end_date date",
    )
    out = {r.spend_date: r.detail for r in outside_campaign_dates(spend, campaigns).collect()}
    assert out == {
        date(2026, 4, 1): "before start 2026-04-02",
        date(2026, 5, 2): "after end 2026-04-30",
    }


def test_spend_checks_are_mapped():
    assert {
        "spend_exact_duplicate": 10,
        "spend_negative": 10,
        "spend_zero_paid_channel": 10,
        "spend_clicks_missing": 10,
        "spend_minor_units_rescaled": 11,
        "spend_minor_units_ambiguous": 11,
        "spend_fx_rate_missing": 12,
        "spend_outside_campaign_dates": 9,
        "spend_clicks_exceed_impressions": 28,
    }.items() <= CHECK_DEFECTS.items()


# ------------------------------------------------------------------ real data


@pytest.fixture(scope="module")
def real_spend(spark, real_leads):
    config, _, _ = real_leads
    return config, build_spend(spark, config)


def _checks(spark, config, name):
    return read_dq_exceptions(spark, config).where(F.col("check_name") == name)


@pytest.mark.slow
def test_spend_rows_reconcile_every_removed_row(spark, real_spend):
    config, rec = real_spend
    # 2,224 = 2,001 + 40 exact copies + 180 CAD June rows (no rate) + 3 negatives.
    assert (rec.rows_in, rec.rows_out, rec.rows_quarantined) == (2_224, 2_001, 223)
    assert rec.removed == {
        "spend_exact_duplicate": 40, "spend_fx_rate_missing": 180, "spend_negative": 3
    }  # fmt: skip
    assert rec.rows_in == rec.rows_out + rec.rows_quarantined
    assert read_table(spark, config, "silver.marketing_spend").count() == 2_001
    r = run_finding(spark, FINDINGS, "spend_row_reconciliation")[0]
    assert (r.bronze_rows, r.exact_copies, r.cad_june_distinct, r.negatives) == (
        2_224, 40, 180, 3
    )  # fmt: skip
    # Earlier notes said "4 missing FX rows": not supported. CAD June has 187 bronze rows,
    # 7 of them copies (counted in the 40), so 180 distinct rows have no rate.
    assert (r.cad_june_rows, r.cad_june_copies) == (187, 7)
    assert r.bronze_rows - r.exact_copies - r.cad_june_distinct - r.negatives == 2_001


@pytest.mark.slow
def test_spend_dq_counts(spark, real_spend):
    config, _ = real_spend
    expect = {
        "spend_exact_duplicate": (40, "info", "quarantined"),
        "spend_fx_rate_missing": (180, "error", "quarantined"),
        "spend_negative": (3, "warning", "quarantined"),
        "spend_zero_paid_channel": (109, "info", "flagged"),
        "spend_clicks_missing": (17, "info", "flagged"),
        "spend_minor_units_rescaled": (181, "warning", "corrected"),
        "spend_clicks_exceed_impressions": (6, "warning", "flagged"),
        "spend_outside_campaign_dates": (325, "info", "flagged"),
    }
    for check, (rows, severity, action) in expect.items():
        got = _checks(spark, config, check)
        assert got.count() == rows, check
        assert {(r.severity, r.action) for r in got.select("severity", "action").distinct()
                .collect()} == {(severity, action)}, check  # fmt: skip
    assert _checks(spark, config, "spend_minor_units_ambiguous").count() == 0


@pytest.mark.slow
def test_silver_spend_content(spark, real_spend):
    config, _ = real_spend
    s = read_table(spark, config, "silver.marketing_spend").cache()
    assert s.where("spend_gbp IS NULL OR rate_to_gbp IS NULL OR spend_local < 0").count() == 0
    assert s.where("currency_code = 'CAD' AND spend_date >= '2026-06-01'").count() == 0
    assert s.groupBy("spend_date", "campaign_id").count().where("count > 1").count() == 0
    rescaled = s.where("minor_unit_rescaled")
    assert rescaled.count() == 181
    assert {r.campaign_id for r in rescaled.select("campaign_id").distinct().collect()} == {
        "CMP-1061", "CMP-1062"
    }  # fmt: skip
    assert rescaled.where("spend_local * 100 != spend_local_raw").count() == 0
    assert s.where("outside_campaign_dates").count() == 325
    assert s.where("clicks IS NULL").count() == 17  # kept null, never 0
    s.unpersist()


@pytest.mark.slow
def test_finding_minor_unit_evidence(spark, real_spend):
    rows = {r.campaign_id: r for r in run_finding(spark, FINDINGS, "minor_unit_evidence")}
    # Every campaign but two has a median CPM 0.88 to 1.05x its currency's median.
    normal = [r for k, r in rows.items() if k not in ("CMP-1061", "CMP-1062")]
    assert all(0.85 < r.median_cpm_ratio < 1.1 for r in normal)
    # CMP-1061 (AUD, 92x: its own rows lift the AUD median) and CMP-1062 (GBP, 99.8x);
    # divided by 100 they land in the normal range.
    for k in ("CMP-1061", "CMP-1062"):
        assert 90 < rows[k].median_cpm_ratio < 101
        assert 0.85 < rows[k].median_cpm_ratio / 100 < 1.1
        # CPC is ~100x too, while click-through is normal: spend is the outlier.
        assert 0.8 < rows[k].median_ctr_ratio < 1.2
    r = run_finding(spark, FINDINGS, "minor_unit_row_bands")[0]
    # Row CPM ratio: normal rows 0.68 to 1.52; minor-unit rows 68 to 143; nothing between
    # 2 and 20 except the 6 rows whose clicks exceed impressions.
    assert (r.normal_rows, r.minor_rows, r.between_rows) == (1_614, 181, 0)
    assert r.normal_max < 2 and r.minor_min > 20 and r.minor_min / 100 > 0.5


@pytest.mark.slow
def test_finding_cpl_before_and_after_rescale(spark, real_spend):
    rows = {r.scope: r for r in run_finding(spark, FINDINGS, "cpl_minor_unit_effect")}
    for k in ("CMP-1061", "CMP-1062"):
        assert round(rows[k].cpl_before_gbp / rows[k].cpl_after_gbp) == 100
    # GBP cost per lead: CMP-1061 9,691 -> 96.91; CMP-1062 3,161 -> 31.61; all campaigns
    # 319.95 -> 34.39 (46,897 leads). Left in minor units, blended CPL is 9.3x too high.
    assert round(rows["CMP-1061"].cpl_after_gbp, 2) == 96.91
    assert round(rows["CMP-1062"].cpl_after_gbp, 2) == 31.61
    assert (round(rows["ALL"].cpl_before_gbp, 2), round(rows["ALL"].cpl_after_gbp, 2)) == (
        319.95, 34.39
    )  # fmt: skip
    assert rows["ALL"].leads == 46_897


@pytest.mark.slow
def test_finding_gbp_reconciles_by_currency_and_month(spark, real_spend):
    rows = run_finding(spark, FINDINGS, "spend_gbp_by_currency_month")
    assert len(rows) == 11  # 4 currencies x 3 months, less CAD June
    for r in rows:
        # Row-level rounding only: |sum(local) x rate - sum(gbp)| <= half a penny per row.
        assert abs(float(r.local_x_rate) - float(r.spend_gbp)) <= 0.005 * r.rows, r
        if r.currency_code == "GBP":
            assert r.spend_gbp == r.spend_local


@pytest.mark.slow
def test_finding_negative_zero_and_null_clicks(spark, real_spend):
    r = run_finding(spark, FINDINGS, "negative_zero_null_evidence")[0]
    # Negatives: none offsets a positive of the same campaign, each is that campaign's
    # only row that day, and each is a fraction of its campaign's median.
    assert (r.negatives, r.negatives_with_offset, r.max_negative_vs_median) == (3, 0, 0.3)
    # Zeros: 91 ORGANIC (no media cost by nature), the rest on tiny delivery.
    assert (r.zero_rows, r.zero_organic, r.zero_max_impressions) == (200, 91, 491)
    # Null clicks: impressions and spend present on every one.
    assert (r.null_click_rows, r.null_click_no_impressions) == (17, 0)


@pytest.mark.slow
def test_finding_clicks_exceed_impressions(spark, real_spend):
    r = run_finding(spark, FINDINGS, "clicks_exceed_impressions")[0]
    # Spend on these rows is normal for the campaign; the impressions value is the error.
    assert (r.rows, r.max_spend_vs_campaign_median) == (6, 1.2)


@pytest.mark.slow
def test_clicks_exceed_impressions_rows_are_not_rescaled(spark, real_spend):
    """Defect 28 rows are excluded from the minor-unit rule: none of the 6 is rescaled."""
    config, _ = real_spend
    s = read_table(spark, config, "silver.marketing_spend")
    assert s.where("clicks > impressions").count() == 6
    assert s.where("clicks > impressions AND minor_unit_rescaled").count() == 0
