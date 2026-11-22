"""A USD-cents row converts to the same GBP value as the equivalent
USD-dollar row, and a month with no rate raises or quarantines rather than
producing null silently."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from src.silver.spend import (
    DEFAULT_MINOR_UNITS,
    classify_minor_units,
    rescale_minor_units,
    split_missing_rate,
    to_gbp,
)

MAY, JUNE = date(2026, 5, 10), date(2026, 6, 10)
SPEND = "spend_date date, campaign_id string, currency_code string, spend_local double, " \
        "impressions long, clicks long"  # fmt: skip
FX = "rate_month string, currency_code string, rate_to_gbp decimal(18,6)"


def test_fx_conversion_cents_and_missing_rate(spark):
    rule = DEFAULT_MINOR_UNITS
    normal = [(MAY, f"N{i}", "USD", 1000.0, 100_000, 900) for i in range(5)]  # CPM 10
    spend = spark.createDataFrame(
        [
            *normal,
            (MAY, "DOLLARS", "USD", 800.0, 80_000, 700),  # $800.00
            (MAY, "CENTS", "USD", 80_000.0, 80_000, 700),  # the same $800.00, in cents
            (JUNE, "CAD_JUNE", "CAD", 500.0, 50_000, 400),  # no CAD rate for June
        ],
        SPEND,
    )
    fx = spark.createDataFrame(
        [("2026-05", "USD", Decimal("0.795463")), ("2026-06", "USD", Decimal("0.798484"))], FX
    )
    # The pipeline's order: rate check first, then the minor-unit rule, then GBP.
    with_rate, missing = split_missing_rate(to_gbp(spend, fx))
    classified = classify_minor_units(
        with_rate, rule["ratio_threshold"], tuple(rule["rescaled_band"]), rule["divisor"]
    )
    rescaled = rescale_minor_units(classified, rule["divisor"]).drop("rate_to_gbp", "spend_gbp")
    gbp = to_gbp(rescaled.withColumn("spend_local", F.col("spend_local").cast("decimal(14,2)")), fx)
    rows = {r.campaign_id: r for r in gbp.collect()}

    cents, dollars = rows["CENTS"], rows["DOLLARS"]
    assert cents.minor_unit_rescaled is True and dollars.minor_unit_rescaled is False
    assert cents.spend_local_raw == Decimal("80000.00")  # the source value is kept
    assert cents.spend_gbp == dollars.spend_gbp == Decimal("636.37")  # 800 x 0.795463

    # The month with no rate is split out for quarantine (error, defect 12), never kept
    # with a null GBP value.
    assert [r.campaign_id for r in missing.collect()] == ["CAD_JUNE"]
    assert "CAD_JUNE" not in rows
    assert gbp.where(F.col("spend_gbp").isNull()).count() == 0
