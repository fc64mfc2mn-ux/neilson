"""Silver spend: silver.marketing_spend from bronze.marketing_spend_raw.

Order on the latest bronze batch of marketing_spend_daily.csv:
1. Exact duplicates (defect 10): repeats of a byte-identical row, quarantined, info.
2. Missing FX rate (defect 12, assumption 7): spend whose currency has no rate
   for its month (CAD June) is quarantined, error. Never a null or guessed rate.
3. Negative spend (defect 10, assumption 21): no negative offsets a positive row
   of its campaign, so none is an adjustment; quarantined, warning.
4. Minor units (defect 11, assumption 6), on positive rows whose clicks do not
   exceed impressions: a row's CPM (spend per 1,000 impressions) against its
   currency's median CPM. At or above `ratio_threshold` (20x), the row is
   rescaled (divided by 100) if the rescaled ratio falls in `rescaled_band`
   (0.5 to 2x), action corrected; otherwise it is ambiguous and quarantined.
5. Flags that keep the row: zero spend on a paid channel (ORGANIC has no media
   cost by nature), null clicks (kept null, never 0), clicks above impressions
   (defect 28: the impressions are wrong, the spend is not), spend outside the
   campaign's dates (defect 9: the spend still counts).
6. GBP at the month's rate: spend_gbp = round(spend_local x rate_to_gbp, 2).

Reconciliation: rows in = silver.marketing_spend rows + rows quarantined.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.bronze.ingest import latest_batch
from src.common.dq import assert_reconciled, quarantined_count, write_dq_exception
from src.common.tables import overwrite_table, read_table
from src.silver.reference import conform_campaign_id, normalise_channel

log = logging.getLogger("northgate.silver")

SPEND_SOURCE = "bronze.marketing_spend_raw"
DEFAULT_MINOR_UNITS = {"ratio_threshold": 20.0, "rescaled_band": [0.5, 2.0], "divisor": 100}
NO_MEDIA_COST_CHANNELS = ("ORGANIC",)


@dataclass(frozen=True)
class SpendReconciliation:
    rows_in: int
    removed: dict[str, int]
    rows_quarantined: int
    rows_out: int


# ------------------------------------------------------------------ pure checks


def exact_copies(raw: DataFrame) -> DataFrame:
    first = Window.partitionBy("_row_hash").orderBy("_source_row")
    return raw.withColumn("_copy", F.row_number().over(first)).where("_copy > 1").drop("_copy")


def clicks_exceed_impressions(df: DataFrame) -> DataFrame:
    return df.where(F.col("clicks") > F.col("impressions"))


def classify_minor_units(
    df: DataFrame, ratio_threshold: float, rescaled_band: tuple[float, float], divisor: int = 100
) -> DataFrame:
    """Add `cpm_ratio` and `minor_unit_action` (`rescale`, `ambiguous` or null).

    Eligible rows: positive spend and impressions, clicks not above impressions.
    The median CPM is per currency, over eligible rows.
    """
    cpm = F.col("spend_local") / F.col("impressions") * 1000
    eligible = (
        (F.col("spend_local") > 0)
        & (F.col("impressions") > 0)
        & ~F.coalesce(F.col("clicks") > F.col("impressions"), F.lit(False))
    )
    medians = (
        df.where(eligible)
        .groupBy("currency_code")
        .agg(F.expr("percentile(spend_local / impressions * 1000, 0.5)").alias("_median_cpm"))
    )
    low, high = rescaled_band
    ratio = F.col("cpm_ratio")
    return (
        df.join(medians, "currency_code", "left")
        .withColumn("cpm_ratio", F.when(eligible, cpm / F.col("_median_cpm")))
        .withColumn(
            "minor_unit_action",
            F.when(
                ratio >= ratio_threshold,
                F.when((ratio / divisor >= low) & (ratio / divisor <= high), "rescale").otherwise(
                    "ambiguous"
                ),
            ),
        )  # fmt: skip
        .drop("_median_cpm")
    )


def to_gbp(spend: DataFrame, fx: DataFrame) -> DataFrame:
    """Join the month's rate; spend_gbp is null where the rate is missing."""
    rates = fx.select(
        F.col("rate_month").alias("_month"), F.col("currency_code").alias("_ccy"), "rate_to_gbp"
    )
    joined = spend.join(
        rates,
        (F.date_format("spend_date", "yyyy-MM") == F.col("_month"))
        & (F.col("currency_code") == F.col("_ccy")),
        "left",
    ).drop("_month", "_ccy")
    gbp = F.round(F.col("spend_local").cast("decimal(18,6)") * F.col("rate_to_gbp"), 2)
    return joined.withColumn("spend_gbp", gbp.cast("decimal(14,2)"))


def outside_campaign_dates(spend: DataFrame, campaigns: DataFrame) -> DataFrame:
    """Spend dated before its campaign's start or after its end, with `detail`."""
    dates = campaigns.select("campaign_id", F.col("start_date").alias("_start"),
                             F.col("end_date").alias("_end"))  # fmt: skip
    detail = (
        F.when(F.col("spend_date") < F.col("_start"),
               F.concat(F.lit("before start "), F.col("_start").cast("string")))
        .when(F.col("spend_date") > F.col("_end"),
              F.concat(F.lit("after end "), F.col("_end").cast("string")))
    )  # fmt: skip
    return (
        spend.join(dates, "campaign_id", "left")
        .withColumn("detail", detail)
        .where(F.col("detail").isNotNull())
        .drop("_start", "_end")
    )


def conform_spend(raw: DataFrame, codes: list[str]) -> DataFrame:
    return raw.select(
        F.to_date("spend_date").alias("spend_date"),
        "campaign_id",
        "region_code",
        normalise_channel(F.col("channel_code"), codes).alias("channel_code"),
        F.col("impressions").cast("double").cast("long").alias("impressions"),
        F.col("clicks").cast("double").cast("long").alias("clicks"),
        F.col("spend_local").cast("double").alias("spend_local"),
        "currency_code",
        F.col("_source_row").alias("source_row"),
    )


# ------------------------------------------------------------------ builder


def build_spend(spark: SparkSession, config: dict) -> SpendReconciliation:
    rule = {**DEFAULT_MINOR_UNITS, **config.get("spend", {}).get("minor_units", {})}
    raw = latest_batch(read_table(spark, config, SPEND_SOURCE)).cache()
    rows_in = raw.count()
    removed: dict[str, int] = {}

    # 1. Exact duplicates.
    copies = exact_copies(raw).withColumn(
        "detail", F.concat(F.lit("repeat at source row "), "_source_row")
    )
    removed["spend_exact_duplicate"] = write_dq_exception(
        copies, "spend_exact_duplicate", SPEND_SOURCE, ["spend_date", "campaign_id"], "info",
        config=config, detail_col="detail",
    )  # fmt: skip
    rest = raw.join(copies.select("_source_row"), "_source_row", "left_anti")

    codes = [r.channel_code for r in read_table(spark, config, "silver.ref_channel").collect()]
    campaigns = read_table(spark, config, "silver.campaign")
    spend = conform_campaign_id(conform_spend(rest, codes), campaigns)
    spend = to_gbp(spend, read_table(spark, config, "silver.fx_rate")).cache()
    key = ["spend_date", "campaign_id"]

    # 2. Missing FX rate.
    no_rate = spend.where(F.col("rate_to_gbp").isNull()).withColumn(
        "detail", F.concat_ws(" ", "currency_code", F.date_format("spend_date", "yyyy-MM"))
    )
    removed["spend_fx_rate_missing"] = write_dq_exception(
        no_rate, "spend_fx_rate_missing", SPEND_SOURCE, key, "error", config=config,
        detail_col="detail",
    )  # fmt: skip
    spend = spend.where(F.col("rate_to_gbp").isNotNull())

    # 3. Negative spend.
    negative = spend.where(F.col("spend_local") < 0).withColumn(
        "detail", F.col("spend_local").cast("string")
    )
    removed["spend_negative"] = write_dq_exception(
        negative, "spend_negative", SPEND_SOURCE, key, "warning", config=config,
        detail_col="detail",
    )  # fmt: skip
    spend = spend.where(F.col("spend_local") >= 0)

    # 4. Minor units.
    spend = classify_minor_units(
        spend, rule["ratio_threshold"], tuple(rule["rescaled_band"]), rule["divisor"]
    ).withColumn("detail", F.concat(F.lit("CPM ratio "), F.round("cpm_ratio", 1).cast("string")))
    ambiguous = spend.where(F.col("minor_unit_action") == "ambiguous")
    n_ambiguous = write_dq_exception(ambiguous, "spend_minor_units_ambiguous", SPEND_SOURCE, key,
                                     "warning", config=config, detail_col="detail")  # fmt: skip
    if n_ambiguous:
        removed["spend_minor_units_ambiguous"] = n_ambiguous
    spend = spend.where(~F.col("minor_unit_action").eqNullSafe("ambiguous"))
    rescale = F.col("minor_unit_action") == "rescale"
    write_dq_exception(spend.where(rescale), "spend_minor_units_rescaled", SPEND_SOURCE, key,
                       "warning", config=config, action="corrected",
                       detail_col="detail")  # fmt: skip
    spend = (
        spend.withColumn("minor_unit_rescaled", F.coalesce(rescale, F.lit(False)))
        .withColumn("spend_local_raw", F.col("spend_local").cast("decimal(14,2)"))
        .withColumn(
            "spend_local",
            F.when(rescale, F.col("spend_local") / rule["divisor"]).otherwise(F.col("spend_local")),
        )
        .drop("rate_to_gbp", "spend_gbp", "detail")
    )

    # 5. Flags that keep the row.
    zero_paid = spend.where(
        (F.col("spend_local") == 0) & ~F.col("channel_code").isin(*NO_MEDIA_COST_CHANNELS)
    ).withColumn("detail", F.concat_ws(" ", "channel_code", F.lit("impressions"), "impressions"))
    write_dq_exception(zero_paid, "spend_zero_paid_channel", SPEND_SOURCE, key, "info",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip
    write_dq_exception(spend.where(F.col("clicks").isNull()), "spend_clicks_missing",
                       SPEND_SOURCE, key, "info", config=config, action="flagged")  # fmt: skip
    over = clicks_exceed_impressions(spend).withColumn(
        "detail",
        F.concat_ws(" > ", F.col("clicks").cast("string"), F.col("impressions").cast("string")),
    )
    write_dq_exception(over, "spend_clicks_exceed_impressions", SPEND_SOURCE, key, "warning",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip
    outside = outside_campaign_dates(spend, campaigns)
    write_dq_exception(outside, "spend_outside_campaign_dates", SPEND_SOURCE, key, "info",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip

    # 6. GBP at the month's rate.
    silver = (
        to_gbp(spend.withColumn("spend_local", F.col("spend_local").cast("decimal(14,2)")),
               read_table(spark, config, "silver.fx_rate"))  # fmt: skip
        .join(outside.select("source_row", F.lit(True).alias("outside_campaign_dates")),
              "source_row", "left")  # fmt: skip
        .withColumn("outside_campaign_dates", F.coalesce("outside_campaign_dates", F.lit(False)))
        .select(
            "spend_date", "campaign_id", "campaign_id_raw", "region_code", "channel_code",
            "impressions", "clicks", "spend_local_raw", "spend_local", "currency_code",
            "rate_to_gbp", "spend_gbp", "minor_unit_rescaled", "outside_campaign_dates",
            "source_row",
        )
    )  # fmt: skip
    overwrite_table(spark, silver, config, "silver.marketing_spend")

    rec = SpendReconciliation(
        rows_in=rows_in,
        removed=removed,
        rows_quarantined=quarantined_count(spark, config, SPEND_SOURCE),
        rows_out=read_table(spark, config, "silver.marketing_spend").count(),
    )
    raw.unpersist()
    assert_reconciled("silver.marketing_spend", rec.rows_in, rec.rows_out, rec.rows_quarantined)
    log.info("silver.marketing_spend: %s", rec)
    return rec
