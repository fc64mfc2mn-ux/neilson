"""Build the gold dimension tables (step 10).

Produces `gold.dim_date` (2026-01-01 to 2026-12-31, calendar quarters as
`fiscal_quarter`, assumption 12), `gold.dim_region` (from ref_region, with
`timezone`, `gos_target_seconds` and the region group of assumption 1),
`gold.dim_channel` and `gold.dim_campaign` (SCD1, each plus an `UNKNOWN`
member), `gold.dim_product` (`TERM_LIFE`, `OVER50`, `CI`) and `gold.dim_agent`
(SCD2: one row per agent per validity window, plus an `UNKNOWN` member).
`gold.dim_lead` is built at step 11 (src/gold/apply_pii_policy.py).

Surrogate keys are deterministic: the first 60 bits of SHA-256 over the
natural key (positive bigint), so a rerun, or a new member, never moves an
existing key. `UNKNOWN` members have key -1. `date_key` is yyyymmdd.

SCD2 windows (assumption 8): the roster has no dates. Per agent, versions are
ordered by bronze `_source_row`. A single version is valid from 2026-01-01 to
9999-12-31. With two, the earlier is valid 2026-01-01 to 2026-03-31 and the
later from 2026-04-01 (the reporting period start). `valid_to` is inclusive.
Three or more versions have no rule and stop the stage.

Employee contact and HR fields (`agent_email`, `manager_name`, `hire_date`,
`fte`) stay in silver; gold keeps what reporting needs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.dq import ReconciliationError
from src.common.period import REPORTING_END, REPORTING_START
from src.common.tables import overwrite_table, read_table

log = logging.getLogger("northgate.gold")

UNKNOWN_KEY = -1
UNKNOWN_CODE = "UNKNOWN"
CALENDAR_START = date(2026, 1, 1)
CALENDAR_END = date(2026, 12, 31)
OPEN_END = date(9999, 12, 31)
PRODUCT_NAMES = {"TERM_LIFE": "Term life", "OVER50": "Over 50s life", "CI": "Critical illness"}
REGION_GROUPS = {"UK": "UK and Ireland"}  # everything else: International (assumption 1)


@dataclass(frozen=True)
class DimResult:
    table: str
    source: str
    rows_in: int
    rows_out: int
    unknown_members: int


@dataclass(frozen=True)
class Coverage:
    rows: int
    matched: int
    unmatched: int
    null: int


# ------------------------------------------------------------------ helpers


def surrogate_key(*cols: str | Column) -> Column:
    """Positive bigint from SHA-256 of the natural key: stable across reruns."""
    parts = [F.coalesce(F.col(c) if isinstance(c, str) else c, F.lit("")).cast("string")
             for c in cols]  # fmt: skip
    digest = F.sha2(F.concat_ws("|", *parts), 256)
    return F.conv(F.substring(digest, 1, 15), 16, 10).cast("bigint")


def _with_unknown(df: DataFrame, key: str, code: str, labels: dict[str, str]) -> DataFrame:
    """Append the UNKNOWN member: key -1, code UNKNOWN, labels given, other columns null."""
    spark = df.sparkSession
    values = {key: F.lit(UNKNOWN_KEY), code: F.lit(UNKNOWN_CODE)}
    values.update({c: F.lit(v) for c, v in labels.items()})
    row = spark.range(1).select(
        *[values.get(f.name, F.lit(None)).cast(f.dataType).alias(f.name) for f in df.schema]
    )
    return df.unionByName(row)


def key_coverage(fact: DataFrame, fact_col: str, dim: DataFrame, dim_col: str) -> Coverage:
    """How many fact rows find their key in the dimension (nulls counted apart)."""
    keys = dim.select(F.col(dim_col).alias("_k")).distinct()
    tagged = fact.select(F.col(fact_col).alias("_k")).join(
        keys.withColumn("_hit", F.lit(True)), "_k", "left"
    )
    r = tagged.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("_hit").isNotNull().cast("int")).alias("matched"),
        F.sum((F.col("_hit").isNull() & F.col("_k").isNotNull()).cast("int")).alias("unmatched"),
        F.sum(F.col("_k").isNull().cast("int")).alias("null"),
    ).first()
    return Coverage(r.rows, r.matched or 0, r.unmatched or 0, r.null or 0)


# ------------------------------------------------------------------ dimensions


def build_dim_date(spark: SparkSession) -> DataFrame:
    days = (CALENDAR_END - CALENDAR_START).days
    dates = spark.range(days + 1).select(
        F.date_add(F.lit(CALENDAR_START), F.col("id").cast("int")).alias("date")
    )
    quarter = F.quarter("date")
    return dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        quarter.alias("quarter"),
        F.concat(F.lit("Q"), quarter, F.lit(" "), F.year("date")).alias("fiscal_quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "MMM").alias("month_short"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.trunc("date", "month").alias("month_start_date"),
        F.dayofmonth("date").alias("day_of_month"),
        ((F.dayofweek("date") + 5) % 7 + 1).alias("day_of_week"),  # ISO: Monday = 1
        F.date_format("date", "EEEE").alias("day_name"),
        (F.dayofweek("date").between(2, 6)).alias("is_weekday"),
        F.weekofyear("date").alias("iso_week"),
        F.col("date")
        .between(F.lit(REPORTING_START), F.lit(REPORTING_END))
        .alias("is_reporting_period"),
    )


def dim_region(ref_region: DataFrame) -> DataFrame:
    group = F.create_map(*[F.lit(x) for kv in REGION_GROUPS.items() for x in kv])
    return ref_region.select(
        surrogate_key("region_code").alias("region_key"),
        "region_code",
        "region_name",
        F.coalesce(group[F.col("region_code")], F.lit("International")).alias("region_group"),
        "currency_code",
        "timezone",
        "business_hours_start",
        "business_hours_end",
        F.col("gos_target_seconds").cast("int").alias("gos_target_seconds"),
        F.col("sla_first_call_minutes").cast("int").alias("sla_first_call_minutes"),
    )


def dim_channel(ref_channel: DataFrame) -> DataFrame:
    df = ref_channel.select(
        surrogate_key("channel_code").alias("channel_key"),
        "channel_code",
        "channel_name",
        "channel_group",
        "paid_flag",
        "attribution_window_days",
    )
    return _with_unknown(df, "channel_key", "channel_code",
                         {"channel_name": "Unknown", "channel_group": "Unknown"})  # fmt: skip


def dim_campaign(campaign: DataFrame) -> DataFrame:
    df = campaign.select(
        surrogate_key("campaign_id").alias("campaign_key"),
        "campaign_id",
        "campaign_name",
        "channel_code",
        "region_code",
        "product_code",
        "objective",
        "start_date",
        "end_date",
        "end_before_start",
    )
    return _with_unknown(df, "campaign_key", "campaign_id", {"campaign_name": "Unknown campaign"})


def dim_product(codes: DataFrame) -> DataFrame:
    names = F.create_map(*[F.lit(x) for kv in PRODUCT_NAMES.items() for x in kv])
    return codes.select(
        surrogate_key("product_code").alias("product_key"),
        "product_code",
        F.coalesce(names[F.col("product_code")], F.col("product_code")).alias("product_name"),
    )


def dim_agent(agent: DataFrame) -> DataFrame:
    """SCD2 windows from `source_row` order (assumption 8), plus UNKNOWN."""
    by_agent = Window.partitionBy("agent_id")
    ordered = by_agent.orderBy("source_row")
    versioned = agent.withColumn("_n", F.count(F.lit(1)).over(by_agent)).withColumn(
        "_v", F.row_number().over(ordered)
    )
    too_many = versioned.where("_n > 2").select("agent_id", "_n").distinct().collect()
    if too_many:
        raise ReconciliationError(
            "silver.agent: no SCD2 rule for "
            + ", ".join(f"{r.agent_id} with {r._n} versions" for r in too_many)
        )
    split = F.col("_n") == 2
    valid_from = F.when(split & (F.col("_v") == 2), F.lit(REPORTING_START)).otherwise(
        F.lit(CALENDAR_START)
    )
    last_day_before = REPORTING_START - timedelta(days=1)
    valid_to = F.when(split & (F.col("_v") == 1), F.lit(last_day_before)).otherwise(F.lit(OPEN_END))
    df = (
        versioned.withColumn("valid_from", valid_from)
        .withColumn("valid_to", valid_to)
        .select(
            surrogate_key("agent_id", "valid_from").alias("agent_key"),
            "agent_id",
            "agent_name",
            "region_code",
            "team",
            "seniority_band",
            "active_flag",
            "valid_from",
            "valid_to",
            (F.col("_v") == F.col("_n")).alias("is_current"),
            F.col("source_row").cast("long").alias("source_row"),
        )
    )
    unknown = _with_unknown(df, "agent_key", "agent_id", {"agent_name": "Unknown agent"})
    return (
        unknown.withColumn(
            "valid_from",
            F.when(F.col("agent_key") == UNKNOWN_KEY, F.lit(CALENDAR_START)).otherwise(
                F.col("valid_from")
            ),
        )
        .withColumn(
            "valid_to",
            F.when(F.col("agent_key") == UNKNOWN_KEY, F.lit(OPEN_END)).otherwise(F.col("valid_to")),
        )
        .withColumn(
            "is_current",
            F.when(F.col("agent_key") == UNKNOWN_KEY, F.lit(True)).otherwise(F.col("is_current")),
        )
    )


def resolve_agent(calls: DataFrame, dim: DataFrame) -> DataFrame:
    """Add `agent_key`: the version valid on the call's date, else UNKNOWN (-1)."""
    versions = dim.select(
        F.col("agent_id").alias("_aid"), F.col("agent_key").alias("_akey"),
        F.col("valid_from").alias("_from"), F.col("valid_to").alias("_to"),
    )  # fmt: skip
    day = F.to_date("call_start_ts_utc")
    joined = calls.join(
        versions,
        (F.col("agent_id") == F.col("_aid")) & day.between(F.col("_from"), F.col("_to")),
        "left",
    )
    return joined.withColumn("agent_key", F.coalesce("_akey", F.lit(UNKNOWN_KEY))).drop(
        "_aid", "_akey", "_from", "_to"
    )


# ------------------------------------------------------------------ builder


def build_dimensions(spark: SparkSession, config: dict) -> list[DimResult]:
    t = lambda name: read_table(spark, config, name)  # noqa: E731
    codes = (
        t("silver.lead").select(F.col("product_interest").alias("product_code"))
        .union(t("silver.campaign").select("product_code"))
        .union(t("silver.policy").select("product_code"))
        .where(F.col("product_code").isNotNull())
        .distinct()
    )  # fmt: skip
    plan = [
        ("gold.dim_date", "generated 2026 calendar", None, build_dim_date(spark), 0),
        ("gold.dim_region", "silver.ref_region", t("silver.ref_region"),
         dim_region(t("silver.ref_region")), 0),
        ("gold.dim_channel", "silver.ref_channel", t("silver.ref_channel"),
         dim_channel(t("silver.ref_channel")), 1),
        ("gold.dim_campaign", "silver.campaign", t("silver.campaign"),
         dim_campaign(t("silver.campaign")), 1),
        ("gold.dim_product", "distinct product codes (lead, campaign, policy)", codes,
         dim_product(codes), 0),
        ("gold.dim_agent", "silver.agent", t("silver.agent"), dim_agent(t("silver.agent")), 1),
    ]  # fmt: skip
    results = []
    for table, source, silver, gold, unknown in plan:
        overwrite_table(spark, gold, config, table)
        rows_out = t(table).count()
        rows_in = rows_out if silver is None else silver.count()
        if rows_out != rows_in + unknown:
            raise ReconciliationError(
                f"{table}: {rows_out} rows != {rows_in} from {source} + {unknown} UNKNOWN"
            )
        results.append(DimResult(table, source, rows_in, rows_out, unknown))
        log.info("Reconciled %s: %d from %s + %d UNKNOWN", table, rows_in, source, unknown)
    return results
