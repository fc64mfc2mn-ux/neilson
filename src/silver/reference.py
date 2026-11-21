"""Silver reference data and dimensions: channel, region, campaign, FX, agent.

Writes `silver.ref_channel`, `silver.ref_region`, `silver.campaign`,
`silver.fx_rate` and `silver.agent`, each from the latest bronze batch, and
reconciles every table: rows in = rows out + rows quarantined.

Decisions (docs/ASSUMPTIONS.md, docs/DEFECT_REGISTER.md):
- Channel labels are normalised to `ref_channel` codes (defect 8); the
  campaign's channel is the source of truth (defect 24, assumption 4).
- Orphan campaign ids on leads map to `UNKNOWN` and are flagged (defect 5).
- Campaigns ending before they start are flagged and kept (defect 9).
- A missing FX rate for a reporting month is an error, never a null (defect 12).
- Agents: both versions of a duplicated agent are kept (defect 18; dates come
  at step 10, ordered by `source_row`, the roster file position: assumption 8).
  A null region is taken from the team prefix (defect 18), and a team that
  contradicts the region is replaced by the agent's other team (defect 27).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.bronze.ingest import latest_batch
from src.common.dq import (
    StageResult,
    assert_reconciled,
    quarantined_count,
    write_dq_exception,
)
from src.common.period import REPORTING_MONTHS
from src.common.tables import overwrite_table, read_table

log = logging.getLogger("northgate.silver")

UNKNOWN_CAMPAIGN_ID = "UNKNOWN"
UNKNOWN_CHANNEL = "UNKNOWN"
# Spellings that are not just a case/punctuation variant of the code.
CHANNEL_ALIASES = {"SOCIALPAID": "PAID_SOCIAL"}


def _blank_to_null(name: str) -> Column:
    col = F.trim(F.col(name))
    return F.when(col == "", None).otherwise(col)


def normalise_channel(value: Column, channel_codes: Iterable[str]) -> Column:
    """Map a channel label to a ref_channel code, or null if it is not recognised.

    Labels are compared on letters only, upper case: `PaidSearch`, `paid_search`
    and `PAID_SEARCH` all match `PAID_SEARCH`. Known aliases are listed in
    CHANNEL_ALIASES. Anything else stays unmapped rather than guessed.
    """
    lookup = {code.replace("_", ""): code for code in channel_codes}
    lookup.update(CHANNEL_ALIASES)
    key = F.upper(F.regexp_replace(value, "[^A-Za-z]", ""))
    mapping = F.create_map(*[F.lit(x) for kv in sorted(lookup.items()) for x in kv])
    return mapping[key]


# ------------------------------------------------------------------ pure transforms


def conform_channels(raw: DataFrame) -> DataFrame:
    return raw.select(
        _blank_to_null("channel_code").alias("channel_code"),
        F.col("channel_name"),
        F.col("channel_group"),
        (F.col("paid_flag") == "1").alias("paid_flag"),
        F.col("attribution_window_days").cast("int").alias("attribution_window_days"),
    )


def conform_regions(raw: DataFrame) -> DataFrame:
    return raw.select(
        _blank_to_null("region_code").alias("region_code"),
        F.col("region_name"),
        F.col("currency_code"),
        F.col("timezone"),
        F.col("business_hours_start"),
        F.col("business_hours_end"),
        F.col("gos_target_seconds").cast("int").alias("gos_target_seconds"),
        F.col("sla_first_call_minutes").cast("int").alias("sla_first_call_minutes"),
    )


def conform_campaigns(
    raw: DataFrame, channel_codes: Iterable[str]
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Typed campaigns, plus the rows whose label was fixed and those with bad dates."""
    codes = list(channel_codes)
    mapped = normalise_channel(F.col("channel_code"), codes)
    campaigns = raw.select(
        F.col("campaign_id"),
        F.col("campaign_name"),
        F.coalesce(mapped, F.lit(UNKNOWN_CHANNEL)).alias("channel_code"),
        F.col("channel_code").alias("channel_code_raw"),
        F.col("region_code"),
        F.col("product_code"),
        F.col("objective"),
        F.to_date("start_date").alias("start_date"),
        F.to_date("end_date").alias("end_date"),
        F.col("landing_page_url"),
    ).withColumn("end_before_start", F.col("end_date") < F.col("start_date"))
    label_fixes = campaigns.where(
        (F.col("channel_code") != UNKNOWN_CHANNEL)
        & (F.col("channel_code") != F.col("channel_code_raw"))
    ).withColumn("detail", F.concat_ws(" -> ", "channel_code_raw", "channel_code"))
    bad_dates = campaigns.where("end_before_start").withColumn(
        "detail",
        F.concat_ws(" < ", F.col("end_date").cast("string"), F.col("start_date").cast("string")),
    )
    return campaigns, label_fixes, bad_dates


def conform_campaign_id(
    df: DataFrame, campaigns: DataFrame, column: str = "campaign_id"
) -> DataFrame:
    """Replace campaign ids that are not in silver.campaign with `UNKNOWN`.

    Adds `campaign_id_raw` (the source value) and `is_orphan_campaign`.
    """
    known = campaigns.select(F.col("campaign_id").alias("_known_campaign_id")).distinct()
    joined = df.join(known, df[column] == known["_known_campaign_id"], "left")
    orphan = F.col("_known_campaign_id").isNull()
    return (
        joined.withColumn("campaign_id_raw", F.col(column))
        .withColumn("is_orphan_campaign", orphan)
        .withColumn(column, F.when(orphan, F.lit(UNKNOWN_CAMPAIGN_ID)).otherwise(F.col(column)))
        .drop("_known_campaign_id")
    )


def conform_fx(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("rate_month"),
        F.to_date(F.concat(F.col("rate_month"), F.lit("-01"))).alias("rate_month_start"),
        F.col("currency_code"),
        F.col("rate_to_gbp").cast("decimal(18,6)").alias("rate_to_gbp"),
    )


def missing_fx_rates(fx: DataFrame, currencies: Iterable[str], months: Iterable[str]) -> DataFrame:
    """Currency-month pairs needed for reporting but absent from the FX table."""
    spark = fx.sparkSession
    expected = spark.createDataFrame(
        [(m, c) for m in months for c in currencies], "rate_month string, currency_code string"
    )
    present = fx.where(F.col("rate_to_gbp").isNotNull()).select("rate_month", "currency_code")
    return expected.join(present, ["rate_month", "currency_code"], "left_anti").select(
        "rate_month",
        "currency_code",
        F.concat_ws("|", "rate_month", "currency_code").alias("fx_key"),
    )


def conform_agents(
    raw: DataFrame, region_codes: Iterable[str]
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Typed agents (every row kept), plus region fixes, team fixes and duplicated versions."""
    regions = list(region_codes)
    by_agent = Window.partitionBy("agent_id")
    typed = raw.select(
        F.col("agent_id"),
        F.col("agent_name"),
        F.col("agent_email"),
        _blank_to_null("region_code").alias("region_code_raw"),
        _blank_to_null("team").alias("team_raw"),
        F.col("seniority_band"),
        F.to_date("hire_date").alias("hire_date"),
        F.col("fte").cast("decimal(4,2)").alias("fte"),
        (F.col("active_flag") == "1").alias("active_flag"),
        F.col("manager_name"),
        F.col("_row_hash").alias("source_row_hash"),
        F.col("_source_row").alias("source_row"),
    )
    prefix = F.substring("team_raw", 1, 2)
    with_region = typed.withColumn(
        "region_code",
        F.coalesce(F.col("region_code_raw"), F.when(prefix.isin(regions), prefix)),
    )
    team_ok = F.substring("team_raw", 1, 2) == F.col("region_code")
    agents = with_region.withColumn(
        "_consistent_team", F.max(F.when(team_ok, F.col("team_raw"))).over(by_agent)
    ).withColumn("team", F.when(team_ok, F.col("team_raw")).otherwise(F.col("_consistent_team")))
    agents = agents.withColumn("_versions", F.count(F.lit(1)).over(by_agent))

    region_fixes = agents.where(
        F.col("region_code_raw").isNull() & F.col("region_code").isNotNull()
    ).withColumn("detail", F.concat(F.lit("region from team prefix: "), F.col("region_code")))
    team_fixes = agents.where(~team_ok | F.col("team").isNull()).withColumn(
        "detail", F.concat_ws(" -> ", "team_raw", F.coalesce("team", F.lit("<null>")))
    )
    versions = agents.where(F.col("_versions") > 1).withColumn(
        "detail",
        F.concat_ws(
            ", ",
            F.concat(F.lit("team="), "team_raw"),
            F.concat(F.lit("seniority="), "seniority_band"),
        ),  # fmt: skip
    )
    columns = [
        "agent_id", "agent_name", "agent_email", "region_code", "region_code_raw", "team",
        "team_raw", "seniority_band", "hire_date", "fte", "active_flag", "manager_name",
        "source_row_hash", "source_row",
    ]  # fmt: skip
    return agents.select(*columns), region_fixes, team_fixes, versions


# ------------------------------------------------------------------ builders


def _bronze(spark: SparkSession, config: dict, table: str) -> DataFrame:
    return latest_batch(read_table(spark, config, table))


def _finish(
    spark: SparkSession, config: dict, table: str, source_table: str, rows_in: int, out: DataFrame
) -> StageResult:
    overwrite_table(spark, out, config, table)
    rows_out = read_table(spark, config, table).count()
    rows_quarantined = quarantined_count(spark, config, source_table)
    assert_reconciled(table, rows_in, rows_out, rows_quarantined)
    return StageResult(table, source_table, rows_in, rows_out, rows_quarantined)


def build_ref_channel(spark: SparkSession, config: dict) -> StageResult:
    raw = _bronze(spark, config, "bronze.ref_channel_raw")
    return _finish(spark, config, "silver.ref_channel", "bronze.ref_channel_raw", raw.count(),
                   conform_channels(raw))  # fmt: skip


def build_ref_region(spark: SparkSession, config: dict) -> StageResult:
    raw = _bronze(spark, config, "bronze.ref_region_raw")
    return _finish(spark, config, "silver.ref_region", "bronze.ref_region_raw", raw.count(),
                   conform_regions(raw))  # fmt: skip


def build_campaign(spark: SparkSession, config: dict) -> StageResult:
    source = "bronze.campaigns_raw"
    raw = _bronze(spark, config, source)
    codes = [r.channel_code for r in read_table(spark, config, "silver.ref_channel").collect()]
    campaigns, label_fixes, bad_dates = conform_campaigns(raw, codes)

    write_dq_exception(label_fixes, "channel_label_variant", source, "campaign_id", "info",
                       config=config, action="corrected", detail_col="detail")  # fmt: skip
    unmapped = campaigns.where(F.col("channel_code") == UNKNOWN_CHANNEL)
    write_dq_exception(unmapped, "channel_label_unmapped", source, "campaign_id", "warning",
                       config=config, action="flagged", detail_col="channel_code_raw")  # fmt: skip
    write_dq_exception(bad_dates, "campaign_end_before_start", source, "campaign_id", "warning",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip
    result = _finish(spark, config, "silver.campaign", source, raw.count(), campaigns)

    # Orphan campaign ids on leads (defect 5): flagged here, mapped to UNKNOWN when
    # silver.lead is built with conform_campaign_id.
    leads = conform_campaign_id(
        _bronze(spark, config, "bronze.leads_raw").select("lead_id", "campaign_id"),
        read_table(spark, config, "silver.campaign"),
    )
    write_dq_exception(leads.where("is_orphan_campaign"), "campaign_orphan_id",
                       "bronze.leads_raw", "lead_id", "warning", config=config,
                       action="flagged", detail_col="campaign_id_raw")  # fmt: skip
    return result


def build_fx_rate(spark: SparkSession, config: dict) -> StageResult:
    source = "bronze.fx_rates_raw"
    raw = _bronze(spark, config, source)
    fx = conform_fx(raw)
    invalid = fx.where(
        F.col("rate_month_start").isNull()
        | F.col("rate_to_gbp").isNull()
        | (F.col("rate_to_gbp") <= 0)
    ).count()
    duplicates = fx.groupBy("rate_month", "currency_code").count().where("count > 1").count()
    if invalid or duplicates:
        raise ValueError(f"{source}: {invalid} unusable and {duplicates} duplicated FX rates")

    currencies = [r.currency_code for r in read_table(spark, config, "silver.ref_region").collect()]
    missing = missing_fx_rates(fx, sorted(currencies), REPORTING_MONTHS)
    write_dq_exception(missing, "fx_rate_missing", source, "fx_key", "error", config=config,
                       action="flagged",
                       detail_col="fx_key")  # fmt: skip
    return _finish(spark, config, "silver.fx_rate", source, raw.count(), fx)


def build_agent(spark: SparkSession, config: dict) -> StageResult:
    source = "bronze.agents_raw"
    raw = _bronze(spark, config, source)
    regions = [r.region_code for r in read_table(spark, config, "silver.ref_region").collect()]
    agents, region_fixes, team_fixes, versions = conform_agents(raw, regions)

    write_dq_exception(region_fixes, "agent_null_region", source, "agent_id", "warning",
                       config=config, action="corrected", detail_col="detail")  # fmt: skip
    still_null = agents.where(F.col("region_code").isNull()).withColumn(
        "detail", F.lit("no region")
    )
    write_dq_exception(still_null, "agent_null_region", source, "agent_id", "warning",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip
    write_dq_exception(team_fixes, "agent_team_region_mismatch", source, "agent_id", "warning",
                       config=config, action="corrected", detail_col="detail")  # fmt: skip
    write_dq_exception(versions, "agent_duplicate_version", source, "agent_id", "info",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip
    return _finish(spark, config, "silver.agent", source, raw.count(), agents)


def build_reference(spark: SparkSession, config: dict) -> list[StageResult]:
    """Build every step 6 table in dependency order."""
    results = [
        build_ref_channel(spark, config),
        build_ref_region(spark, config),
        build_campaign(spark, config),
        build_fx_rate(spark, config),
        build_agent(spark, config),
    ]
    for r in results:
        log.info(
            "%s: %d in, %d out, %d quarantined", r.table, r.rows_in, r.rows_out, r.rows_quarantined
        )
    return results
