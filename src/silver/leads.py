"""Silver leads and consent: silver.lead, silver.lead_dedup_map, silver.consent_current.

Order of checks on the latest bronze batch of leads.csv:
1. Primary-key conflicts (defect 2): every row of a `lead_id` that appears with
   different content is quarantined, severity error. Not resolved.
2. Unidentifiable leads (defect 4): neither email nor phone; quarantined, error.
   A lead with one of the two is matched on the key it has.
3. Typing and conforming the rest: orphan campaign ids become `UNKNOWN`
   (defect 5, flagged at step 6); `channel_code` is the campaign's channel, and
   the lead's own label is kept as `lead_channel_code` (defect 24, assumption 4;
   the lead's label is the fallback only when the campaign is unknown).
4. Flags that keep the lead: implausible date of birth, nulled (defect 7,
   assumption 18); created outside the reporting period (defect 6); lead channel
   disagreeing with the campaign channel (defect 24).
5. Dedup (defect 3, src/silver/dedupe_leads.py): survivors go to silver.lead,
   every deduplicated lead to silver.lead_dedup_map, and each mapped duplicate
   is also flagged in silver.dq_exception so every bronze row is traceable.

Reconciliation, by distinct `lead_id`:
    distinct ids in = survivors + mapped duplicates + unidentifiable + PK conflicts
and by row:
    rows in = lead_dedup_map rows + rows quarantined.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.bronze.ingest import latest_batch
from src.common.dq import (
    ReconciliationError,
    StageResult,
    assert_reconciled,
    quarantined_count,
    write_dq_exception,
)
from src.common.period import REPORTING_END, REPORTING_START
from src.common.tables import overwrite_table, read_table
from src.silver.dedupe_leads import dedupe_leads
from src.silver.reference import conform_campaign_id, normalise_channel
from src.silver.resolve_consent import resolve_consent

log = logging.getLogger("northgate.silver")

LEADS_SOURCE = "bronze.leads_raw"
CONSENT_SOURCE = "bronze.consent_events_raw"
MIN_AGE, MAX_AGE = 18, 100
ISO_UTC = "yyyy-MM-dd'T'HH:mm:ssX"


@dataclass(frozen=True)
class LeadReconciliation:
    rows_in: int
    distinct_ids_in: int
    pk_conflict_ids: int
    pk_conflict_rows: int
    unidentifiable_ids: int
    rows_quarantined: int
    survivors: int
    mapped_duplicates: int
    map_rows: int


# ------------------------------------------------------------------ pure checks


def primary_key_conflicts(raw: DataFrame) -> DataFrame:
    """Every row of each `lead_id` that appears more than once."""
    by_id = Window.partitionBy("lead_id")
    return (
        raw.withColumn("_n", F.count(F.lit(1)).over(by_id))
        .withColumn("_distinct", F.size(F.collect_set("_row_hash").over(by_id)))
        .where("_n > 1")
    )


def unidentifiable(leads: DataFrame) -> DataFrame:
    """Leads with neither an email nor a phone."""
    blank = lambda c: F.col(c).isNull() | (F.trim(F.col(c)) == "")  # noqa: E731
    return leads.where(blank("email") & blank("phone"))


def age_at_creation() -> F.Column:
    return F.floor(F.months_between(F.to_date("created_at_utc"), F.col("date_of_birth")) / 12)


def implausible_dob(leads: DataFrame) -> DataFrame:
    """Leads aged under 18 or over 100 when created (includes births after creation)."""
    age = age_at_creation()
    return leads.where(F.col("date_of_birth").isNotNull() & ((age < MIN_AGE) | (age > MAX_AGE)))


def null_implausible_dob(leads: DataFrame) -> DataFrame:
    """Null a DOB giving an age at creation under 18 or over 100; keep every lead.

    Age is whole years at the lead's creation date, so someone whose 18th birthday is
    after creation is a minor (DOB nulled), and an 18th birthday on the creation day
    is an adult (DOB kept).
    """
    age = age_at_creation()
    return leads.withColumn(
        "date_of_birth",
        F.when((age < MIN_AGE) | (age > MAX_AGE), None).otherwise(F.col("date_of_birth")),
    )


def conform_leads(raw: DataFrame) -> DataFrame:
    flag = lambda c: F.when(F.col(c) == "Y", True).when(F.col(c) == "N", False)  # noqa: E731
    return raw.select(
        "lead_id",
        F.to_timestamp("created_at_utc", ISO_UTC).alias("created_at_utc"),
        "region_code",
        "campaign_id",
        F.col("channel_code").alias("lead_channel_code_raw"),
        "source_detail",
        "product_interest",
        "first_name",
        "last_name",
        "email",
        "phone",
        "postcode",
        F.to_date("date_of_birth").alias("date_of_birth"),
        flag("smoker_flag").alias("smoker_flag"),
        flag("health_condition_flag").alias("health_condition_flag"),
        F.col("estimated_cover_amount").cast("decimal(14,2)").alias("estimated_cover_amount"),
        "consent_marketing",
        F.to_timestamp("consent_timestamp_utc", ISO_UTC).alias("consent_timestamp_utc"),
        "consent_source",
        "lead_status",
        "duplicate_of_lead_id",
        F.to_timestamp("ingested_at_utc", ISO_UTC).alias("ingested_at_utc"),
        F.col("_source_row").alias("source_row"),
    )


# ------------------------------------------------------------------ builders


def build_leads(spark: SparkSession, config: dict) -> LeadReconciliation:
    raw = latest_batch(read_table(spark, config, LEADS_SOURCE)).cache()
    rows_in = raw.count()
    distinct_ids_in = raw.select("lead_id").distinct().count()

    # 1. Primary-key conflicts.
    pk = primary_key_conflicts(raw).cache()
    if pk.where("_distinct = 1").count():
        raise ReconciliationError(f"{LEADS_SOURCE}: exact duplicate lead rows; no rule for them")
    pk = pk.withColumn("detail", F.concat(F.lit("campaign_id="), F.col("campaign_id")))
    write_dq_exception(pk, "lead_pk_conflict", LEADS_SOURCE, "lead_id", "error",
                       config=config, detail_col="detail")  # fmt: skip
    rest = raw.join(pk.select("lead_id").distinct(), "lead_id", "left_anti")

    # 2. Unidentifiable.
    unid = unidentifiable(rest)
    write_dq_exception(unid, "lead_unidentifiable", LEADS_SOURCE, "lead_id", "error",
                       config=config)  # fmt: skip
    identified = rest.join(unid.select("lead_id"), "lead_id", "left_anti")

    # 3. Type and conform.
    campaigns = read_table(spark, config, "silver.campaign")
    codes = [r.channel_code for r in read_table(spark, config, "silver.ref_channel").collect()]
    leads = conform_campaign_id(conform_leads(identified), campaigns)
    leads = (
        leads.join(
            campaigns.select("campaign_id", F.col("channel_code").alias("_campaign_channel")),
            "campaign_id",
            "left",
        )
        .withColumn("lead_channel_code", normalise_channel(F.col("lead_channel_code_raw"), codes))
        .withColumn("channel_code", F.coalesce("_campaign_channel", "lead_channel_code"))
    )

    # 4. Flags that keep the lead.
    mismatch = leads.where(
        F.col("_campaign_channel").isNotNull()
        & ~F.col("lead_channel_code").eqNullSafe(F.col("_campaign_channel"))
    ).withColumn("detail", F.concat_ws(" vs campaign ", "lead_channel_code", "_campaign_channel"))
    write_dq_exception(mismatch, "lead_campaign_channel_mismatch", LEADS_SOURCE, "lead_id",
                       "info", config=config, action="flagged", detail_col="detail")  # fmt: skip

    bad_dob = implausible_dob(leads).withColumn(
        "detail", F.concat(F.lit("age at creation "), age_at_creation().cast("string"))
    )
    write_dq_exception(bad_dob, "lead_dob_implausible", LEADS_SOURCE, "lead_id", "warning",
                       config=config, action="corrected", detail_col="detail")  # fmt: skip
    leads = null_implausible_dob(leads)

    created = F.to_date("created_at_utc")
    outside = leads.where(
        (created < F.lit(REPORTING_START)) | (created > F.lit(REPORTING_END))
    ).withColumn("detail", created.cast("string"))
    write_dq_exception(outside, "lead_outside_period", LEADS_SOURCE, "lead_id", "info",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip

    # 5. Dedup.
    leads = leads.drop("_campaign_channel").cache()
    survivors, mapping = dedupe_leads(leads)
    overwrite_table(spark, survivors, config, "silver.lead")
    overwrite_table(spark, mapping, config, "silver.lead_dedup_map")
    dedup_map = read_table(spark, config, "silver.lead_dedup_map")
    duplicates = dedup_map.where("source_lead_id != surviving_lead_id").withColumn(
        "detail", F.concat_ws(" via ", "surviving_lead_id", "match_rule")
    )
    write_dq_exception(duplicates, "lead_near_duplicate", LEADS_SOURCE, "source_lead_id",
                       "info", config=config, action="flagged", detail_col="detail")  # fmt: skip

    rec = LeadReconciliation(
        rows_in=rows_in,
        distinct_ids_in=distinct_ids_in,
        pk_conflict_ids=pk.select("lead_id").distinct().count(),
        pk_conflict_rows=pk.count(),
        unidentifiable_ids=unid.select("lead_id").distinct().count(),
        rows_quarantined=quarantined_count(spark, config, LEADS_SOURCE),
        survivors=read_table(spark, config, "silver.lead").count(),
        mapped_duplicates=duplicates.count(),
        map_rows=dedup_map.count(),
    )
    raw.unpersist()
    pk.unpersist()
    leads.unpersist()
    assert_reconciled("silver.lead_dedup_map", rec.rows_in, rec.map_rows, rec.rows_quarantined)
    accounted = rec.survivors + rec.mapped_duplicates + rec.unidentifiable_ids + rec.pk_conflict_ids
    if rec.distinct_ids_in != accounted:
        raise ReconciliationError(
            f"silver.lead: {rec.distinct_ids_in} distinct ids != {rec.survivors} surviving + "
            f"{rec.mapped_duplicates} mapped + {rec.unidentifiable_ids} unidentifiable + "
            f"{rec.pk_conflict_ids} PK conflicts"
        )
    log.info("silver.lead: %s", rec)
    return rec


def build_consent(spark: SparkSession, config: dict) -> StageResult:
    raw = latest_batch(read_table(spark, config, CONSENT_SOURCE))
    events = raw.select(
        "event_id",
        "lead_id",
        F.to_timestamp("event_ts_utc", ISO_UTC).alias("event_ts_utc"),
        "event_type",
        F.from_json("channels", "array<string>").alias("channels"),
    )
    unusable = events.where(F.col("lead_id").isNull() | F.col("event_ts_utc").isNull()).count()
    if unusable:
        raise ReconciliationError(f"{CONSENT_SOURCE}: {unusable} events without lead or time")

    overwrite_table(spark, resolve_consent(events), config, "silver.consent_current")
    current = read_table(spark, config, "silver.consent_current")

    withdrawn = current.where("current_event_type = 'WITHDRAWN'").withColumn(
        "detail", F.col("current_event_id")
    )
    write_dq_exception(withdrawn, "consent_latest_withdrawn", CONSENT_SOURCE, "lead_id", "info",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip
    lead_says_y = (
        latest_batch(read_table(spark, config, LEADS_SOURCE))
        .where("consent_marketing = 'Y'")
        .select("lead_id")
        .distinct()
    )
    still_y = withdrawn.join(lead_says_y, "lead_id").withColumn(
        "detail", F.lit("lead record consent_marketing = Y")
    )
    write_dq_exception(still_y, "consent_withdrawn_marketing_y", LEADS_SOURCE, "lead_id",
                       "warning", config=config, action="flagged", detail_col="detail")  # fmt: skip

    rows_in = events.select("lead_id").distinct().count()
    result = StageResult("silver.consent_current", CONSENT_SOURCE, rows_in, current.count(),
                         quarantined_count(spark, config, CONSENT_SOURCE))  # fmt: skip
    assert_reconciled(result.table, result.rows_in, result.rows_out, result.rows_quarantined)
    return result
