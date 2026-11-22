"""Resolve consent_events.json into silver.consent_current.

Some leads have more than one consent event, so the latest event by
`event_ts_utc` (`event_id` breaking ties) sets the current state, one row per
lead. `silver.consent_current` is the source of truth for consent, not the
lead record's `consent_marketing` column (defect 25).

`consent_phone_current_flag` comes from the latest event whose channels
include PHONE, because a withdrawal can cover email only. It is null when the
lead has no PHONE event. It feeds `gold.fact_lead_funnel` and the consent
governance check.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

GRANTING_EVENTS = ("GRANTED", "RE_GRANTED")


def resolve_consent(consent_events: DataFrame) -> DataFrame:
    """One current consent row per lead from typed consent events.

    Input columns: `event_id`, `lead_id`, `event_ts_utc` (timestamp),
    `event_type`, `channels` (array<string>).
    """
    latest = Window.partitionBy("lead_id").orderBy(
        F.col("event_ts_utc").desc(), F.col("event_id").desc()
    )
    granted = F.upper("event_type").isin(*GRANTING_EVENTS)
    ranked = consent_events.withColumn("_rank", F.row_number().over(latest)).withColumn(
        "event_count", F.count(F.lit(1)).over(Window.partitionBy("lead_id"))
    )
    current = ranked.where("_rank = 1").select(
        "lead_id",
        F.col("event_id").alias("current_event_id"),
        F.col("event_ts_utc").alias("current_event_ts_utc"),
        F.col("event_type").alias("current_event_type"),
        F.col("channels").alias("current_event_channels"),
        granted.alias("consent_marketing_current"),
        F.col("event_count").cast("int").alias("event_count"),
    )
    phone = (
        consent_events.where(
            F.array_contains(F.coalesce("channels", F.array().cast("array<string>")), "PHONE")
        )
        .withColumn("_rank", F.row_number().over(latest))
        .where("_rank = 1")
        .select("lead_id", granted.alias("consent_phone_current_flag"))
    )
    return current.join(phone, "lead_id", "left")
