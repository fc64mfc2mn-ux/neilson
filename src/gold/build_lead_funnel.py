"""Build gold.fact_lead_funnel, the integration table, plus its bridges (step 12).

`gold.fact_lead_funnel`: one row per surviving lead, the lead to call to
policy path. The brief's 21 columns, in order (`FUNNEL_COLUMNS`), then
`lead_in_business_hours_flag` for the Ops view (assumption 9):

- `campaign_key`: first touch. The surviving lead is the earliest in its
  dedup cluster, so its campaign is the first-touch campaign.
- Calls come through `gold.bridge_lead_call` (calls on duplicates count for
  their survivor). `first_call_*` is the earliest call; `last_call_agent_key`
  is the agent on the last call on or before the policy application date, or
  the last call overall when there is no policy. A lead with no call has null
  call keys and timings, zero counts and false flags.
- `within_first_call_sla_flag`: first call within the region's
  `sla_first_call_minutes`; false when never called.
- `quoted_flag`: a QUOTED, APPLICATION_STARTED or SOLD call, or a policy.
  `application_flag`: an APPLICATION_STARTED or SOLD call, or a policy.
- Policy: the lead's policy in `gold.fact_policy` (at most one per lead, else
  the stage stops). `days_lead_to_policy` runs from lead creation to
  application date.

`gold.bridge_lead_call`: one row per lead by call, for calls with a surviving
lead (61,690 of 62,829), with `call_sequence` per lead.
`gold.bridge_user_region`: one row per user by region from
`bronze.sec_user_region_raw` (latest batch; three string columns, typed here).
Region `ALL` stays as written, with a null `region_key` and
`all_regions_flag`; an unmapped user has no rows (default deny).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.bronze.ingest import latest_batch
from src.common.dq import ReconciliationError
from src.common.tables import overwrite_table, read_table

log = logging.getLogger("northgate.gold")

FUNNEL_COLUMNS = (
    "lead_key",
    "lead_created_ts_utc",
    "lead_created_date_key",
    "region_key",
    "campaign_key",
    "channel_key",
    "product_key",
    "first_call_ts_utc",
    "first_call_agent_key",
    "last_call_agent_key",
    "call_attempt_count",
    "connected_call_count",
    "minutes_to_first_call",
    "first_call_bucket",
    "first_call_bucket_sort",
    "within_first_call_sla_flag",
    "quoted_flag",
    "application_flag",
    "policy_key",
    "policy_start_date_key",
    "annual_premium_gbp",
    "days_lead_to_policy",
    "consent_phone_current_flag",
    "dedup_cluster_size",
    "lead_in_business_hours_flag",
)
QUOTED = ("QUOTED", "APPLICATION_STARTED", "SOLD")
APPLIED = ("APPLICATION_STARTED", "SOLD")
ALL_REGIONS = "ALL"


# Time-to-first-call distribution (step 16b): label and upper bound in minutes, closed
# below and open above; the last bucket has no upper bound. Never called: null.
FIRST_CALL_BUCKETS = (
    ("0-15 min", 15),
    ("15-30 min", 30),
    ("30-60 min", 60),
    ("1-4 h", 240),
    ("4-24 h", 1440),
    ("Over 24 h", None),
)


def first_call_bucket(minutes: Column) -> tuple[Column, Column]:
    """(label, sort order 1 to 6) for minutes to first call; null stays null."""
    label, sort = F.lit(None).cast("string"), F.lit(None).cast("int")
    for i, (name, upper) in reversed(list(enumerate(FIRST_CALL_BUCKETS, start=1))):
        cond = minutes.isNotNull() if upper is None else minutes < upper
        label = F.when(cond, F.lit(name)).otherwise(label)
        sort = F.when(cond, F.lit(i)).otherwise(sort)
    return label, sort


@dataclass(frozen=True)
class FunnelResult:
    funnel_rows: int
    leads: int
    bridge_lead_call_rows: int
    bridge_user_region_rows: int
    leads_with_calls: int
    leads_with_policy: int
    leads_in_business_hours: int


def bridge_lead_call(fact_call: DataFrame) -> DataFrame:
    """One row per lead by call, calls with a surviving lead only, numbered per lead."""
    order = Window.partitionBy("lead_key").orderBy("call_start_ts_utc", "call_key")
    return (
        fact_call.where(F.col("lead_key").isNotNull())
        .select("lead_key", "call_key", "call_start_ts_utc")
        .withColumn("call_sequence", F.row_number().over(order))
        .select("lead_key", "call_key", "call_sequence")
    )


def bridge_user_region(sec: DataFrame, dim_region: DataFrame) -> DataFrame:
    """One row per user by region. ALL kept (null key, flag); other regions must resolve."""
    users = sec.select(
        F.lower(F.trim("user_email")).alias("user_principal_name"),
        F.trim("role_name").alias("role_name"),
        F.upper(F.trim("region_code")).alias("region_code"),
    ).distinct()
    keys = dim_region.select(F.col("region_code").alias("_r"), "region_key")
    j = users.join(keys, users["region_code"] == keys["_r"], "left")
    unresolved = j.where((F.col("region_code") != ALL_REGIONS) & F.col("region_key").isNull())
    if unresolved.count():
        raise ReconciliationError(
            f"bridge_user_region: {unresolved.count()} row(s) name a region not in gold.dim_region"
        )
    return j.select(
        "user_principal_name",
        "role_name",
        "region_code",
        "region_key",
        (F.col("region_code") == ALL_REGIONS).alias("all_regions_flag"),
    )


def lead_funnel(
    fact_lead: DataFrame,
    dim_lead: DataFrame,
    fact_call: DataFrame,
    fact_policy: DataFrame,
    dim_region: DataFrame,
) -> DataFrame:
    """gold.fact_lead_funnel: one row per lead in `fact_lead`."""
    policies = fact_policy.where(F.col("lead_key").isNotNull()).select(
        "lead_key", "policy_key", "application_date", "policy_start_date_key", "annual_premium_gbp"
    )
    calls = (
        fact_call.where(F.col("lead_key").isNotNull())
        .select(
            "lead_key", "call_key", "agent_key", "call_start_ts_utc", "disposition", "is_connected"
        )  # fmt: skip
        .join(policies.select("lead_key", "application_date"), "lead_key", "left")
    )
    before_app = F.col("application_date").isNull() | (
        F.to_date("call_start_ts_utc") <= F.col("application_date")
    )
    by_call = lambda c: F.struct("call_start_ts_utc", "call_key", c)  # noqa: E731
    per_lead = calls.groupBy("lead_key").agg(
        F.count(F.lit(1)).alias("call_attempt_count"),
        F.sum(F.col("is_connected").cast("int")).alias("connected_call_count"),
        F.min("call_start_ts_utc").alias("first_call_ts_utc"),
        F.min(by_call("agent_key")).getField("agent_key").alias("first_call_agent_key"),
        F.max(F.when(before_app, by_call("agent_key")))
        .getField("agent_key")
        .alias("last_call_agent_key"),
        F.max(F.col("disposition").isin(*QUOTED)).alias("_quoted_call"),
        F.max(F.col("disposition").isin(*APPLIED)).alias("_applied_call"),
    )
    sla = dim_region.select("region_key", "sla_first_call_minutes")
    df = (
        fact_lead.join(
            dim_lead.select("lead_key", "consent_phone_current_flag", "dedup_cluster_size"),
            "lead_key",
            "left",
        )  # fmt: skip
        .join(per_lead, "lead_key", "left")
        .join(policies, "lead_key", "left")
        .join(sla, "region_key", "left")
    )
    minutes = (
        (F.unix_timestamp("first_call_ts_utc") - F.unix_timestamp("lead_created_ts_utc")) / 60
    ).cast("decimal(12,2)")
    has_policy = F.col("policy_key").isNotNull()
    bucket, bucket_sort = first_call_bucket(minutes)
    return df.select(
        "lead_key",
        "lead_created_ts_utc",
        "lead_created_date_key",
        "region_key",
        "campaign_key",
        "channel_key",
        "product_key",
        "first_call_ts_utc",
        "first_call_agent_key",
        "last_call_agent_key",
        F.coalesce("call_attempt_count", F.lit(0)).cast("int").alias("call_attempt_count"),
        F.coalesce("connected_call_count", F.lit(0)).cast("int").alias("connected_call_count"),
        minutes.alias("minutes_to_first_call"),
        bucket.alias("first_call_bucket"),
        bucket_sort.alias("first_call_bucket_sort"),
        F.coalesce(minutes <= F.col("sla_first_call_minutes"), F.lit(False)).alias(
            "within_first_call_sla_flag"
        ),
        (F.coalesce("_quoted_call", F.lit(False)) | has_policy).alias("quoted_flag"),
        (F.coalesce("_applied_call", F.lit(False)) | has_policy).alias("application_flag"),
        "policy_key",
        "policy_start_date_key",
        "annual_premium_gbp",
        F.datediff("application_date", F.to_date("lead_created_ts_utc")).alias(
            "days_lead_to_policy"
        ),
        "consent_phone_current_flag",
        "dedup_cluster_size",
        "lead_in_business_hours_flag",
    )


def build_lead_funnel(spark: SparkSession, config: dict) -> FunnelResult:
    t = lambda name: read_table(spark, config, name)  # noqa: E731
    fact_call, fact_policy = t("gold.fact_call"), t("gold.fact_policy")
    per_lead = fact_policy.where("lead_key IS NOT NULL").groupBy("lead_key").count()
    if per_lead.where("count > 1").count():
        raise ReconciliationError("gold.fact_policy: a lead has more than one policy; no rule")

    overwrite_table(spark, bridge_lead_call(fact_call), config, "gold.bridge_lead_call")
    bridge_rows = t("gold.bridge_lead_call").count()
    expected = fact_call.where("lead_key IS NOT NULL").count()
    if bridge_rows != expected:
        raise ReconciliationError(f"gold.bridge_lead_call: {bridge_rows} != {expected} calls")

    sec = latest_batch(t("bronze.sec_user_region_raw"))
    users = bridge_user_region(sec, t("gold.dim_region"))
    overwrite_table(spark, users, config, "gold.bridge_user_region")
    user_rows = t("gold.bridge_user_region").count()
    if user_rows != sec.count():
        raise ReconciliationError(f"gold.bridge_user_region: {user_rows} != {sec.count()} rows")

    funnel = lead_funnel(t("gold.fact_lead"), t("gold.dim_lead"), fact_call, fact_policy,
                         t("gold.dim_region"))  # fmt: skip
    overwrite_table(spark, funnel, config, "gold.fact_lead_funnel")
    f = t("gold.fact_lead_funnel")
    s = f.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("lead_key").alias("leads"),
        F.sum((F.col("call_attempt_count") > 0).cast("int")).alias("called"),
        F.sum(F.col("policy_key").isNotNull().cast("int")).alias("policies"),
        F.sum(F.col("lead_in_business_hours_flag").cast("int")).alias("in_hours"),
    ).first()
    lead_rows = t("gold.fact_lead").count()
    if not s.rows == s.leads == lead_rows:
        raise ReconciliationError(
            f"gold.fact_lead_funnel: {s.rows} rows, {s.leads} leads != {lead_rows} in fact_lead"
        )
    result = FunnelResult(s.rows, s.leads, bridge_rows, user_rows, s.called, s.policies,
                          s.in_hours)  # fmt: skip
    log.info("Reconciled gold.fact_lead_funnel and bridges: %s", result)
    return result
