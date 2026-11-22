"""Build the gold fact tables other than the lead funnel (step 12).

- `gold.fact_lead`: one row per surviving lead (`silver.lead`), with
  `lead_created_date_key`, `campaign_key`, `channel_key`, `region_key`,
  `product_key` and `lead_in_business_hours_flag`.
- `gold.fact_call`: one row per deduplicated call (`silver.call`), seconds
  and UTC throughout. `is_connected` follows assumption 2.
- `gold.fact_policy`: one row per deduplicated policy (`silver.policy`), with
  `annual_premium_gbp` at the application month's rate, and the surviving lead's
  `lead_created_date_key`, `campaign_key` and `channel_key` (null without a lead), so
  lead-linked policies follow the lead's date and first-touch campaign (step 16b).
- `gold.fact_marketing_spend_daily`: date by campaign (`silver.marketing_spend`).

Foreign keys (CLAUDE.md decisions): campaign, channel and agent resolve to
`UNKNOWN` (-1) and are counted; region and product have no `UNKNOWN` member,
so an unresolved key stops the stage. Rows are never dropped and keys never
set null by a failed lookup. Every date key must exist in `gold.dim_date`.

A call or policy whose lead is not a surviving lead keeps a null `lead_key`
and says why in `lead_match_status` (assumptions 11 and 20): `dim_lead` is
one row per real lead, so it has no `UNKNOWN` member.

Policy GBP: no rate is invented (assumption 7). A policy whose currency has no
rate for its application month keeps a null `annual_premium_gbp`, is flagged
`gbp_rate_missing` and is recorded in `silver.dq_exception`.

Columns the brief rates tier 3 (`underwriting_decision`, `call_notes`,
`recording_url`) do not reach gold; the PII scan in `apply_pii_policy` checks.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.bronze.ingest import latest_batch
from src.common.dq import ReconciliationError, write_dq_exception
from src.common.tables import overwrite_table, read_table
from src.gold.build_dimensions import UNKNOWN_KEY, resolve_agent, surrogate_key

log = logging.getLogger("northgate.gold")

CONNECTED_DISPOSITIONS = ("CONNECTED", "QUOTED", "APPLICATION_STARTED", "SOLD")  # assumption 2


@dataclass(frozen=True)
class FactResult:
    table: str
    source: str
    rows_in: int
    rows_out: int
    unknown: dict[str, int] = field(default_factory=dict)
    flagged: dict[str, int] = field(default_factory=dict)


# ------------------------------------------------------------------ helpers


def date_key(c: str | Column) -> Column:
    """yyyymmdd as int, matching `gold.dim_date.date_key`."""
    col = F.col(c) if isinstance(c, str) else c
    return F.date_format(col, "yyyyMMdd").cast("int")


def check_date_keys(df: DataFrame, cols: list[str], dim_date: DataFrame, label: str) -> None:
    """Every non-null date in `cols` must be a row of `gold.dim_date`."""
    dates = dim_date.select(F.col("date").alias("_d"))
    for c in cols:
        missing = (
            df.where(F.col(c).isNotNull())
            .select(F.to_date(c).alias("_d"))
            .join(dates, "_d", "left_anti")
            .count()
        )
        if missing:
            raise ReconciliationError(f"{label}.{c}: {missing} dates not in gold.dim_date")


def resolve_key(
    df: DataFrame,
    fact_col: str,
    dim: DataFrame,
    dim_col: str,
    key_col: str,
    *,
    unknown: bool,
    label: str,
) -> tuple[DataFrame, int]:
    """Add `key_col` by looking `fact_col` up in `dim`. Returns (df, rows set to UNKNOWN).

    `unknown=True`: unmatched or null values get `UNKNOWN` (-1) and are counted
    (rows already carrying the UNKNOWN code count too). `unknown=False`: any
    unmatched or null value raises; nothing is dropped or nulled.
    """
    keys = dim.select(F.col(dim_col).alias("_nk"), F.col(key_col).alias("_key"))
    joined = df.join(keys, df[fact_col] == keys["_nk"], "left").drop("_nk")
    if unknown:
        out = joined.withColumn(key_col, F.coalesce("_key", F.lit(UNKNOWN_KEY))).drop("_key")
        return out, out.where(F.col(key_col) == UNKNOWN_KEY).count()
    bad = joined.where(F.col("_key").isNull()).count()
    if bad:
        raise ReconciliationError(
            f"{label}: {bad} row(s) with no {key_col}; this dimension has no UNKNOWN member"
        )
    return joined.withColumnRenamed("_key", key_col), 0


def business_hours_flag(df: DataFrame, ts_col: str, ref_region: DataFrame, out: str) -> DataFrame:
    """True when `ts_col` (UTC) falls Monday to Friday, start inclusive and end
    exclusive, in the row region's own timezone (assumption 9; no holidays)."""
    hours = ref_region.select(
        F.col("region_code").alias("_r"),
        F.col("timezone").alias("_tz"),
        F.col("business_hours_start").alias("_start"),
        F.col("business_hours_end").alias("_end"),
    )
    j = df.join(hours, df["region_code"] == hours["_r"], "left")
    local = F.from_utc_timestamp(F.col(ts_col), F.col("_tz"))
    hhmm = F.date_format(local, "HH:mm")
    weekday = F.dayofweek(local).between(2, 6)  # Spark: Sunday = 1
    flag = weekday & (hhmm >= F.col("_start")) & (hhmm < F.col("_end"))
    return j.withColumn(out, F.coalesce(flag, F.lit(False))).drop("_r", "_tz", "_start", "_end")


def policy_gbp(policy: DataFrame, fx: DataFrame) -> DataFrame:
    """Add `rate_to_gbp`, `annual_premium_gbp` (application month) and `gbp_rate_missing`."""
    rates = fx.select(
        F.col("rate_month").alias("_m"),
        F.col("currency_code").alias("_c"),
        F.col("rate_to_gbp"),
    )
    month = F.date_format("application_date", "yyyy-MM")
    j = policy.join(
        rates, (month == F.col("_m")) & (policy["currency_code"] == F.col("_c")), "left"
    ).drop("_m", "_c")
    gbp = F.round(F.col("annual_premium_local") * F.col("rate_to_gbp"), 2).cast("decimal(14,2)")
    return j.withColumn("annual_premium_gbp", gbp).withColumn(
        "gbp_rate_missing", F.col("rate_to_gbp").isNull()
    )


def call_lead_status(calls: DataFrame, dedup_map: DataFrame, raw_lead_ids: DataFrame) -> DataFrame:
    """Add `surviving_lead_id` and `lead_match_status`: matched, lead_quarantined (in the
    lead file but not in dedup scope) or lead_not_found (in no lead file)."""
    m = dedup_map.select(F.col("source_lead_id").alias("_src"), "surviving_lead_id")
    raw = raw_lead_ids.select(F.col("lead_id").alias("_raw")).distinct()
    j = calls.join(m, calls["lead_id"] == m["_src"], "left").join(
        raw, calls["lead_id"] == raw["_raw"], "left"
    )
    status = (
        F.when(F.col("surviving_lead_id").isNotNull(), "matched")
        .when(F.col("_raw").isNotNull(), "lead_quarantined")
        .when(F.col("lead_id").isNull(), "no_lead")
        .otherwise("lead_not_found")
    )
    return j.withColumn("lead_match_status", status).drop("_src", "_raw")


LEAD_KEYS = ("lead_created_date_key", "campaign_key", "channel_key")


def attach_lead_keys(policy: DataFrame, leads: DataFrame) -> DataFrame:
    """Add the surviving lead's date, campaign and channel keys after `is_lead_attributed`.
    Left join on the unique `lead_key`: a policy with no lead keeps null keys."""
    keys = leads.select(F.col("lead_key").alias("_lk"), *LEAD_KEYS)
    joined = policy.join(keys, policy["lead_key"] == keys["_lk"], "left")
    cols = []
    for c in policy.columns:
        cols.append(c)
        if c == "is_lead_attributed":
            cols += list(LEAD_KEYS)
    return joined.select(*cols)


def _check(table: str, rows_in: int, rows_out: int) -> None:
    if rows_in != rows_out:
        raise ReconciliationError(f"{table}: {rows_out} rows != {rows_in} from silver")


# ------------------------------------------------------------------ facts


def fact_lead(lead: DataFrame, dims: dict[str, DataFrame], ref_region: DataFrame):
    df, u_campaign = resolve_key(
        lead,
        "campaign_id",
        dims["campaign"],
        "campaign_id",
        "campaign_key",
        unknown=True,
        label="fact_lead.campaign",
    )
    df, u_channel = resolve_key(
        df,
        "channel_code",
        dims["channel"],
        "channel_code",
        "channel_key",
        unknown=True,
        label="fact_lead.channel",
    )
    df, _ = resolve_key(df, "region_code", dims["region"], "region_code", "region_key",
                        unknown=False, label="fact_lead.region")  # fmt: skip
    df, _ = resolve_key(df, "product_interest", dims["product"], "product_code", "product_key",
                        unknown=False, label="fact_lead.product")  # fmt: skip
    df = business_hours_flag(df, "created_at_utc", ref_region, "lead_in_business_hours_flag")
    out = df.select(
        surrogate_key("lead_id").alias("lead_key"),
        date_key(F.to_date("created_at_utc")).alias("lead_created_date_key"),
        F.col("created_at_utc").alias("lead_created_ts_utc"),
        "campaign_key",
        "channel_key",
        "region_key",
        "product_key",
        "lead_in_business_hours_flag",
        "is_orphan_campaign",
    )
    return out, {"campaign_key": u_campaign, "channel_key": u_channel}


def fact_call(calls: DataFrame, dims: dict[str, DataFrame], dedup_map, raw_lead_ids):
    df = call_lead_status(calls, dedup_map, raw_lead_ids)
    df = resolve_agent(df, dims["agent"])
    u_agent = df.where(F.col("agent_key") == UNKNOWN_KEY).count()
    df, _ = resolve_key(df, "region_code", dims["region"], "region_code", "region_key",
                        unknown=False, label="fact_call.region")  # fmt: skip
    out = df.select(
        surrogate_key("call_id").alias("call_key"),
        "call_id",
        F.when(F.col("lead_match_status") == "matched", surrogate_key("surviving_lead_id"))
        .cast("bigint")
        .alias("lead_key"),
        "lead_match_status",
        "agent_key",
        "region_key",
        date_key(F.to_date("call_start_ts_utc")).alias("call_date_key"),
        "call_start_ts_utc",
        "call_end_ts_utc",
        "duration_seconds",
        "talk_time_seconds",
        "hold_time_seconds",
        "queue_wait_seconds",
        "call_direction",
        "disposition",
        "wrap_up_code",
        "attempt_number",
        "answered_within_sla_flag",
        F.col("disposition").isin(*CONNECTED_DISPOSITIONS).alias("is_connected"),
        "qa_score",
        "compliance_fail_flag",
        "after_do_not_call",
        "residual_offset_flag",
        "source_file",
    )
    return out, {"agent_key": u_agent}


def fact_policy(policy, dims, dedup_map, raw_lead_ids, calls, fx, leads):
    df = call_lead_status(policy, dedup_map, raw_lead_ids)
    df = resolve_agent(df, dims["agent"], day=F.col("application_date"))
    u_agent = df.where(F.col("agent_key") == UNKNOWN_KEY).count()
    df, _ = resolve_key(df, "region_code", dims["region"], "region_code", "region_key",
                        unknown=False, label="fact_policy.region")  # fmt: skip
    df, _ = resolve_key(df, "product_code", dims["product"], "product_code", "product_key",
                        unknown=False, label="fact_policy.product")  # fmt: skip
    known_calls = calls.select(F.col("call_id").alias("_cid"), F.lit(True).alias("_call_ok"))
    df = df.join(known_calls, df["call_id"] == known_calls["_cid"], "left")
    df = policy_gbp(df, fx)
    out = df.select(
        surrogate_key("policy_id").alias("policy_key"),
        "policy_id",
        F.when(F.col("lead_match_status") == "matched", surrogate_key("surviving_lead_id"))
        .cast("bigint")
        .alias("lead_key"),
        "lead_match_status",
        (F.col("lead_match_status") == "matched").alias("is_lead_attributed"),
        F.when(F.col("_call_ok"), surrogate_key("call_id")).cast("bigint").alias("call_key"),
        "call_id",
        "agent_key",
        "region_key",
        "product_key",
        date_key("application_date").alias("application_date_key"),
        "application_date",
        date_key("policy_start_date").alias("policy_start_date_key"),
        date_key("cancellation_date").alias("cancellation_date_key"),
        "policy_status",
        (F.col("policy_status") != "NTU").alias("counts_as_sale"),  # assumption 3
        "payment_frequency",
        "currency_code",
        "annual_premium_local",
        "monthly_premium_local",
        "sum_assured_local",
        "rate_to_gbp",
        "annual_premium_gbp",
        "gbp_rate_missing",
        "annual_premium_derived",
    )
    return attach_lead_keys(out, leads), {"agent_key": u_agent}


def fact_spend(spend: DataFrame, dims: dict[str, DataFrame]):
    df, u_campaign = resolve_key(
        spend,
        "campaign_id",
        dims["campaign"],
        "campaign_id",
        "campaign_key",
        unknown=True,
        label="fact_spend.campaign",
    )
    df, u_channel = resolve_key(
        df,
        "channel_code",
        dims["channel"],
        "channel_code",
        "channel_key",
        unknown=True,
        label="fact_spend.channel",
    )
    df, _ = resolve_key(df, "region_code", dims["region"], "region_code", "region_key",
                        unknown=False, label="fact_spend.region")  # fmt: skip
    out = df.select(
        date_key("spend_date").alias("date_key"),
        "campaign_key",
        "channel_key",
        "region_key",
        "currency_code",
        "spend_local",
        "rate_to_gbp",
        "spend_gbp",
        "impressions",
        "clicks",
        "minor_unit_rescaled",
        "outside_campaign_dates",
    )
    return out, {"campaign_key": u_campaign, "channel_key": u_channel}


# ------------------------------------------------------------------ builder


def build_facts(spark: SparkSession, config: dict) -> list[FactResult]:
    t = lambda name: read_table(spark, config, name)  # noqa: E731
    dims = {d: t(f"gold.dim_{d}") for d in ("campaign", "channel", "region", "product", "agent")}
    dim_date = t("gold.dim_date")
    dedup_map = t("silver.lead_dedup_map")
    raw_lead_ids = latest_batch(t("bronze.leads_raw")).select("lead_id").distinct()
    lead, calls, policy, spend = (t("silver.lead"), t("silver.call"), t("silver.policy"),
                                  t("silver.marketing_spend"))  # fmt: skip
    check_date_keys(lead, ["created_at_utc"], dim_date, "silver.lead")
    check_date_keys(calls, ["call_start_ts_utc"], dim_date, "silver.call")
    check_date_keys(policy, ["application_date", "policy_start_date", "cancellation_date"],
                    dim_date, "silver.policy")  # fmt: skip
    check_date_keys(spend, ["spend_date"], dim_date, "silver.marketing_spend")

    lead_fact = fact_lead(lead, dims, t("silver.ref_region"))
    plan = [
        ("gold.fact_lead", "silver.lead", lead, lead_fact),
        ("gold.fact_call", "silver.call", calls, fact_call(calls, dims, dedup_map, raw_lead_ids)),
        ("gold.fact_policy", "silver.policy", policy,
         fact_policy(policy, dims, dedup_map, raw_lead_ids, calls, t("silver.fx_rate"),
                     lead_fact[0])),
        ("gold.fact_marketing_spend_daily", "silver.marketing_spend", spend,
         fact_spend(spend, dims)),
    ]  # fmt: skip
    results = []
    for table, source, silver, (gold, unknown) in plan:
        overwrite_table(spark, gold, config, table)
        rows_in, rows_out = silver.count(), t(table).count()
        _check(table, rows_in, rows_out)
        flagged = {}
        if table == "gold.fact_policy":
            missing = t(table).where("gbp_rate_missing").select(
                "policy_id",
                F.format_string("no %s rate for %s", F.col("currency_code"),
                                F.date_format("application_date", "yyyy-MM")).alias("detail"),
            )  # fmt: skip
            flagged["policy_fx_rate_missing"] = write_dq_exception(
                missing, "policy_fx_rate_missing", "silver.policy", "policy_id", "warning",
                config=config, action="flagged", detail_col="detail",
            )  # fmt: skip
        if table == "gold.fact_marketing_spend_daily":
            gbp_in = silver.agg(F.sum("spend_gbp")).first()[0]
            gbp_out = t(table).agg(F.sum("spend_gbp")).first()[0]
            if gbp_in != gbp_out:
                raise ReconciliationError(f"{table}: GBP {gbp_out} != silver {gbp_in}")
        results.append(FactResult(table, source, rows_in, rows_out, unknown, flagged))
        log.info("Reconciled %s: %d rows from %s; UNKNOWN %s; flagged %s",
                 table, rows_out, source, unknown, flagged)  # fmt: skip
    return results
