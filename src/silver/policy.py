"""Silver policy: silver.policy from bronze.policies_raw (PolicyCore).

Order on the latest bronze batch of policies.csv:
1. Exact duplicates (defect 19): every repeat of a byte-identical row after the
   first is quarantined, info.
2. Conflicting duplicates (defect 19): copies of one `policy_id` that differ.
   Merged into one row: for each field the earliest copy (lowest `_source_row`)
   wins a value-vs-value conflict, and a null is filled from a later copy. The
   earliest copy matches the independent evidence where there is some (the
   linked call's agent; annual = monthly x 12). Each later copy is quarantined,
   warning, with every differing field and its resolution in `detail`.
3. Typing. Null `lead_id` (defect 20) is kept and flagged: those policies are
   excluded from lead-attributed measures later (assumption 11).
4. Null annual premium (defect 21): derived as monthly x 12, action corrected.

Statuses are kept raw (INFORCE, LAPSED, NTU, CANCELLED): which count as
converted is a measure decision. DECLINED-yet-INFORCE (defect 22) is a finding
query only (src/sql/findings_policy.sql).

Reconciliation: rows in = silver.policy rows + rows quarantined, and
silver.policy rows = distinct policy ids in.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.bronze.ingest import latest_batch
from src.common.dq import (
    ReconciliationError,
    assert_reconciled,
    quarantined_count,
    write_dq_exception,
)
from src.common.tables import overwrite_table, read_table

log = logging.getLogger("northgate.silver")

POLICY_SOURCE = "bronze.policies_raw"
FIELDS = (
    "lead_id",
    "call_id",
    "agent_id",
    "region_code",
    "product_code",
    "application_date",
    "policy_start_date",
    "annual_premium_local",
    "monthly_premium_local",
    "payment_frequency",
    "currency_code",
    "sum_assured_local",
    "policy_status",
    "cancellation_date",
    "underwriting_decision",
)


@dataclass(frozen=True)
class PolicyReconciliation:
    rows_in: int
    distinct_ids_in: int
    exact_copies: int
    conflicting_removed: int
    rows_quarantined: int
    rows_out: int


# ------------------------------------------------------------------ pure steps


def split_duplicates(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(exact copies to remove, every remaining row of ids with conflicting copies)."""
    first = Window.partitionBy("policy_id", "_row_hash").orderBy("_source_row")
    ranked = raw.withColumn("_copy", F.row_number().over(first))
    exact = ranked.where("_copy > 1").drop("_copy")
    rest = ranked.where("_copy = 1").drop("_copy")
    by_id = Window.partitionBy("policy_id")
    conflicting = rest.withColumn("_n", F.count(F.lit(1)).over(by_id)).where("_n > 1").drop("_n")
    return exact, conflicting


def _value(c: str) -> F.Column:
    return F.coalesce(F.col(c).cast("string"), F.lit("NULL"))


def merge_conflicting(rows: DataFrame, fields: list[str] | tuple[str, ...] = FIELDS):
    """Merge copies of each policy_id: earliest non-null value per field wins.

    Returns (merged: one row per id, keeping the earliest copy's `_source_row`;
    removed: the later copies, with `detail` naming each differing field as
    `field: earliest vs later (kept x)` or `(filled)` when the earliest is null).
    """
    ordered = Window.partitionBy("policy_id").orderBy("_source_row")
    whole = ordered.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ranked = rows.withColumn("_rank", F.row_number().over(ordered))
    merged = (
        ranked.select(
            "*",
            *[F.first(c, ignorenulls=True).over(whole).alias(f"_m_{c}") for c in fields],
        )
        .where("_rank = 1")
        .drop("_rank", *fields)
    )
    for c in fields:
        merged = merged.withColumnRenamed(f"_m_{c}", c)

    earliest = ranked.where("_rank = 1").select(
        "policy_id",
        F.col("_source_row").alias("_kept_row"),
        *[F.col(c).alias(f"_e_{c}") for c in fields],
    )
    later = ranked.where("_rank > 1").join(earliest, "policy_id")
    parts = []
    for c in fields:
        e, v = F.col(f"_e_{c}"), F.col(c)
        text = F.concat(
            F.lit(f"{c}: "), _value(f"_e_{c}"), F.lit(" vs "), _value(c),
            F.when(e.isNull(), F.lit(" (filled)"))
            .otherwise(F.concat(F.lit(" (kept "), e.cast("string"), F.lit(")"))),
        )  # fmt: skip
        parts.append(F.when(~e.eqNullSafe(v), text))
    removed = later.withColumn("detail", F.concat_ws("; ", *parts)).drop(
        "_rank", "_kept_row", *[f"_e_{c}" for c in fields]
    )
    return merged.select(rows.columns), removed


def derive_annual_premium(df: DataFrame) -> DataFrame:
    """Annual premium = monthly x 12 where annual is null and monthly is present."""
    derive = F.col("annual_premium_local").isNull() & F.col("monthly_premium_local").isNotNull()
    return df.withColumn("annual_premium_derived", derive).withColumn(
        "annual_premium_local",
        F.when(derive, F.col("monthly_premium_local") * 12).otherwise(
            F.col("annual_premium_local")
        ),
    )


def conform_policies(df: DataFrame) -> DataFrame:
    money = lambda c: F.col(c).cast("decimal(14,2)").alias(c)  # noqa: E731
    return df.select(
        "policy_id",
        "lead_id",
        "call_id",
        "agent_id",
        "region_code",
        "product_code",
        F.to_date("application_date").alias("application_date"),
        F.to_date("policy_start_date").alias("policy_start_date"),
        money("annual_premium_local"),
        money("monthly_premium_local"),
        "payment_frequency",
        "currency_code",
        F.col("sum_assured_local").cast("decimal(16,2)").alias("sum_assured_local"),
        "policy_status",
        F.to_date("cancellation_date").alias("cancellation_date"),
        "underwriting_decision",
        "merged_from_duplicates",
        F.col("_source_row").alias("source_row"),
    )


# ------------------------------------------------------------------ builder


def build_policy(spark: SparkSession, config: dict) -> PolicyReconciliation:
    raw = latest_batch(read_table(spark, config, POLICY_SOURCE)).cache()
    blank = {c: F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)) for c in FIELDS}
    raw = raw.withColumns(blank)
    rows_in = raw.count()
    distinct_ids_in = raw.select("policy_id").distinct().count()

    # 1. Exact duplicates.
    exact, conflicting = split_duplicates(raw)
    exact = exact.withColumn("detail", F.concat(F.lit("repeat at source row "), "_source_row"))
    n_exact = write_dq_exception(exact, "policy_exact_duplicate", POLICY_SOURCE, "policy_id",
                                 "info", config=config, detail_col="detail")  # fmt: skip

    # 2. Conflicting duplicates: merge, quarantine the later copies with the differences.
    merged, removed = merge_conflicting(conflicting)
    removed = removed.withColumn(
        "detail", F.concat(F.lit("merged at source row "), "_source_row", F.lit(": "), "detail")
    )
    n_removed = write_dq_exception(removed, "policy_conflicting_duplicate", POLICY_SOURCE,
                                   "policy_id", "warning", config=config,
                                   detail_col="detail")  # fmt: skip
    singles = raw.join(exact.select("_source_row"), "_source_row", "left_anti").join(
        conflicting.select("policy_id").distinct(), "policy_id", "left_anti"
    )
    policies = singles.withColumn("merged_from_duplicates", F.lit(False)).unionByName(
        merged.withColumn("merged_from_duplicates", F.lit(True))
    )

    # 3. Type; flag missing leads.  4. Derive annual premium.
    policies = derive_annual_premium(conform_policies(policies)).cache()
    no_lead = policies.where(F.col("lead_id").isNull())
    write_dq_exception(no_lead, "policy_lead_missing", POLICY_SOURCE, "policy_id", "info",
                       config=config, action="flagged")  # fmt: skip
    derived = policies.where("annual_premium_derived").withColumn(
        "detail", F.concat(F.lit("monthly "), F.col("monthly_premium_local").cast("string"),
                           F.lit(" x 12"))
    )  # fmt: skip
    write_dq_exception(derived, "policy_annual_premium_derived", POLICY_SOURCE, "policy_id",
                       "info", config=config, action="corrected", detail_col="detail")  # fmt: skip
    overwrite_table(spark, policies, config, "silver.policy")

    rec = PolicyReconciliation(
        rows_in=rows_in,
        distinct_ids_in=distinct_ids_in,
        exact_copies=n_exact,
        conflicting_removed=n_removed,
        rows_quarantined=quarantined_count(spark, config, POLICY_SOURCE),
        rows_out=read_table(spark, config, "silver.policy").count(),
    )
    raw.unpersist()
    policies.unpersist()
    assert_reconciled("silver.policy", rec.rows_in, rec.rows_out, rec.rows_quarantined)
    if rec.rows_out != rec.distinct_ids_in:
        raise ReconciliationError(
            f"silver.policy: {rec.rows_out} rows != {rec.distinct_ids_in} distinct policy ids"
        )
    log.info("silver.policy: %s", rec)
    return rec
