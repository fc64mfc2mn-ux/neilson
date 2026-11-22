"""Silver calls: silver.call from bronze.calls_raw (both Aurora Dialler files).

Order on the latest bronze batch of each call file:
1. Exact duplicates (defect 13): every repeat of a byte-identical row (same
   file, `call_id` and `_row_hash`) after the first is quarantined, info. A
   `call_id` repeated with different content has no rule and stops the stage.
2. Typing and union (defect 14, src/silver/normalise_calls.py): seconds, UTC,
   per-region source timezone from config (assumption 5).
3. Quarantine rules, first match wins (`classify_quarantine`):
   - `call_timestamp_unparseable` (defect 14, error): start or end not parsed.
   - `call_end_before_start` (defect 16, error): physically impossible.
   - `call_start_before_lead` (defect 16, warning): the call starts before its
     own lead record was created. The call may be real and the lead time wrong,
     hence warning, but it cannot be placed in the funnel.
4. Flags that keep the call (on rows kept in silver):
   - `call_lead_not_found` (defect 17): lead id in no lead file. Kept; gold maps
     it to the UNKNOWN lead (assumption 20).
   - `call_lead_quarantined` (defect 17): lead exists but was quarantined at
     step 7. Kept, same treatment.
   - `call_agent_not_found` (defect 17): agent not on the roster; UNKNOWN agent.
   - `call_au_timestamp_unverified` (defect 15): region in
     `calls.timestamp_unverified_regions`; kept as stored (assumption 5).
   - `call_after_do_not_call` (defect 26): a later call on a lead that already
     had a DO_NOT_CALL disposition.

Reconciliation, by row:   rows in = silver.call rows + rows quarantined
and by call id:           distinct ids in = silver.call rows + rule 3 rows.
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
from src.silver.normalise_calls import (
    DEFAULT_SOURCE_TIMEZONE,
    ISO_UTC,
    SOURCE_CA_US,
    SOURCE_UK_AU,
    normalise_calls,
)

log = logging.getLogger("northgate.silver")

CALLS_SOURCE = "bronze.calls_raw"
LEADS_SOURCE = "bronze.leads_raw"
# Quarantine rules in precedence order, with severity.
QUARANTINE_RULES = (
    ("call_timestamp_unparseable", "error"),
    ("call_end_before_start", "error"),
    ("call_start_before_lead", "warning"),
)


@dataclass(frozen=True)
class CallReconciliation:
    rows_in: int
    rows_in_by_file: dict[str, int]
    distinct_ids_in: int
    removed_by_file: dict[str, dict[str, int]]
    rows_quarantined: int
    rows_out: int


# ------------------------------------------------------------------ pure checks


def exact_duplicate_copies(raw: DataFrame) -> DataFrame:
    """Second and later copies of byte-identical rows, per file and `call_id`.

    Raises ReconciliationError if a `call_id` repeats in a file with different
    content: there is no rule for choosing between versions.
    """
    conflicts = (
        raw.groupBy("_source_file", "call_id")
        .agg(F.countDistinct("_row_hash").alias("_versions"))
        .where("_versions > 1")
    )
    sample = [r.call_id for r in conflicts.limit(5).collect()]
    if sample:
        raise ReconciliationError(f"{CALLS_SOURCE}: call_id with different content: {sample}")
    first = Window.partitionBy("_source_file", "call_id", "_row_hash").orderBy("_source_row")
    return raw.withColumn("_copy", F.row_number().over(first)).where("_copy > 1").drop("_copy")


def classify_quarantine(calls: DataFrame, leads: DataFrame) -> DataFrame:
    """Add `quarantine_rule` (first matching rule, or null) and `lead_created_at_utc`."""
    joined = calls.join(leads.select("lead_id", "lead_created_at_utc"), "lead_id", "left")
    start, end = F.col("call_start_ts_utc"), F.col("call_end_ts_utc")
    rule = (
        F.when(start.isNull() | end.isNull(), "call_timestamp_unparseable")
        .when(end < start, "call_end_before_start")
        .when(start < F.col("lead_created_at_utc"), "call_start_before_lead")
    )
    return joined.withColumn("quarantine_rule", rule)


def lead_match(calls: DataFrame, leads: DataFrame, dedup_map: DataFrame) -> DataFrame:
    """Add `lead_match`: `found` (in silver.lead_dedup_map), `lead_quarantined`
    (in the lead file but quarantined at step 7) or `not_found`."""
    in_map = dedup_map.select(F.col("source_lead_id").alias("lead_id"), F.lit(True).alias("_m"))
    in_file = leads.select("lead_id", F.lit(True).alias("_f")).distinct()
    return (
        calls.join(in_map, "lead_id", "left")
        .join(in_file, "lead_id", "left")
        .withColumn(
            "lead_match",
            F.when(F.col("_m"), "found")
            .when(F.col("_f"), "lead_quarantined")
            .otherwise("not_found"),
        )
        .drop("_m", "_f")
    )


def calls_after_do_not_call(calls: DataFrame) -> DataFrame:
    """Calls starting after the lead's first DO_NOT_CALL disposition."""
    by_lead = Window.partitionBy("lead_id")
    dnc_ts = F.when(F.col("disposition") == "DO_NOT_CALL", F.col("call_start_ts_utc"))
    return calls.withColumn("first_do_not_call_ts_utc", F.min(dnc_ts).over(by_lead)).where(
        F.col("call_start_ts_utc") > F.col("first_do_not_call_ts_utc")
    )


def lead_created_times(leads_raw: DataFrame) -> DataFrame:
    """Creation time per lead id in the lead file (earliest, if a PK conflict repeats it)."""
    return leads_raw.groupBy("lead_id").agg(
        F.min(F.to_timestamp("created_at_utc", ISO_UTC)).alias("lead_created_at_utc")
    )


def _by_file(df: DataFrame, file_col: str) -> dict[str, int]:
    return {r[0]: r[1] for r in df.groupBy(file_col).count().collect()}


# ------------------------------------------------------------------ builder


def build_calls(spark: SparkSession, config: dict) -> CallReconciliation:
    call_config = config.get("calls", {})
    source_timezone = {**DEFAULT_SOURCE_TIMEZONE, **call_config.get("source_timezone", {})}
    unverified_regions = list(call_config.get("timestamp_unverified_regions", []))

    raw = latest_batch(read_table(spark, config, CALLS_SOURCE)).cache()
    rows_in_by_file = _by_file(raw, "_source_file")
    distinct_ids_in = raw.select("call_id").distinct().count()
    removed: dict[str, dict[str, int]] = {}

    # 1. Exact duplicates.
    dups = (
        exact_duplicate_copies(raw)
        .withColumn("detail", F.concat(F.lit("repeat at source row "), F.col("_source_row")))
        .cache()
    )
    removed["call_exact_duplicate"] = _by_file(dups, "_source_file")
    write_dq_exception(dups, "call_exact_duplicate", CALLS_SOURCE, "call_id", "info",
                       config=config, detail_col="detail")  # fmt: skip
    rest = raw.join(dups.select("_source_file", "_source_row"),
                    ["_source_file", "_source_row"], "left_anti")  # fmt: skip

    # 2. Type, union, UTC.
    typed = normalise_calls(
        rest.where(F.col("_source_file") == SOURCE_UK_AU),
        rest.where(F.col("_source_file") == SOURCE_CA_US),
        source_timezone,
    )

    # 3. Quarantine rules.
    leads_raw = latest_batch(read_table(spark, config, LEADS_SOURCE))
    lead_created = lead_created_times(leads_raw)
    classified = classify_quarantine(typed, lead_created).cache()
    minutes_before_lead = F.round(
        (F.unix_timestamp("lead_created_at_utc") - F.unix_timestamp("call_start_ts_utc")) / 60, 1
    )
    detail = (
        F.when(
            F.col("quarantine_rule") == "call_end_before_start",
            F.concat(
                F.lit("ends seconds before start: "),
                (F.unix_timestamp("call_start_ts_utc") - F.unix_timestamp("call_end_ts_utc")).cast(
                    "string"
                ),
            ),
        )  # fmt: skip
        .when(
            F.col("quarantine_rule") == "call_start_before_lead",
            F.concat(F.lit("minutes before lead: "), minutes_before_lead.cast("string")),
        )
        .otherwise(F.lit(None))
    )
    classified = classified.withColumn("detail", detail)
    for rule, severity in QUARANTINE_RULES:
        hit = classified.where(F.col("quarantine_rule") == rule)
        n = write_dq_exception(hit, rule, CALLS_SOURCE, "call_id", severity,
                               config=config, detail_col="detail")  # fmt: skip
        if n:
            removed[rule] = _by_file(hit, "source_file")
    kept = classified.where(F.col("quarantine_rule").isNull()).drop(
        "quarantine_rule", "lead_created_at_utc", "detail"
    )

    # 4. Flags that keep the call.
    dedup_map = read_table(spark, config, "silver.lead_dedup_map")
    matched = lead_match(kept, lead_created, dedup_map).cache()
    for status, check in (("not_found", "call_lead_not_found"),
                          ("lead_quarantined", "call_lead_quarantined")):  # fmt: skip
        write_dq_exception(matched.where(F.col("lead_match") == status), check, CALLS_SOURCE,
                           "call_id", "info", config=config, action="flagged")  # fmt: skip

    roster = read_table(spark, config, "silver.agent").select("agent_id").distinct()
    ghost_agents = kept.join(roster, "agent_id", "left_anti").withColumn(
        "detail", F.col("agent_id")
    )
    write_dq_exception(ghost_agents, "call_agent_not_found", CALLS_SOURCE, "call_id", "info",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip

    unverified = kept.where(F.col("region_code").isin(unverified_regions)).withColumn(
        "detail", F.lit("timestamps kept as stored; first-call offset unexplained (assumption 5)")
    )
    write_dq_exception(unverified, "call_au_timestamp_unverified", CALLS_SOURCE, "call_id",
                       "warning", config=config, action="flagged", detail_col="detail")  # fmt: skip

    after_dnc = calls_after_do_not_call(kept).withColumn(
        "detail", F.concat(F.lit("first DO_NOT_CALL at "), F.col("first_do_not_call_ts_utc"))
    )
    write_dq_exception(after_dnc, "call_after_do_not_call", CALLS_SOURCE, "call_id", "warning",
                       config=config, action="flagged", detail_col="detail")  # fmt: skip

    silver = (
        kept.join(
            after_dnc.select("call_id", F.lit(True).alias("after_do_not_call")), "call_id", "left"
        )  # fmt: skip
        .withColumn("after_do_not_call", F.coalesce("after_do_not_call", F.lit(False)))
        .withColumn("start_ts_offset_unverified", F.col("region_code").isin(unverified_regions))
    )
    overwrite_table(spark, silver, config, "silver.call")

    rec = CallReconciliation(
        rows_in=sum(rows_in_by_file.values()),
        rows_in_by_file=rows_in_by_file,
        distinct_ids_in=distinct_ids_in,
        removed_by_file=removed,
        rows_quarantined=quarantined_count(spark, config, CALLS_SOURCE),
        rows_out=read_table(spark, config, "silver.call").count(),
    )
    for df in (raw, dups, classified, matched):
        df.unpersist()
    assert_reconciled("silver.call", rec.rows_in, rec.rows_out, rec.rows_quarantined)
    rule_rows = sum(n for rule, files in removed.items() if rule != "call_exact_duplicate"
                    for n in files.values())  # fmt: skip
    if rec.distinct_ids_in != rec.rows_out + rule_rows:
        raise ReconciliationError(
            f"silver.call: {rec.distinct_ids_in} distinct call ids != {rec.rows_out} out + "
            f"{rule_rows} quarantined by rule"
        )
    log.info("silver.call: %s", rec)
    return rec
