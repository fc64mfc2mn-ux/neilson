"""Deduplicate leads into silver.lead and silver.lead_dedup_map.

Rule (CLAUDE.md, docs/ASSUMPTIONS.md): two leads are the same person when they
share a normalised email (trimmed, lower case) or a normalised phone (digits
only) and were created within 48 hours of each other. Links chain: per key, a
lead is linked to the previous lead with that key when the gap is at most 48
hours, and links through either key join leads into one cluster (connected
components). The earliest `created_at_utc` in a cluster survives, `lead_id`
breaking ties.

`silver.lead_dedup_map` has one row per lead in scope: `source_lead_id`,
`surviving_lead_id`, `match_rule` and `cluster_size`. A survivor maps to
itself (`match_rule = 'self'`). Otherwise `match_rule` names the key shared
with the survivor (`email`, `phone`, `email+phone`), or `transitive` when the
lead only reaches the survivor through another lead.

The source `duplicate_of_lead_id` flag is not used: it is incomplete, and is
compared with this rule in src/sql/findings_leads.sql. Callers quarantine
primary-key conflicts and leads with neither email nor phone first; a lead
with only one of the two keys is matched on the key it has.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

WINDOW_HOURS = 48
MAX_LINKS = 1_000_000  # links are collected to the driver; fail loudly if implausible


def normalise_email(column: str) -> Column:
    value = F.lower(F.trim(F.col(column)))
    return F.when(value == "", None).otherwise(value)


def normalise_phone(column: str) -> Column:
    value = F.regexp_replace(F.col(column), r"\D", "")
    return F.when(value == "", None).otherwise(value)


def _links(leads: DataFrame) -> DataFrame:
    """(lead_id, previous lead_id) pairs: same key, at most 48 hours apart."""
    keyed = leads.select(
        "lead_id",
        "created_at_utc",
        F.explode(
            F.array(
                F.when(F.col("email_norm").isNotNull(), F.concat(F.lit("e:"), "email_norm")),
                F.when(F.col("phone_norm").isNotNull(), F.concat(F.lit("p:"), "phone_norm")),
            )
        ).alias("key"),
    ).where(F.col("key").isNotNull())
    ordered = Window.partitionBy("key").orderBy("created_at_utc", "lead_id")
    lagged = keyed.withColumn("prev_id", F.lag("lead_id").over(ordered)).withColumn(
        "prev_ts", F.lag("created_at_utc").over(ordered)
    )
    gap = F.unix_timestamp("created_at_utc") - F.unix_timestamp("prev_ts")
    return lagged.where(F.col("prev_id").isNotNull() & (gap <= WINDOW_HOURS * 3600)).select(
        "lead_id", "prev_id"
    )


def _components(links: list[tuple[str, str]]) -> dict[str, str]:
    """Union-find: lead_id -> component representative, for linked leads only."""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in links:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    return {x: find(x) for x in parent}


def dedupe_leads(leads: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (survivors, lead_dedup_map).

    `leads` needs `lead_id`, `created_at_utc`, `email` and `phone`; `lead_id`
    must be unique. Survivors keep every input column, plus `email_norm`,
    `phone_norm` and `dedup_cluster_size`.
    """
    spark = leads.sparkSession
    normed = leads.withColumn("email_norm", normalise_email("email")).withColumn(
        "phone_norm", normalise_phone("phone")
    )
    links = _links(normed)
    if links.count() > MAX_LINKS:
        raise ValueError(f"More than {MAX_LINKS} duplicate links; review the dedup rule")
    groups = _components([(r.lead_id, r.prev_id) for r in links.collect()])
    group_df = spark.createDataFrame(
        list(groups.items()) or [("", "")], "lead_id string, _group string"
    ).where(F.col("lead_id") != "")

    labelled = normed.join(group_df, "lead_id", "left").withColumn(
        "_group", F.coalesce("_group", "lead_id")
    )
    by_group = Window.partitionBy("_group")
    ranked = (
        labelled.withColumn(
            "_rank", F.row_number().over(by_group.orderBy("created_at_utc", "lead_id"))
        )
        .withColumn("dedup_cluster_size", F.count(F.lit(1)).over(by_group))
        .withColumn(
            "surviving_lead_id",
            F.first("lead_id").over(by_group.orderBy("created_at_utc", "lead_id")),
        )
    )
    survivor_keys = ranked.where("_rank = 1").select(
        F.col("lead_id").alias("surviving_lead_id"),
        F.col("email_norm").alias("_s_email"),
        F.col("phone_norm").alias("_s_phone"),
    )
    with_rule = ranked.join(survivor_keys, "surviving_lead_id")
    same_email = F.col("email_norm").isNotNull() & (F.col("email_norm") == F.col("_s_email"))
    same_phone = F.col("phone_norm").isNotNull() & (F.col("phone_norm") == F.col("_s_phone"))
    rule = (
        F.when(F.col("_rank") == 1, "self")
        .when(same_email & same_phone, "email+phone")
        .when(same_email, "email")
        .when(same_phone, "phone")
        .otherwise("transitive")
    )
    mapping = with_rule.select(
        F.col("lead_id").alias("source_lead_id"),
        "surviving_lead_id",
        rule.alias("match_rule"),
        F.col("dedup_cluster_size").cast("int").alias("cluster_size"),
    )
    survivors = ranked.where("_rank = 1").drop("_rank", "_group", "surviving_lead_id")
    return survivors, mapping
