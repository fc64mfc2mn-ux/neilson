"""Union both call files into one typed frame: units in seconds, timestamps in UTC.

The two Aurora Dialler files differ in format (defect 14):

| File              | Timestamps                         | Durations |
| ----------------- | ---------------------------------- | --------- |
| `calls_uk_au.csv` | ISO 8601 with `Z`, seconds         | seconds   |
| `calls_ca_us.csv` | `dd/MM/yyyy HH:mm`, minutes        | minutes   |

Both carry the same operational columns (SLA flag, wrap-up code, QA score,
compliance flag, queue wait); only UK/AU has `call_notes`. A timestamp in the
other file's format is not guessed: it parses to null and is quarantined by
the caller.

Timezone per region (assumption 5, defect 15) comes from config
`calls.source_timezone`. UK, CA and US are `UTC` (kept as stored): they fit UTC
on first-call latency and on policy dates. AU is `Australia/Sydney`: AU call
times are local wall-clock time, converted to UTC DST-aware (policy application
dates match the converted call date for all 426 AU policies). Any IANA zone
works; `UTC` keeps the stored value, which is the reversal route.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SOURCE_UK_AU = "calls_uk_au.csv"
SOURCE_CA_US = "calls_ca_us.csv"
DEFAULT_SOURCE_TIMEZONE = {"UK": "UTC", "AU": "Australia/Sydney", "CA": "UTC", "US": "UTC"}
ISO_UTC = "yyyy-MM-dd'T'HH:mm:ssX"
DAY_FIRST = "dd/MM/yyyy HH:mm"

OUTPUT_COLUMNS = (
    "call_id",
    "lead_id",
    "agent_id",
    "region_code",
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
    "qa_score",
    "compliance_fail_flag",
    "recording_url",
    "call_notes",
    "duration_unit_raw",
    "source_file",
    "source_row",
)


def _seconds(column: str) -> Column:
    return F.round(F.col(column).cast("double")).cast("int")


def _minutes_to_seconds(column: str) -> Column:
    return F.round(F.col(column).cast("double") * 60).cast("int")


def _flag(column: str) -> Column:
    """Source flags arrive as `1.0` / `0.0`; anything else stays null."""
    value = F.col(column).cast("double")
    return F.when(value == 1, True).when(value == 0, False)


def _common(df: DataFrame) -> list[Column]:
    return [
        F.col("queue_wait_seconds").cast("double").cast("int").alias("queue_wait_seconds"),
        "call_direction",
        "disposition",
        "wrap_up_code",
        F.col("attempt_number").cast("double").cast("int").alias("attempt_number"),
        _flag("answered_within_sla_flag").alias("answered_within_sla_flag"),
        F.col("qa_score").cast("double").alias("qa_score"),
        _flag("compliance_fail_flag").alias("compliance_fail_flag"),
        "recording_url",
    ]


def type_uk_au(df: DataFrame) -> DataFrame:
    return df.select(
        "call_id",
        "lead_id",
        "agent_id",
        "region_code",
        F.to_timestamp("call_start_ts_utc", ISO_UTC).alias("call_start_ts_utc"),
        F.to_timestamp("call_end_ts_utc", ISO_UTC).alias("call_end_ts_utc"),
        _seconds("duration_seconds").alias("duration_seconds"),
        _seconds("talk_time_seconds").alias("talk_time_seconds"),
        _seconds("hold_time_seconds").alias("hold_time_seconds"),
        *_common(df),
        "call_notes",
        F.lit("seconds").alias("duration_unit_raw"),
        F.col("_source_file").alias("source_file"),
        F.col("_source_row").cast("long").alias("source_row"),
    )


def type_ca_us(df: DataFrame) -> DataFrame:
    return df.select(
        "call_id",
        "lead_id",
        "agent_id",
        "region_code",
        F.to_timestamp("call_start_ts", DAY_FIRST).alias("call_start_ts_utc"),
        F.to_timestamp("call_end_ts", DAY_FIRST).alias("call_end_ts_utc"),
        _minutes_to_seconds("duration_minutes").alias("duration_seconds"),
        _minutes_to_seconds("talk_time_minutes").alias("talk_time_seconds"),
        _minutes_to_seconds("hold_time_minutes").alias("hold_time_seconds"),
        *_common(df),
        F.lit(None).cast("string").alias("call_notes"),
        F.lit("minutes").alias("duration_unit_raw"),
        F.col("_source_file").alias("source_file"),
        F.col("_source_row").cast("long").alias("source_row"),
    )


def to_utc(df: DataFrame, source_timezone: dict[str, str]) -> DataFrame:
    """Convert each region's timestamps from its declared source zone to UTC."""
    local = [(r, tz) for r, tz in source_timezone.items() if tz.upper() != "UTC"]
    if not local:
        return df
    zone = F.create_map(*[F.lit(v) for pair in local for v in pair])[F.col("region_code")]
    for c in ("call_start_ts_utc", "call_end_ts_utc"):
        df = df.withColumn(
            c, F.when(zone.isNotNull(), F.to_utc_timestamp(c, zone)).otherwise(F.col(c))
        )
    return df


def normalise_calls(
    calls_uk_au: DataFrame,
    calls_ca_us: DataFrame,
    source_timezone: dict[str, str] | None = None,
) -> DataFrame:
    """Type both raw call frames, union them and put timestamps in UTC.

    No rows are dropped here: duplicates and impossible times are handled by
    `src/silver/calls.py`, which records every removal in silver.dq_exception.
    """
    unioned = type_uk_au(calls_uk_au).unionByName(type_ca_us(calls_ca_us))
    return to_utc(unioned, source_timezone or DEFAULT_SOURCE_TIMEZONE).select(*OUTPUT_COLUMNS)
