"""A minutes-denominated row and a seconds-denominated row for the same
logical duration produce equal `duration_seconds`, and an AU timestamp
shifts by the correct offset."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.silver.normalise_calls import (
    DEFAULT_SOURCE_TIMEZONE,
    SOURCE_CA_US,
    SOURCE_UK_AU,
    normalise_calls,
)

UK_AU_COLUMNS = (
    "call_id lead_id agent_id region_code call_start_ts_utc call_end_ts_utc duration_seconds "
    "talk_time_seconds hold_time_seconds queue_wait_seconds call_direction disposition "
    "wrap_up_code attempt_number answered_within_sla_flag qa_score compliance_fail_flag "
    "recording_url call_notes _source_file _source_row"
).split()
CA_US_COLUMNS = (
    "call_id lead_id agent_id region_code call_start_ts call_end_ts duration_minutes "
    "talk_time_minutes hold_time_minutes queue_wait_seconds call_direction disposition "
    "wrap_up_code attempt_number answered_within_sla_flag qa_score compliance_fail_flag "
    "recording_url _source_file _source_row"
).split()


def uk_au_row(call_id, region, start, end, duration, talk="0", hold="0", **kw):
    row = dict.fromkeys(UK_AU_COLUMNS)
    row.update(call_id=call_id, lead_id="L1", agent_id="AG0001", region_code=region,
               call_start_ts_utc=start, call_end_ts_utc=end, duration_seconds=duration,
               talk_time_seconds=talk, hold_time_seconds=hold, call_direction="OUTBOUND",
               disposition="CONNECTED", wrap_up_code="WR-X", attempt_number="1",
               _source_file=SOURCE_UK_AU, _source_row="1")  # fmt: skip
    row.update(kw)
    return row


def ca_us_row(call_id, region, start, end, minutes, talk="0.0", hold="0.0", **kw):
    row = dict.fromkeys(CA_US_COLUMNS)
    row.update(call_id=call_id, lead_id="L2", agent_id="AG0002", region_code=region,
               call_start_ts=start, call_end_ts=end, duration_minutes=minutes,
               talk_time_minutes=talk, hold_time_minutes=hold, call_direction="INBOUND",
               disposition="CONNECTED", wrap_up_code="WR-Y", attempt_number="2",
               _source_file=SOURCE_CA_US, _source_row="1")  # fmt: skip
    row.update(kw)
    return row


def frames(spark, uk_rows, ca_rows):
    schema = lambda cols: ", ".join(f"{c} string" for c in cols)  # noqa: E731
    uk = spark.createDataFrame([[r[c] for c in UK_AU_COLUMNS] for r in uk_rows],
                               schema(UK_AU_COLUMNS))  # fmt: skip
    ca = spark.createDataFrame([[r[c] for c in CA_US_COLUMNS] for r in ca_rows],
                               schema(CA_US_COLUMNS))  # fmt: skip
    return uk, ca


def by_id(df):
    """Rows by call id, timestamps as UTC strings formatted in Spark.

    collect() would turn timestamps into the host's local time; the session is UTC.
    """
    for c in ("call_start_ts_utc", "call_end_ts_utc"):
        df = df.withColumn(c, F.date_format(c, "yyyy-MM-dd HH:mm"))
    return {r.call_id: r for r in df.collect()}


def test_normalise_calls_units_and_timezone(spark):
    uk, ca = frames(
        spark,
        [
            uk_au_row("S", "UK", "2026-05-10T09:00:00Z", "2026-05-10T09:02:12Z", "132", "90", "12"),
            uk_au_row("AU", "AU", "2026-05-10T09:00:00Z", "2026-05-10T09:01:00Z", "60"),
            uk_au_row("AU_DST", "AU", "2026-04-02T09:00:00Z", "2026-04-02T09:01:00Z", "60"),
        ],
        [ca_us_row("M", "CA", "10/05/2026 09:00", "10/05/2026 09:02", "2.2", "1.5", "0.2")],
    )
    out = by_id(normalise_calls(uk, ca))

    # Units: 2.2 minutes and 132 seconds are the same duration.
    assert out["M"].duration_seconds == out["S"].duration_seconds == 132
    assert (out["M"].talk_time_seconds, out["M"].hold_time_seconds) == (90, 12)
    assert (out["S"].talk_time_seconds, out["S"].hold_time_seconds) == (90, 12)
    assert (out["M"].duration_unit_raw, out["S"].duration_unit_raw) == ("minutes", "seconds")

    # Formats: dd/MM/yyyy (10 May, not 5 October) and ISO with Z.
    assert out["M"].call_start_ts_utc == "2026-05-10 09:00"
    assert out["S"].call_start_ts_utc == "2026-05-10 09:00"

    # Decided (assumption 5): every region, AU included, is kept as stored UTC.
    assert set(DEFAULT_SOURCE_TIMEZONE.values()) == {"UTC"}
    assert out["AU"].call_start_ts_utc == "2026-05-10 09:00"

    # The reversal route: declaring AU as Sydney local shifts by the correct offset,
    # AEST +10 in May and AEDT +11 before DST ended on 5 April 2026. UK is untouched.
    sydney = by_id(normalise_calls(uk, ca, {**DEFAULT_SOURCE_TIMEZONE, "AU": "Australia/Sydney"}))
    assert sydney["AU"].call_start_ts_utc == "2026-05-09 23:00"
    assert sydney["AU"].call_end_ts_utc == "2026-05-09 23:01"
    assert sydney["AU_DST"].call_start_ts_utc == "2026-04-01 22:00"
    assert sydney["S"].call_start_ts_utc == "2026-05-10 09:00"


def test_ca_us_keeps_its_own_sla_wrap_up_and_compliance_columns(spark):
    # Trap in the earlier attempt: these exist in calls_ca_us.csv and were nulled,
    # which would blank Grade of Service for CA and US.
    uk, ca = frames(
        spark,
        [
            uk_au_row(
                "S",
                "UK",
                "2026-05-10T09:00:00Z",
                "2026-05-10T09:01:00Z",
                "60",
                answered_within_sla_flag="0.0",
                qa_score="58.0",
                compliance_fail_flag="0.0",
                queue_wait_seconds="29.0",
                call_notes="note",
            )
        ],  # fmt: skip
        [
            ca_us_row(
                "M",
                "US",
                "10/05/2026 09:00",
                "10/05/2026 09:01",
                "1.0",
                answered_within_sla_flag="1.0",
                qa_score="42.0",
                compliance_fail_flag="1.0",
                queue_wait_seconds="58.0",
            )
        ],  # fmt: skip
    )
    out = by_id(normalise_calls(uk, ca))
    assert (out["M"].wrap_up_code, out["M"].answered_within_sla_flag) == ("WR-Y", True)
    assert (out["M"].compliance_fail_flag, out["M"].queue_wait_seconds) == (True, 58)
    assert (out["M"].qa_score, out["M"].attempt_number, out["M"].call_notes) == (42.0, 2, None)
    assert (out["S"].answered_within_sla_flag, out["S"].compliance_fail_flag) == (False, False)
    assert (out["S"].qa_score, out["S"].queue_wait_seconds, out["S"].call_notes) == (
        58.0, 29, "note"
    )  # fmt: skip
    assert (out["M"].source_file, out["S"].source_file) == (SOURCE_CA_US, SOURCE_UK_AU)


def test_unparseable_timestamp_is_null_not_guessed(spark):
    uk, ca = frames(
        spark,
        [uk_au_row("S", "UK", "10/05/2026 09:00", "2026-05-10T09:01:00Z", "60")],
        [ca_us_row("M", "CA", "2026-05-10T09:00:00Z", "10/05/2026 09:01", "1.0")],
    )
    out = by_id(normalise_calls(uk, ca))
    assert out["S"].call_start_ts_utc is None and out["M"].call_start_ts_utc is None
