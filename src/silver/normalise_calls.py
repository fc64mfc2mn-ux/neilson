"""Union both call files into one silver.call, normalised.

Both call files union into one `silver.call`, with durations normalised to
seconds and timestamps normalised to genuine UTC. The two source files differ
in delimiter, timestamp format, duration unit and available columns; both
must resolve to one common, correctly-typed schema before any downstream
measure is built on top of it.
"""

from __future__ import annotations

from pyspark.sql import DataFrame


def normalise_calls(calls_uk_au: DataFrame, calls_ca_us: DataFrame) -> DataFrame:
    raise NotImplementedError
