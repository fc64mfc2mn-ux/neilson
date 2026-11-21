"""Land all twelve input files into bronze.

Bronze is append-only, all columns typed as string, plus `_ingest_ts`,
`_source_file` and `_row_hash`. No cleansing in bronze. Must produce
`bronze.leads_raw`, `bronze.consent_events_raw`, `bronze.calls_raw`,
`bronze.policies_raw`, `bronze.marketing_spend_raw`, `bronze.campaigns_raw`,
`bronze.agents_raw`, `bronze.fx_rates_raw`, `bronze.ref_channel_raw`,
`bronze.ref_region_raw` and `bronze.sec_user_region_raw`. `bronze.calls_raw`
must carry both call files, with `_source_file` distinguishing them.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def ingest_all(spark: SparkSession, config: dict) -> None:
    raise NotImplementedError
