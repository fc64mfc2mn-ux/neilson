"""Resolve consent_events.json into silver.consent_current.

Some leads have more than one consent event, so latest-event resolution by
`event_ts_utc` is required to produce one current consent state per lead.
Must expose a current phone-consent flag per lead, since it is required by
`gold.fact_lead_funnel`'s `consent_phone_current_flag` column and by the
consent-compliance governance check.
"""

from __future__ import annotations

from pyspark.sql import DataFrame


def resolve_consent(consent_events: DataFrame) -> DataFrame:
    raise NotImplementedError
