"""Apply the PII handling policy when building gold.dim_lead.

`gold.dim_lead` must: hash `email`, `phone` and `date_of_birth` with SHA-256
and a salt read from config, not literal; drop `first_name` and `last_name`;
replace `date_of_birth` with `age_band` in 10-year bands; truncate `postcode`
to the outward code; exclude `smoker_flag` and `health_condition_flag` from
the table served to `Marketing_Analyst_NoPII`. `call_notes` and
`recording_url` must not reach any gold table.
"""

from __future__ import annotations

from pyspark.sql import DataFrame


def apply_pii_policy(leads: DataFrame, salt: str) -> DataFrame:
    raise NotImplementedError
