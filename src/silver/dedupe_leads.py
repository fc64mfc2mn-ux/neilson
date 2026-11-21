"""Deduplicate leads into silver.lead and silver.lead_dedup_map.

Must deduplicate near-duplicate leads (same identity key, different
`lead_id`, created within 48 hours of each other) to one surviving lead per
cluster, keeping the earliest `created_at_utc`, and keep the rest in a
mapping table. `silver.lead_dedup_map` has `source_lead_id`,
`surviving_lead_id`, `match_rule` and `cluster_size`. The source
`duplicate_of_lead_id` flag is incomplete: the rest of the clusters must be
derived, with the rule stated. Leads with a null `email` or `phone` need a
fallback identity key, and leads with both null cannot be deduplicated and
must be quarantined or flagged. A true `lead_id` primary-key violation (the
same id with conflicting `campaign_id` and `email`) must be quarantined, not
silently resolved.
"""

from __future__ import annotations

from pyspark.sql import DataFrame


def dedupe_leads(leads: DataFrame) -> tuple[DataFrame, DataFrame]:
    raise NotImplementedError
