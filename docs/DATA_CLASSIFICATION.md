# Gold column classification

Source for the column classification in `docs/GOVERNANCE.md`. Tiers are the brief's:
0 aggregate, 1 internal (no personal data), 2 confidential (personal data under UK GDPR),
3 restricted (special category, or reveals it). A hash of personal data is still personal
data (pseudonymised, not anonymised), so hashes are tier 2. Generalised values (age band,
outward postcode) are tier 1 by design: that is what the PII policy is for.

`tests/test_pii_policy.py::test_classification_covers_every_gold_column` checks this table
against the built gold schemas: every column listed once, nothing missing, no extra rows.
Step 12 adds the facts and bridges.

Not in gold at all (stay in silver): lead `first_name`, `last_name`, `email`, `phone`,
`postcode`, `date_of_birth`, `consent_timestamp_utc`, `consent_source`; call `call_notes`
and `recording_url` (tier 3); agent `agent_name`, `agent_email`, `manager_name`,
`hire_date`, `fte`.

| Table | Column | Tier | Treatment |
| --- | --- | --- | --- |
| `gold.dim_date` | `date_key` | 1 | Kept |
| `gold.dim_date` | `date` | 1 | Kept |
| `gold.dim_date` | `year` | 1 | Kept |
| `gold.dim_date` | `quarter` | 1 | Kept |
| `gold.dim_date` | `fiscal_quarter` | 1 | Kept |
| `gold.dim_date` | `month` | 1 | Kept |
| `gold.dim_date` | `month_name` | 1 | Kept |
| `gold.dim_date` | `month_short` | 1 | Kept |
| `gold.dim_date` | `year_month` | 1 | Kept |
| `gold.dim_date` | `month_start_date` | 1 | Kept |
| `gold.dim_date` | `day_of_month` | 1 | Kept |
| `gold.dim_date` | `day_of_week` | 1 | Kept |
| `gold.dim_date` | `day_name` | 1 | Kept |
| `gold.dim_date` | `is_weekday` | 1 | Kept |
| `gold.dim_date` | `iso_week` | 1 | Kept |
| `gold.dim_date` | `is_reporting_period` | 1 | Kept |
| `gold.dim_region` | `region_key` | 1 | Kept |
| `gold.dim_region` | `region_code` | 1 | Kept |
| `gold.dim_region` | `region_name` | 1 | Kept |
| `gold.dim_region` | `region_group` | 1 | Kept |
| `gold.dim_region` | `currency_code` | 1 | Kept |
| `gold.dim_region` | `timezone` | 1 | Kept |
| `gold.dim_region` | `business_hours_start` | 1 | Kept |
| `gold.dim_region` | `business_hours_end` | 1 | Kept |
| `gold.dim_region` | `gos_target_seconds` | 1 | Kept |
| `gold.dim_region` | `sla_first_call_minutes` | 1 | Kept |
| `gold.dim_channel` | `channel_key` | 1 | Kept |
| `gold.dim_channel` | `channel_code` | 1 | Kept |
| `gold.dim_channel` | `channel_name` | 1 | Kept |
| `gold.dim_channel` | `channel_group` | 1 | Kept |
| `gold.dim_channel` | `paid_flag` | 1 | Kept |
| `gold.dim_channel` | `attribution_window_days` | 1 | Kept |
| `gold.dim_campaign` | `campaign_key` | 1 | Kept |
| `gold.dim_campaign` | `campaign_id` | 1 | Kept |
| `gold.dim_campaign` | `campaign_name` | 1 | Kept |
| `gold.dim_campaign` | `channel_code` | 1 | Kept |
| `gold.dim_campaign` | `region_code` | 1 | Kept |
| `gold.dim_campaign` | `product_code` | 1 | Kept |
| `gold.dim_campaign` | `objective` | 1 | Kept |
| `gold.dim_campaign` | `start_date` | 1 | Kept |
| `gold.dim_campaign` | `end_date` | 1 | Kept |
| `gold.dim_campaign` | `end_before_start` | 1 | Kept |
| `gold.dim_product` | `product_key` | 1 | Kept |
| `gold.dim_product` | `product_code` | 1 | Kept |
| `gold.dim_product` | `product_name` | 1 | Kept |
| `gold.dim_agent` | `agent_key` | 1 | Kept (surrogate) |
| `gold.dim_agent` | `agent_id` | 2 | Kept: employee identifier, needed by Ops; hidden from roles without tier 2 |
| `gold.dim_agent` | `region_code` | 1 | Kept |
| `gold.dim_agent` | `team` | 1 | Kept |
| `gold.dim_agent` | `seniority_band` | 2 | Kept: HR data (brief), needed by Ops; hidden from roles without tier 2 |
| `gold.dim_agent` | `active_flag` | 1 | Kept |
| `gold.dim_agent` | `valid_from` | 1 | Kept (SCD2, assumption 8) |
| `gold.dim_agent` | `valid_to` | 1 | Kept (SCD2, assumption 8) |
| `gold.dim_agent` | `is_current` | 1 | Kept (SCD2) |
| `gold.dim_agent` | `source_row` | 1 | Kept: lineage to the roster row |
| `gold.dim_lead` | `lead_key` | 1 | Surrogate key (52-bit hash of `lead_id`) |
| `gold.dim_lead` | `lead_id` | 2 | Kept: pseudonymous, links to source records holding names and contacts |
| `gold.dim_lead` | `created_at_utc` | 1 | Kept |
| `gold.dim_lead` | `region_code` | 1 | Kept |
| `gold.dim_lead` | `campaign_id` | 1 | Kept |
| `gold.dim_lead` | `channel_code` | 1 | Kept |
| `gold.dim_lead` | `source_detail` | 1 | Kept |
| `gold.dim_lead` | `product_interest` | 1 | Kept |
| `gold.dim_lead` | `lead_status` | 1 | Kept |
| `gold.dim_lead` | `email_hash` | 2 | Salted SHA-256 of trimmed, lowercased email |
| `gold.dim_lead` | `phone_hash` | 2 | Salted SHA-256 of the E.164 phone |
| `gold.dim_lead` | `date_of_birth_hash` | 2 | Salted SHA-256 of `yyyy-MM-dd` |
| `gold.dim_lead` | `age_band` | 1 | Generalised from `date_of_birth`: decade at lead creation, `UNKNOWN` if null |
| `gold.dim_lead` | `age_band_sort` | 1 | Sort order for `age_band` |
| `gold.dim_lead` | `postcode_outward` | 1 | Generalised from `postcode`: rule per region (assumption 25) |
| `gold.dim_lead` | `postcode_format_valid` | 1 | False when the postcode fits no rule for its region (defect 29) |
| `gold.dim_lead` | `estimated_cover_amount` | 2 | Kept: financial data tied to an individual |
| `gold.dim_lead` | `smoker_flag` | 3 | Kept for cleared roles only; never in `dim_lead_nopii` |
| `gold.dim_lead` | `health_condition_flag` | 3 | Kept for cleared roles only; never in `dim_lead_nopii` |
| `gold.dim_lead` | `consent_marketing_current` | 2 | From `silver.consent_current`, else the capture flag (defect 25) |
| `gold.dim_lead` | `consent_marketing_source` | 2 | `consent_events` or `lead_capture` |
| `gold.dim_lead` | `consent_current_event_ts_utc` | 2 | Latest consent event time, null without events |
| `gold.dim_lead` | `consent_phone_current_flag` | 2 | Latest PHONE consent event, null without one |
| `gold.dim_lead` | `is_orphan_campaign` | 1 | Kept (defect 5) |
| `gold.dim_lead` | `dedup_cluster_size` | 1 | Kept |
| `gold.dim_lead_nopii` | `lead_key` | 1 | Joins to the facts |
| `gold.dim_lead_nopii` | `created_date` | 1 | Date only, not the timestamp |
| `gold.dim_lead_nopii` | `region_code` | 1 | Kept |
| `gold.dim_lead_nopii` | `campaign_id` | 1 | Kept |
| `gold.dim_lead_nopii` | `channel_code` | 1 | Kept |
| `gold.dim_lead_nopii` | `source_detail` | 1 | Kept |
| `gold.dim_lead_nopii` | `product_interest` | 1 | Kept |
| `gold.dim_lead_nopii` | `lead_status` | 1 | Kept |
| `gold.dim_lead_nopii` | `age_band` | 1 | As `dim_lead` |
| `gold.dim_lead_nopii` | `age_band_sort` | 1 | As `dim_lead` |
| `gold.dim_lead_nopii` | `postcode_outward` | 1 | As `dim_lead` |
| `gold.dim_lead_nopii` | `is_orphan_campaign` | 1 | Kept |
| `gold.dim_lead_nopii` | `dedup_cluster_size` | 1 | Kept |
