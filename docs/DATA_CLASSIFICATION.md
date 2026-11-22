# Gold column classification

Source for the column classification in `docs/GOVERNANCE.md`. Tiers are the brief's:
0 aggregate, 1 internal (no personal data), 2 confidential (personal data under UK GDPR),
3 restricted (special category, or reveals it). A hash of personal data is still personal
data (pseudonymised, not anonymised), so hashes are tier 2. Generalised values (age band,
outward postcode) are tier 1 by design: that is what the PII policy is for.

`tests/test_pii_policy.py::test_classification_covers_every_gold_column` checks this table
against the built gold schemas: every column listed once, nothing missing, no extra rows.

Not in gold at all (stay in silver): lead `first_name`, `last_name`, `email`, `phone`,
`postcode`, `date_of_birth`, `consent_timestamp_utc`, `consent_source`; call `call_notes`
and `recording_url` (tier 3); policy `underwriting_decision` (tier 3); agent `agent_name`, `agent_email`, `manager_name`,
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
| `gold.dim_agent` | `seniority_band` | 2 | Kept: HR data (brief), needed by Ops; the Power BI model takes it from `dim_agent_seniority` instead and removes it here |
| `gold.dim_agent` | `active_flag` | 1 | Kept |
| `gold.dim_agent` | `valid_from` | 1 | Kept (SCD2, assumption 8) |
| `gold.dim_agent` | `valid_to` | 1 | Kept (SCD2, assumption 8) |
| `gold.dim_agent` | `is_current` | 1 | Kept (SCD2) |
| `gold.dim_agent` | `source_row` | 1 | Kept: lineage to the roster row |
| `gold.dim_agent_seniority` | `agent_key` | 1 | Surrogate key, one row per `dim_agent` row |
| `gold.dim_agent_seniority` | `seniority_band` | 2 | HR data (brief), for the seniority visual; a table of its own so object-level security hides it whole from roles without tier 2 (`UNKNOWN` labelled `Unknown`) |
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
| `gold.fact_lead` | `lead_key` | 1 | Joins to `dim_lead` |
| `gold.fact_lead` | `lead_created_date_key` | 1 | Kept |
| `gold.fact_lead` | `lead_created_ts_utc` | 1 | Kept |
| `gold.fact_lead` | `campaign_key` | 1 | UNKNOWN (-1) for orphan campaigns (defect 5) |
| `gold.fact_lead` | `channel_key` | 1 | Campaign channel (assumption 4); UNKNOWN if unresolved |
| `gold.fact_lead` | `region_key` | 1 | Strict: every row resolves |
| `gold.fact_lead` | `product_key` | 1 | Strict: every row resolves |
| `gold.fact_lead` | `lead_in_business_hours_flag` | 1 | Weekdays, regional hours, own timezone (assumption 9) |
| `gold.fact_lead` | `is_orphan_campaign` | 1 | Kept (defect 5) |
| `gold.fact_call` | `call_key` | 1 | Surrogate key (52-bit hash of `call_id`) |
| `gold.fact_call` | `call_id` | 1 | Kept: dialler reference, no personal data |
| `gold.fact_call` | `lead_key` | 1 | Null when the lead is not a surviving lead (assumption 20) |
| `gold.fact_call` | `lead_match_status` | 1 | `matched`, `lead_not_found` or `lead_quarantined` |
| `gold.fact_call` | `agent_key` | 1 | Version valid on the call date; UNKNOWN for AG9001 to AG9004 |
| `gold.fact_call` | `region_key` | 1 | Strict |
| `gold.fact_call` | `call_date_key` | 1 | Kept |
| `gold.fact_call` | `call_start_ts_utc` | 1 | Kept (UTC; AU converted from Sydney time) |
| `gold.fact_call` | `call_end_ts_utc` | 1 | Kept |
| `gold.fact_call` | `duration_seconds` | 1 | Kept (seconds throughout) |
| `gold.fact_call` | `talk_time_seconds` | 1 | Kept |
| `gold.fact_call` | `hold_time_seconds` | 1 | Kept |
| `gold.fact_call` | `queue_wait_seconds` | 1 | Kept |
| `gold.fact_call` | `call_direction` | 1 | Kept |
| `gold.fact_call` | `disposition` | 1 | Kept |
| `gold.fact_call` | `wrap_up_code` | 1 | Kept |
| `gold.fact_call` | `attempt_number` | 1 | Kept |
| `gold.fact_call` | `answered_within_sla_flag` | 1 | Kept |
| `gold.fact_call` | `is_connected` | 1 | CONNECTED, QUOTED, APPLICATION_STARTED, SOLD (assumption 2) |
| `gold.fact_call` | `qa_score` | 2 | Employee performance (brief); Ops roles only |
| `gold.fact_call` | `compliance_fail_flag` | 2 | Employee performance (brief); Ops roles only |
| `gold.fact_call` | `after_do_not_call` | 1 | Kept (defect 26) |
| `gold.fact_call` | `residual_offset_flag` | 1 | Kept (defect 15) |
| `gold.fact_call` | `source_file` | 1 | Lineage |
| `gold.fact_policy` | `policy_key` | 1 | Surrogate key (52-bit hash of `policy_id`) |
| `gold.fact_policy` | `policy_id` | 1 | Kept: policy reference |
| `gold.fact_policy` | `lead_key` | 1 | Null when no surviving lead (assumption 11) |
| `gold.fact_policy` | `lead_match_status` | 1 | `matched`, `no_lead` or `lead_quarantined` |
| `gold.fact_policy` | `is_lead_attributed` | 1 | True when `lead_key` is set |
| `gold.fact_policy` | `lead_created_date_key` | 1 | The surviving lead's created date; null without a lead (step 16b) |
| `gold.fact_policy` | `campaign_key` | 1 | The lead's first-touch campaign; null without a lead |
| `gold.fact_policy` | `channel_key` | 1 | The lead's channel; null without a lead |
| `gold.fact_policy` | `call_key` | 1 | Null when the call is not in `silver.call` |
| `gold.fact_policy` | `call_id` | 1 | Kept as supplied |
| `gold.fact_policy` | `agent_key` | 1 | Version valid on the application date |
| `gold.fact_policy` | `region_key` | 1 | Strict |
| `gold.fact_policy` | `product_key` | 1 | Strict |
| `gold.fact_policy` | `application_date_key` | 1 | Kept |
| `gold.fact_policy` | `application_date` | 1 | Kept |
| `gold.fact_policy` | `policy_start_date_key` | 1 | Kept |
| `gold.fact_policy` | `cancellation_date_key` | 1 | Kept |
| `gold.fact_policy` | `policy_status` | 1 | Kept |
| `gold.fact_policy` | `counts_as_sale` | 1 | Status other than NTU (assumption 3) |
| `gold.fact_policy` | `payment_frequency` | 1 | Kept |
| `gold.fact_policy` | `currency_code` | 1 | Kept |
| `gold.fact_policy` | `annual_premium_local` | 2 | Financial data tied to an individual (brief) |
| `gold.fact_policy` | `monthly_premium_local` | 2 | Financial data tied to an individual (brief) |
| `gold.fact_policy` | `sum_assured_local` | 2 | Financial data tied to an individual (brief) |
| `gold.fact_policy` | `rate_to_gbp` | 1 | Application month; null when Treasury has no rate |
| `gold.fact_policy` | `annual_premium_gbp` | 2 | Null when no rate (assumption 7, defect 12) |
| `gold.fact_policy` | `gbp_rate_missing` | 1 | Flag |
| `gold.fact_policy` | `annual_premium_derived` | 1 | Kept (defect 21) |
| `gold.fact_marketing_spend_daily` | `date_key` | 0 | Campaign-day aggregate |
| `gold.fact_marketing_spend_daily` | `campaign_key` | 0 | Campaign-day aggregate |
| `gold.fact_marketing_spend_daily` | `channel_key` | 0 | Campaign-day aggregate |
| `gold.fact_marketing_spend_daily` | `region_key` | 0 | Campaign-day aggregate |
| `gold.fact_marketing_spend_daily` | `currency_code` | 0 | Campaign-day aggregate |
| `gold.fact_marketing_spend_daily` | `spend_local` | 0 | Minor units rescaled (defect 11) |
| `gold.fact_marketing_spend_daily` | `rate_to_gbp` | 0 | Spend month rate |
| `gold.fact_marketing_spend_daily` | `spend_gbp` | 0 | Reconciles to silver exactly |
| `gold.fact_marketing_spend_daily` | `impressions` | 0 | Campaign-day aggregate |
| `gold.fact_marketing_spend_daily` | `clicks` | 0 | Campaign-day aggregate |
| `gold.fact_marketing_spend_daily` | `minor_unit_rescaled` | 0 | Flag (defect 11) |
| `gold.fact_marketing_spend_daily` | `outside_campaign_dates` | 0 | Flag (defect 9) |
| `gold.fact_lead_funnel` | `lead_key` | 1 | One row per surviving lead |
| `gold.fact_lead_funnel` | `lead_created_ts_utc` | 1 | Kept |
| `gold.fact_lead_funnel` | `lead_created_date_key` | 1 | From `fact_lead` (step 16b) |
| `gold.fact_lead_funnel` | `region_key` | 1 | Strict |
| `gold.fact_lead_funnel` | `campaign_key` | 1 | First touch: the surviving (earliest) lead's campaign |
| `gold.fact_lead_funnel` | `channel_key` | 1 | As `fact_lead` |
| `gold.fact_lead_funnel` | `product_key` | 1 | Strict |
| `gold.fact_lead_funnel` | `first_call_ts_utc` | 1 | Null when never called |
| `gold.fact_lead_funnel` | `first_call_agent_key` | 1 | Null when never called |
| `gold.fact_lead_funnel` | `last_call_agent_key` | 1 | Last call on or before the application date |
| `gold.fact_lead_funnel` | `call_attempt_count` | 1 | Calls in `bridge_lead_call` |
| `gold.fact_lead_funnel` | `connected_call_count` | 1 | Assumption 2 |
| `gold.fact_lead_funnel` | `minutes_to_first_call` | 1 | Null when never called |
| `gold.fact_lead_funnel` | `first_call_bucket` | 1 | Distribution bucket of `minutes_to_first_call`; null if never called |
| `gold.fact_lead_funnel` | `first_call_bucket_sort` | 1 | Sort order of the bucket, 1 to 6 |
| `gold.fact_lead_funnel` | `within_first_call_sla_flag` | 1 | Against the region's SLA; false when never called |
| `gold.fact_lead_funnel` | `quoted_flag` | 1 | Quoted or later call, or a policy |
| `gold.fact_lead_funnel` | `application_flag` | 1 | Application or sale call, or a policy |
| `gold.fact_lead_funnel` | `policy_key` | 1 | Null without a policy |
| `gold.fact_lead_funnel` | `policy_start_date_key` | 1 | Null without a policy |
| `gold.fact_lead_funnel` | `annual_premium_gbp` | 2 | Financial data tied to an individual |
| `gold.fact_lead_funnel` | `days_lead_to_policy` | 1 | Lead creation to application date |
| `gold.fact_lead_funnel` | `consent_phone_current_flag` | 2 | From `silver.consent_current` |
| `gold.fact_lead_funnel` | `dedup_cluster_size` | 1 | Kept |
| `gold.fact_lead_funnel` | `lead_in_business_hours_flag` | 1 | Assumption 9 |
| `gold.bridge_lead_call` | `lead_key` | 1 | Surviving lead |
| `gold.bridge_lead_call` | `call_key` | 1 | Call with a surviving lead |
| `gold.bridge_lead_call` | `call_sequence` | 1 | Order of the call for its lead |
| `gold.bridge_user_region` | `user_principal_name` | 2 | Staff sign-in name for RLS; exempt from the email pattern only |
| `gold.bridge_user_region` | `role_name` | 1 | Kept |
| `gold.bridge_user_region` | `region_code` | 1 | `ALL` kept as stated |
| `gold.bridge_user_region` | `region_key` | 1 | Null for `ALL` |
| `gold.bridge_user_region` | `all_regions_flag` | 1 | True for `ALL` |
