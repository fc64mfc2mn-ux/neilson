// Power Query (M) for the Northgate Life model: one query per imported table in
// powerbi/MODEL_DESIGN.md "Tables", plus the ExportFolder parameter.
//
// How to paste a query: Home > Get data > Blank query, then Home > Advanced Editor.
// Replace everything in the editor with the query below its comment line, click Done,
// and rename the query to the name in that comment line (exactly, for example
// fact_policy). Create ExportFolder first: every other query reads from it.
//
// Each query reads <ExportFolder><table>.csv as UTF-8 (65001) and comma-delimited,
// promotes the headers, removes the columns MODEL_DESIGN.md lists under "Remove in
// Power Query", turns empty strings into null in the non-text columns, and sets every
// column's type explicitly (no type detection; culture en-GB). Text columns keep empty
// strings as "" (MODEL_DESIGN.md, limit 10). Keys are Int64 (52-bit, exact).
//
// Row counts to check after Close & Apply (tests/test_gold_facts.py and
// tests/test_gold_dimensions.py assert them in gold):
//   dim_date                        365
//   dim_region                        4
//   dim_channel                       9
//   dim_campaign                     25
//   dim_agent                        46
//   dim_agent_seniority              46
//   dim_product                       3
//   dim_lead_nopii               47,201
//   fact_lead_funnel             47,201
//   fact_policy                   5,172
//   fact_call                    62,829
//   fact_marketing_spend_daily    2,001

// ===== ExportFolder (parameter: text, required)
"C:\northgate\export\powerbi\" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]

// ===== dim_date
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "dim_date.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    EmptyToNull = Table.ReplaceValue(
        Promoted,
        "",
        null,
        Replacer.ReplaceValue,
        {"date_key", "date", "year", "quarter", "month", "month_start_date", "day_of_month", "day_of_week", "is_weekday", "iso_week", "is_reporting_period"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"date_key", Int64.Type},
            {"date", type date},
            {"year", Int64.Type},
            {"quarter", Int64.Type},
            {"fiscal_quarter", type text},
            {"month", Int64.Type},
            {"month_name", type text},
            {"month_short", type text},
            {"year_month", type text},
            {"month_start_date", type date},
            {"day_of_month", Int64.Type},
            {"day_of_week", Int64.Type},
            {"day_name", type text},
            {"is_weekday", type logical},
            {"iso_week", Int64.Type},
            {"is_reporting_period", type logical}
        },
        "en-GB"
    )
in
    Typed

// ===== dim_region
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "dim_region.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Removed = Table.RemoveColumns(
        Promoted,
        {"timezone", "business_hours_start", "business_hours_end"}
    ),
    EmptyToNull = Table.ReplaceValue(
        Removed,
        "",
        null,
        Replacer.ReplaceValue,
        {"region_key", "gos_target_seconds", "sla_first_call_minutes"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"region_key", Int64.Type},
            {"region_code", type text},
            {"region_name", type text},
            {"region_group", type text},
            {"currency_code", type text},
            {"gos_target_seconds", Int64.Type},
            {"sla_first_call_minutes", Int64.Type}
        },
        "en-GB"
    )
in
    Typed

// ===== dim_channel
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "dim_channel.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    EmptyToNull = Table.ReplaceValue(
        Promoted,
        "",
        null,
        Replacer.ReplaceValue,
        {"channel_key", "paid_flag", "attribution_window_days"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"channel_key", Int64.Type},
            {"channel_code", type text},
            {"channel_name", type text},
            {"channel_group", type text},
            {"paid_flag", type logical},
            {"attribution_window_days", Int64.Type}
        },
        "en-GB"
    )
in
    Typed

// ===== dim_campaign
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "dim_campaign.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    EmptyToNull = Table.ReplaceValue(
        Promoted,
        "",
        null,
        Replacer.ReplaceValue,
        {"campaign_key", "start_date", "end_date", "end_before_start"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"campaign_key", Int64.Type},
            {"campaign_id", type text},
            {"campaign_name", type text},
            {"channel_code", type text},
            {"region_code", type text},
            {"product_code", type text},
            {"objective", type text},
            {"start_date", type date},
            {"end_date", type date},
            {"end_before_start", type logical}
        },
        "en-GB"
    )
in
    Typed

// ===== dim_agent
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "dim_agent.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Removed = Table.RemoveColumns(
        Promoted,
        {"seniority_band", "source_row"}
    ),
    EmptyToNull = Table.ReplaceValue(
        Removed,
        "",
        null,
        Replacer.ReplaceValue,
        {"agent_key", "active_flag", "valid_from", "valid_to", "is_current"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"agent_key", Int64.Type},
            {"agent_id", type text},
            {"region_code", type text},
            {"team", type text},
            {"active_flag", type logical},
            {"valid_from", type date},
            {"valid_to", type date},
            {"is_current", type logical}
        },
        "en-GB"
    )
in
    Typed

// ===== dim_agent_seniority
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "dim_agent_seniority.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    EmptyToNull = Table.ReplaceValue(
        Promoted,
        "",
        null,
        Replacer.ReplaceValue,
        {"agent_key"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"agent_key", Int64.Type},
            {"seniority_band", type text}
        },
        "en-GB"
    )
in
    Typed

// ===== dim_product
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "dim_product.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    EmptyToNull = Table.ReplaceValue(
        Promoted,
        "",
        null,
        Replacer.ReplaceValue,
        {"product_key"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"product_key", Int64.Type},
            {"product_code", type text},
            {"product_name", type text}
        },
        "en-GB"
    )
in
    Typed

// ===== dim_lead_nopii
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "dim_lead_nopii.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Removed = Table.RemoveColumns(
        Promoted,
        {"created_date", "region_code", "campaign_id", "channel_code"}
    ),
    EmptyToNull = Table.ReplaceValue(
        Removed,
        "",
        null,
        Replacer.ReplaceValue,
        {"lead_key", "age_band_sort", "is_orphan_campaign", "dedup_cluster_size"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"lead_key", Int64.Type},
            {"source_detail", type text},
            {"product_interest", type text},
            {"lead_status", type text},
            {"age_band", type text},
            {"age_band_sort", Int64.Type},
            {"postcode_outward", type text},
            {"is_orphan_campaign", type logical},
            {"dedup_cluster_size", Int64.Type}
        },
        "en-GB"
    )
in
    Typed

// ===== fact_lead_funnel
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "fact_lead_funnel.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Removed = Table.RemoveColumns(
        Promoted,
        {"policy_key", "policy_start_date_key", "annual_premium_gbp", "days_lead_to_policy"}
    ),
    EmptyToNull = Table.ReplaceValue(
        Removed,
        "",
        null,
        Replacer.ReplaceValue,
        {"lead_key", "lead_created_ts_utc", "lead_created_date_key", "region_key", "campaign_key", "channel_key", "product_key", "first_call_ts_utc", "first_call_agent_key", "last_call_agent_key", "call_attempt_count", "connected_call_count", "minutes_to_first_call", "first_call_bucket_sort", "within_first_call_sla_flag", "quoted_flag", "application_flag", "consent_phone_current_flag", "dedup_cluster_size", "lead_in_business_hours_flag"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"lead_key", Int64.Type},
            {"lead_created_ts_utc", type datetime},
            {"lead_created_date_key", Int64.Type},
            {"region_key", Int64.Type},
            {"campaign_key", Int64.Type},
            {"channel_key", Int64.Type},
            {"product_key", Int64.Type},
            {"first_call_ts_utc", type datetime},
            {"first_call_agent_key", Int64.Type},
            {"last_call_agent_key", Int64.Type},
            {"call_attempt_count", Int64.Type},
            {"connected_call_count", Int64.Type},
            {"minutes_to_first_call", type number},
            {"first_call_bucket", type text},
            {"first_call_bucket_sort", Int64.Type},
            {"within_first_call_sla_flag", type logical},
            {"quoted_flag", type logical},
            {"application_flag", type logical},
            {"consent_phone_current_flag", type logical},
            {"dedup_cluster_size", Int64.Type},
            {"lead_in_business_hours_flag", type logical}
        },
        "en-GB"
    )
in
    Typed

// ===== fact_policy
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "fact_policy.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Removed = Table.RemoveColumns(
        Promoted,
        {"call_id", "call_key", "lead_match_status"}
    ),
    EmptyToNull = Table.ReplaceValue(
        Removed,
        "",
        null,
        Replacer.ReplaceValue,
        {"policy_key", "lead_key", "is_lead_attributed", "lead_created_date_key", "campaign_key", "channel_key", "agent_key", "region_key", "product_key", "application_date_key", "application_date", "policy_start_date_key", "cancellation_date_key", "counts_as_sale", "annual_premium_local", "monthly_premium_local", "sum_assured_local", "rate_to_gbp", "annual_premium_gbp", "gbp_rate_missing", "annual_premium_derived"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"policy_key", Int64.Type},
            {"policy_id", type text},
            {"lead_key", Int64.Type},
            {"is_lead_attributed", type logical},
            {"lead_created_date_key", Int64.Type},
            {"campaign_key", Int64.Type},
            {"channel_key", Int64.Type},
            {"agent_key", Int64.Type},
            {"region_key", Int64.Type},
            {"product_key", Int64.Type},
            {"application_date_key", Int64.Type},
            {"application_date", type date},
            {"policy_start_date_key", Int64.Type},
            {"cancellation_date_key", Int64.Type},
            {"policy_status", type text},
            {"counts_as_sale", type logical},
            {"payment_frequency", type text},
            {"currency_code", type text},
            {"annual_premium_local", Currency.Type},
            {"monthly_premium_local", Currency.Type},
            {"sum_assured_local", Currency.Type},
            {"rate_to_gbp", type number},
            {"annual_premium_gbp", Currency.Type},
            {"gbp_rate_missing", type logical},
            {"annual_premium_derived", type logical}
        },
        "en-GB"
    )
in
    Typed

// ===== fact_call
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "fact_call.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Removed = Table.RemoveColumns(
        Promoted,
        {"source_file"}
    ),
    EmptyToNull = Table.ReplaceValue(
        Removed,
        "",
        null,
        Replacer.ReplaceValue,
        {"call_key", "lead_key", "agent_key", "region_key", "call_date_key", "call_start_ts_utc", "call_end_ts_utc", "duration_seconds", "talk_time_seconds", "hold_time_seconds", "queue_wait_seconds", "attempt_number", "answered_within_sla_flag", "is_connected", "qa_score", "compliance_fail_flag", "after_do_not_call", "residual_offset_flag"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"call_key", Int64.Type},
            {"call_id", type text},
            {"lead_key", Int64.Type},
            {"lead_match_status", type text},
            {"agent_key", Int64.Type},
            {"region_key", Int64.Type},
            {"call_date_key", Int64.Type},
            {"call_start_ts_utc", type datetime},
            {"call_end_ts_utc", type datetime},
            {"duration_seconds", Int64.Type},
            {"talk_time_seconds", Int64.Type},
            {"hold_time_seconds", Int64.Type},
            {"queue_wait_seconds", Int64.Type},
            {"call_direction", type text},
            {"disposition", type text},
            {"wrap_up_code", type text},
            {"attempt_number", Int64.Type},
            {"answered_within_sla_flag", type logical},
            {"is_connected", type logical},
            {"qa_score", type number},
            {"compliance_fail_flag", type logical},
            {"after_do_not_call", type logical},
            {"residual_offset_flag", type logical}
        },
        "en-GB"
    )
in
    Typed

// ===== fact_marketing_spend_daily
let
    Source = Csv.Document(
        File.Contents(ExportFolder & "fact_marketing_spend_daily.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    EmptyToNull = Table.ReplaceValue(
        Promoted,
        "",
        null,
        Replacer.ReplaceValue,
        {"date_key", "campaign_key", "channel_key", "region_key", "spend_local", "rate_to_gbp", "spend_gbp", "impressions", "clicks", "minor_unit_rescaled", "outside_campaign_dates"}
    ),
    Typed = Table.TransformColumnTypes(
        EmptyToNull,
        {
            {"date_key", Int64.Type},
            {"campaign_key", Int64.Type},
            {"channel_key", Int64.Type},
            {"region_key", Int64.Type},
            {"currency_code", type text},
            {"spend_local", Currency.Type},
            {"rate_to_gbp", type number},
            {"spend_gbp", Currency.Type},
            {"impressions", Int64.Type},
            {"clicks", Int64.Type},
            {"minor_unit_rescaled", type logical},
            {"outside_campaign_dates", type logical}
        },
        "en-GB"
    )
in
    Typed
