# Power BI model design (step 16)

The semantic model on the gold export, for building by hand in Power BI Desktop on the
Windows VM. Measures: `powerbi/measures.dax`. Values to check against:
`powerbi/expected_values.csv` (from `python -m src.powerbi_expected --config
config/config.yaml`). RLS and `bridge_user_region` are added in step 17 (last section); the
`Connected Call to Policy Conversion Rate` is step 18 (last section).

## Tables

Source: the CSVs in `export/powerbi/`, copied to the VM's disk (checklist step 1).
Row counts are asserted in `tests/test_gold_facts.py` and `tests/test_gold_dimensions.py`.

| Table | Load | Remove in Power Query | Hide in the model |
| --- | --- | --- | --- |
| `dim_date` | Yes, marked date table on `date` | none | `date_key` |
| `dim_region` | Yes | `timezone`, `business_hours_start`, `business_hours_end` | `region_key` |
| `dim_channel` | Yes | none | `channel_key` |
| `dim_campaign` | Yes | none | `campaign_key`, `channel_code`, `region_code`, `product_code` |
| `dim_agent` | Yes | `seniority_band` (kept in gold: the SCD2 versions of AG0008 and AG0037 differ only by it; the model takes it from `dim_agent_seniority`), `source_row` | `agent_key` (`agent_id` stays visible: decided, tier 2) |
| `dim_agent_seniority` | Yes: `agent_key`, `seniority_band` (tier 2, its own table so step 17's object-level security can hide it whole) | none | `agent_key` |
| `dim_product` | Yes | none | `product_key` |
| `dim_lead_nopii` | Yes, keeps its name | `created_date`, `region_code`, `campaign_id`, `channel_code` (the dimensions own them) | `lead_key`, `age_band_sort` |
| `fact_lead_funnel` | Yes | `policy_key`, `policy_start_date_key`, `annual_premium_gbp`, `days_lead_to_policy` (policies come from `fact_policy`) | every key, `first_call_bucket_sort` |
| `fact_policy` | Yes | `call_id`, `call_key`, `lead_match_status` | keys, `policy_id`, local amounts, `rate_to_gbp`, `counts_as_sale` |
| `fact_call` | Yes | `source_file` | keys, `call_id`, `lead_match_status` |
| `fact_marketing_spend_daily` | Yes | none | keys, `spend_local`, `rate_to_gbp` (local currencies do not add up) |
| `Target CPL` | DAX table (below) | n/a | none |
| `Key Measures` | Empty table for the measures | n/a | its placeholder column |

Not imported: `dim_lead` (hashes and health flags stay out of the model), `fact_lead`
(`fact_lead_funnel` has the same grain and keys), `bridge_lead_call` (the funnel already
holds call counts per lead; the bridge would need many-to-many), `bridge_user_region`
(step 17).

**No Power Query merges.** Gold carries every key the model filters on. `fact_policy` has
the lead's `lead_created_date_key`, `campaign_key` and `channel_key` (null for the 405
policies without a lead), so lead-linked policies follow the lead's date and first-touch
campaign. `fact_lead_funnel` has `lead_created_date_key` and `first_call_bucket`. Power
Query only types columns and removes them.

**Data types.** Set them explicitly: use "Do not detect data types" on import, then set
each type yourself.

| Columns | Type |
| --- | --- |
| `*_key`, `*_date_key`, counts, seconds, `impressions`, `clicks`, `attempt_number`, `first_call_bucket_sort` | Whole number (keys are 52-bit, exact) |
| `spend_gbp`, `annual_premium_gbp`, local amounts | Fixed decimal number |
| `minutes_to_first_call`, `rate_to_gbp`, `qa_score` | Decimal number |
| `date`, `month_start_date`, `valid_from`, `valid_to`, `start_date`, `end_date`, `application_date` | Date |
| `*_ts_utc` | Date/Time (UTC as text; do not convert) |
| `true`/`false` columns (`is_*`, `*_flag`, `paid_flag`) | True/False |
| everything else | Text |

If the VM's locale is not English, use "Change type with locale: English (United
Kingdom)" for the decimals and dates.

## Relationships

All are many-to-one (dimension on the one side), with single-direction filtering from the
dimension. There are no bidirectional relationships and no fact-to-fact relationships.
Turn off autodetect and create these by hand:

| From (one) | To (many) | Active |
| --- | --- | --- |
| `dim_date[date_key]` | `fact_marketing_spend_daily[date_key]` | Yes |
| `dim_date[date_key]` | `fact_lead_funnel[lead_created_date_key]` | Yes |
| `dim_date[date_key]` | `fact_policy[lead_created_date_key]` | Yes |
| `dim_date[date_key]` | `fact_policy[policy_start_date_key]` | **No**: used with `USERELATIONSHIP` (Average Annual Premium; and `Connected Call to Policy Conversion Rate`) |
| `dim_date[date_key]` | `fact_call[call_date_key]` | Yes |
| `dim_region[region_key]` | `region_key` on all four facts | Yes |
| `dim_channel[channel_key]` | `channel_key` on spend, funnel, policy | Yes |
| `dim_campaign[campaign_key]` | `campaign_key` on spend, funnel, policy | Yes |
| `dim_product[product_key]` | `fact_lead_funnel[product_key]`, `fact_policy[product_key]` | Yes |
| `dim_agent[agent_key]` | `fact_call[agent_key]`, `fact_policy[agent_key]` | Yes |
| `dim_agent[agent_key]` | `fact_lead_funnel[last_call_agent_key]` | Yes |
| `dim_agent[agent_key]` | `fact_lead_funnel[first_call_agent_key]` | No |
| `dim_agent_seniority[agent_key]` | `fact_call[agent_key]`, `fact_policy[agent_key]`, `fact_lead_funnel[last_call_agent_key]` | Yes |
| `dim_lead_nopii[lead_key]` | `lead_key` on funnel, policy, call | Yes |

`dim_lead_nopii` to `fact_lead_funnel` is unique on both sides. Desktop will offer
one-to-one, which filters both ways. Set it to many-to-one, single direction.

`dim_agent_seniority` is also unique on `agent_key`, as `dim_agent` is. Do not relate the
two to each other: each is a dimension of the facts.

**Date table.** Mark `dim_date` as the date table on `date` (365 days, all of 2026). Sort
`month_name` and `month_short` by `month`, and `fact_lead_funnel[first_call_bucket]` by
`first_call_bucket_sort`. Turn off Auto date/time for the file.

**Target CPL** (disconnected, no relationships). Create it with New table, not the What-if
dialog: the dialog's measure has a default value, so "nothing selected" could not happen.

```DAX
Target CPL = SELECTCOLUMNS ( GENERATESERIES ( 20, 120, 5 ), "Target CPL (GBP)", [Value] )
```

That gives 21 values, from £20 to £120 in £5 steps. Format the column `\£#,0`, and use a
single-select slicer.

**Display folders and formats** are in each measure's comment block in `measures.dax`:
`1. Spend and leads`, `2. Funnel`, `3. Calls`, `4. Policy`. Use `\£#,0.00` for GBP and
`0.00%` for conversion.

## What cannot be reproduced exactly, and limits

The values for every core measure (whole dataset, region, month, plus channel, seniority
and first-call bucket where a visual uses them) and Cost Per Policy can be reproduced from
gold. Expect differences only past the sixth decimal place: DAX divides in floating point,
Spark in decimal, and the file rounds half-up to six places. Other limits:

1. **Not in `expected_values.csv`.** `Cost Per Lead (GBP) MoM %` follows from the month
   rows (May on April). `CPL vs Target Variance (GBP)` depends on the slicer selection.
   Stretch measures 4, 6 and 8 have no expected rows yet.
2. **Each measure uses its own date.** Spend uses the spend date, leads and lead-linked
   policies the lead-created date, calls the call date (calls run into July; spend and
   leads stop in June), and premium the policy-start date. A month's CPL compares that
   month's spend with that month's leads. It is not a cohort cost.
3. **CA's June CPL is understated.** The 180 CAD June spend rows are quarantined (no
   rate), but CA's June leads still count. This affects June and CA, and the expected
   file carries the same gap.
4. **Slicers that do not reach spend.** Product and `dim_lead_nopii` attributes filter
   leads and policies but not spend (spend has neither key). CPL and Cost Per Policy are
   wrong under those slicers; don't show them that way.
5. **Funnel-only columns do not reach `fact_policy`.** Slicing on a `fact_lead_funnel`
   column (for example `lead_in_business_hours_flag` or `first_call_bucket`) filters the
   denominator but not the policies. Use `dim_lead_nopii` attributes, which filter both.
   Do not show conversion by first-call bucket.
6. **Policy region and product.** Policies use their own `region_key` (equal to the
   lead's for all 4,767 linked policies; tested) and the product sold, not the lead's
   `product_interest`.
7. **AHT scope.** Only handle time on connected calls counts. Calls that did not connect
   carry talk time too (11,736 of them). Counting it would raise AHT from 517.6 to 642.6
   seconds (`findings_powerbi.sql: aht_numerator_scope`).
8. **AU Time to First Call.** The AU median (511.7 minutes) includes the unexplained
   offset that remains after the Sydney conversion. It is flagged, not corrected.
9. **Dates are UTC days**, as in gold. A late-evening AU or CA lead can fall on the next
   or previous local day.
10. **Empty text.** The CSVs have no null marker, so empty text arrives as `""`, not
    blank. The `NTU` filter keeps both, and the SQL copies this with `COALESCE`.

## The four visuals (one report page)

Each visual's numbers can be checked against `expected_values.csv` (the `slice` column
names the axis). Leave out blank and `Unknown` categories unless stated.

1. **Channel CPL against cost per policy.** Scatter chart. X axis: `Cost Per Lead (GBP)`.
   Y axis: `Cost Per Policy (GBP)`. Values (one point per channel):
   `dim_channel[channel_name]`. Size: `Leads (Deduplicated)`. Tooltips: `Marketing Spend
   (GBP)`, `Lead to Policy Conversion Rate`. Check: `channel` rows of both cost measures.
   Spend follows the spend date and policies the lead's date and channel (limit 2). Cost
   per policy is stretch measure 10, built here because this visual needs it.
2. **Funnel by region.** Line and clustered column chart. X axis:
   `dim_region[region_name]`. Columns: `Leads (Deduplicated)` and `Policies Linked to
   Surviving Leads`. Line (secondary axis): `Lead to Policy Conversion Rate`. Check:
   `region` rows of leads and conversion (policies = conversion x leads).
3. **Agent seniority against conversion.** Clustered column chart. X axis:
   `dim_agent_seniority[seniority_band]`, filtered to Core, Senior and Trainee. Y axis:
   `Lead to Policy Conversion Rate`. Tooltip: `Leads (Deduplicated)`. Check: `seniority`
   rows: Core 16.3%, Senior 15.3%, Trainee 3.9%. Leads follow their last-call agent, and
   policies their selling agent. These are the same agent for 4,626 of 4,767 lead-linked
   policies (97.0%, `seniority_attribution_agreement`). Leads never called have no
   seniority and fall in the blank bar, but the policies of 42 of them still count under
   their seller's band.
   **Small-group risk.** Current agents per region and band
   (`agent_seniority_group_sizes`): AU has 6 Core and 2 Senior and no current Trainee;
   CA has 1 Trainee; US has 2 Core and 2 Senior. Sliced by region, or seen by a
   `Regional_Analyst` limited to one region, a bar can be one or two named people's
   results. That is tier 2 HR performance data, and too few agents for a stable rate.
   Keep region off this visual. **Hide small groups (decided):** add the helper
   `Agents in Group` to the visual's filters as a visual-level filter, set to "is greater
   than or equal to 3". A band then shows only with at least 3 distinct agents behind it,
   including under a region slicer or RLS. The helper counts `dim_agent[agent_id]`
   through `fact_call`, because `dim_agent_seniority` does not filter `dim_agent`
   directly. It is not a KPI and adds no second conversion measure.
4. **Distribution of time to first call.** Clustered column chart. X axis:
   `fact_lead_funnel[first_call_bucket]` (sorted by `first_call_bucket_sort`; 0-15 min,
   15-30 min, 30-60 min, 1-4 h, 4-24 h, over 24 h). Y axis: `Leads (Deduplicated)`. Filter
   out blank (never called). Card beside it: `Time to First Call (Median Minutes)`.
   Check: `first_call_bucket` rows of `Leads (Deduplicated)`.

## Build checklist (Windows VM)

1. On the Mac, run `python -m src.run_pipeline --config config/config.yaml`. Copy
   `export/powerbi/` and `powerbi/` to the VM's own disk at `C:\northgate\` (copy, not
   link). Load from `C:\`, never from the Parallels share (`\\Mac\...`): the share is slow,
   and its path changes between machines.
2. Open Power BI Desktop. In File > Options > Current file > Data load, turn off
   "Autodetect new relationships" and "Auto date/time".
3. Create a text parameter `ExportFolder` = `C:\northgate\export\powerbi\`. Every source
   uses it.
4. Import each CSV in the table above with Text/CSV: UTF-8 (65001), comma delimiter, "Do
   not detect data types". Promote headers, set the types, remove the columns listed.
5. Close & Apply. Check the row counts: 365 dates, 47,201 funnel rows, 62,829 calls,
   5,172 policies, 2,001 spend rows, 46 agent and agent-seniority rows.
6. In Model view, delete any automatic relationships and create the table above. Set the
   policy-start and first-call relationships to inactive.
7. Mark the date table, and set the sort-by columns.
8. Create the `Target CPL` table and the `Key Measures` table (Enter data, one placeholder
   column).
9. Paste `measures.dax` into DAX query view, run it, and add each measure to `Key Measures`.
   Set each description, display folder and format from its comment block. Build the
   helper measures first.
10. Hide the keys and columns listed. Hide the placeholder column in `Key Measures`.
11. Run the three `EVALUATE` queries at the end of `measures.dax`. Compare them with
    `expected_values.csv` (rates are fractions: 0.095316 is 9.53%). Screenshot the result
    into `powerbi/`.
12. Build the four visuals below on one page. Compare each with its
    `expected_values.csv` rows.
13. Save as `powerbi/northgate_funnel.pbix`, and copy it back to the repo on the Mac.

## Row-level security (step 17)

**Load `bridge_user_region`:** Get data -> Text/CSV on `export/powerbi/bridge_user_region.csv`.
Columns: `user_principal_name`, `role_name`, `region_code`, `region_key`,
`all_regions_flag`. Hide the table from report view. It has no relationships: the role
filter reads it with DAX.

**Role `Regional_Analyst`**, filter on `dim_region`: the user's mapped `region_key`
values from the bridge, matched on `USERPRINCIPALNAME()`. A user whose row has
`all_regions_flag` true sees every region; an unmapped user sees no rows. The facts are
filtered through the `dim_region` relationships.

**Tested with View as** (screenshots in `powerbi/`):

| User | Expected | Evidence |
|---|---|---|
| `anz.manager@` | AU and US | `rls_view_as_anz_manager.png` |
| `uk.analyst@` | UK only | `rls_view_as_uk_analyst.png` |
| `nobody@` (unmapped) | zero rows | `rls_view_as_unmapped.png`, `rls_view_as_no_user.png` |

`rls_view_not_active_all_data.png` shows the same page with no role active (all data), and
`rls_role_definition.png` the role's DAX filter.

## Connected Call to Policy Conversion Rate (step 18)

Paste the new block of `measures.dax` into the `Key Measures` table (folder `2. Funnel`,
format `0.00%`). It needs no new table or relationship, and the .pbix is not changed by this
step. How it works: the business-hours lead set is read once from `fact_lead_funnel`, with
the lead-created date filter switched off (`CROSSFILTER ... NONE`), and applied to
`fact_policy` and `fact_call` with `TREATAS` on `lead_key`. Policies then use the
policy-start date (`USERELATIONSHIP`), calls the active call date. Region, product and
`dim_lead_nopii` slicers reach both facts as before.

**Check in Desktop** against `expected_values.csv` (all, four regions, months, and the new
quarter rows) and the new Q2 query at the end of `measures.dax`:

| Slice | Expected |
| --- | ---: |
| All dates | 24.0142% (1,760 / 7,329) |
| Q2 2026 | 18.5564% (1,324 / 7,135) |
| AU / CA / UK / US | 12.03% / 26.95% / 25.20% / 28.41% |
| April / May / June | 8.56% / 20.50% / 25.32% |

**Limits.** July (and Q3) shows 224.7%: policies start to 31 July, calls end on 19 July.
Do not put this measure on a page that shows July. Funnel columns other than the
business-hours set do not reach the policies (limit 5 above). The DAX and the SQL agree on
region only because a lead, its calls and its policies always share one region (a slow test
checks it). `docs/KPI_RECONCILIATION.md` is the bridge from Lead to Policy Conversion Rate.
