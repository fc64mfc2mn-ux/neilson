# Power BI model design (step 16)

The semantic model on the gold export, for building by hand in Power BI Desktop on the
Windows VM. Measures: `powerbi/measures.dax`. Values to check against:
`powerbi/expected_values.csv` (from `python -m src.powerbi_expected --config
config/config.yaml`). Not in this step: RLS and `bridge_user_region` (step 17), the
Connected Call measure (step 18).

## Tables

Source: the CSVs in `export/powerbi/`, copied to the VM's disk (checklist step 1).
Row counts are asserted in `tests/test_gold_facts.py` and `tests/test_gold_dimensions.py`.

| Table | Load | Remove in Power Query | Hide in the model |
| --- | --- | --- | --- |
| `dim_date` | Yes, marked date table on `date` | none | `date_key` |
| `dim_region` | Yes | `timezone`, `business_hours_start`, `business_hours_end` | `region_key` |
| `dim_channel` | Yes | none | `channel_key` |
| `dim_campaign` | Yes | none | `campaign_key`, `channel_code`, `region_code`, `product_code` |
| `dim_agent` | Yes | `seniority_band` (decided), `source_row` | `agent_key`; `agent_id` is tier 2, step 17 hides it |
| `dim_product` | Yes | none | `product_key` |
| `dim_lead_nopii` | Yes, keeps its name | `created_date`, `region_code`, `campaign_id`, `channel_code` (the dimensions own them) | `lead_key`, `age_band_sort` |
| `fact_lead_funnel` | Yes | `policy_key`, `policy_start_date_key`, `annual_premium_gbp`, `days_lead_to_policy` (policies come from `fact_policy`) | every key |
| `fact_policy` | Yes | `call_id`, `call_key`, `lead_match_status` | keys, `policy_id`, local amounts, `rate_to_gbp`, `counts_as_sale` |
| `fact_call` | Yes | `source_file` | keys, `call_id`, `lead_match_status` |
| `fact_marketing_spend_daily` | Yes | none | keys, `spend_local`, `rate_to_gbp` (local currencies do not add up) |
| `fact_lead` | Staging only: **Enable load off** | none | n/a |
| `Target CPL` | DAX table (below) | n/a | none |
| `Measures` | Empty table for the measures | n/a | its placeholder column |

Not imported: `dim_lead` (hashes and health flags stay out of the model), `bridge_lead_call`
(the funnel already holds call counts per lead; the bridge would need many-to-many),
`bridge_user_region` (step 17).

**Power Query merges** (left outer, on `lead_key`, from the `fact_lead` staging query):

- `fact_lead_funnel` gets `lead_created_date_key`.
- `fact_policy` gets `lead_created_date_key`, `campaign_key`, `channel_key`, so lead-linked
  policies follow the lead's date and first-touch campaign. Policies with no lead get
  blanks and drop out of the lead measures.

This merge is the only logic in Power Query. `findings_powerbi.sql` copies it as a join
on `fact_lead`. Moving these columns into gold is the reversal route.

**Data types.** Set them explicitly: use "Do not detect data types" on import, then set
each type yourself.

| Columns | Type |
| --- | --- |
| `*_key`, `*_date_key`, counts, seconds, `impressions`, `clicks`, `attempt_number` | Whole number (keys are 52-bit, exact) |
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
| `dim_date[date_key]` | `fact_policy[policy_start_date_key]` | **No**: used with `USERELATIONSHIP` (Average Annual Premium; the Ops measure at step 18) |
| `dim_date[date_key]` | `fact_call[call_date_key]` | Yes |
| `dim_region[region_key]` | `region_key` on all four facts | Yes |
| `dim_channel[channel_key]` | `channel_key` on spend, funnel, policy | Yes |
| `dim_campaign[campaign_key]` | `campaign_key` on spend, funnel, policy | Yes |
| `dim_product[product_key]` | `fact_lead_funnel[product_key]`, `fact_policy[product_key]` | Yes |
| `dim_agent[agent_key]` | `fact_call[agent_key]`, `fact_policy[agent_key]` | Yes |
| `dim_agent[agent_key]` | `fact_lead_funnel[last_call_agent_key]` | Yes |
| `dim_agent[agent_key]` | `fact_lead_funnel[first_call_agent_key]` | No |
| `dim_lead_nopii[lead_key]` | `lead_key` on funnel, policy, call | Yes |

`dim_lead_nopii` to `fact_lead_funnel` is unique on both sides. Desktop will offer
one-to-one, which filters both ways. Set it to many-to-one, single direction.

**Date table.** Mark `dim_date` as the date table on `date` (365 days, all of 2026). Sort
`month_name` and `month_short` by `month`. Turn off Auto date/time for the file.

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

The values for every core measure (whole dataset, region, month) can be reproduced from
gold. Expect differences only past the sixth decimal place: DAX divides in floating point,
Spark in decimal, and the file rounds half-up to six places. Other limits:

1. **Not in `expected_values.csv`.** `Cost Per Lead (GBP) MoM %` follows from the month
   rows (May on April). `CPL vs Target Variance (GBP)` depends on the slicer selection.
   Stretch measures 4, 6, 8 and 10 have no expected rows yet.
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
   column (for example `lead_in_business_hours_flag`) filters the denominator but not
   the policies. Use `dim_lead_nopii` attributes, which filter both.
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

## Open for the human

- **Seniority visual (step 17).** The visual "agent seniority against conversion" needs
  `seniority_band`, which is removed from `dim_agent`. My proposal is a separate table,
  `dim_agent_seniority` (`agent_key`, `seniority_band`, from `dim_agent.csv`), related
  like `dim_agent`. OLS can then hide the whole table from the two analyst roles. The
  `agent_id` column could move there too.

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
5. Turn off Enable load for `fact_lead`. Add the two merges and expand only the named
   columns.
6. Close & Apply. Check the row counts: 365 dates, 47,201 funnel and lead rows, 62,829
   calls, 5,172 policies, 2,001 spend rows.
7. In Model view, delete any automatic relationships and create the table above. Set the
   policy-start and first-call relationships to inactive.
8. Mark the date table, and set the sort-by columns.
9. Create the `Target CPL` table and the `Measures` table (Enter data, one placeholder
   column).
10. Paste `measures.dax` into DAX query view, run it, and add each measure to `Measures`.
    Set each description, display folder and format from its comment block. Build the
    helper measures first.
11. Hide the keys and columns listed. Hide the placeholder column in `Measures`.
12. Run the three `EVALUATE` queries at the end of `measures.dax`. Compare them with
    `expected_values.csv` (rates are fractions: 0.095316 is 9.53%). Screenshot the result
    into `powerbi/`.
13. Save as `powerbi/northgate_funnel.pbix`, and copy it back to the repo on the Mac.
