# Legacy port: sp_MarketingLeadSummary

The brief asks for the behaviour differences between `data/legacy/sp_MarketingLeadSummary.sql`
and the port "in your submission" without naming a document, so they are here.

Two versions sit side by side in `src/sql/`, run by `src/legacy_port.py`:

- **Naive:** `legacy_sp_marketing_lead_summary_naive.sql`. A literal Spark SQL translation
  that keeps the procedure's logic, run on the raw data (bronze, typed as the SQL Server
  tables). It is the baseline for the parity test, not a report.
- **Ported:** `port_sp_marketing_lead_summary.sql`. The corrected report on gold: one row
  per region and channel, GBP spend, deduplicated leads, calls and policies aggregated per
  lead before joining.

`GETDATE()` is a parameter in both, `as_of` (default `2026-06-30 00:00:00`), so every result
is reproducible; `@RegionCode` is `region_code`. Every number below comes from a named query
in `src/sql/findings_legacy.sql` (run at that `as_of`) and is asserted in
`tests/test_legacy_parity.py`, which also has one fixture test per difference.

| # | Difference | Cause in the procedure | Evidence query | Number | Business impact |
| --- | --- | --- | --- | --- | --- |
| 1 | `TotalCalls` is join rows, not calls | Leads, Calls and Policies are joined before aggregating; `COUNT(l.LeadID)` counts lead x call x policy rows, and a lead with no call still counts 1 | `legacy_fan_out` | 76,090 naive against 61,690 calls | Call volumes overstated by 23% |
| 2 | `ConvertedLeads` counts a sale once per joined row | `SUM(CASE WHEN p.PolicyID IS NOT NULL ...)` over the same fan-out | `legacy_fan_out` | 8,001 naive against 4,499 converted leads; top rate 34.7% against 18.6% | Conversion inflated by 78%. It can pass 100% (fixture test); on this data no group does |
| 3 | CPL divides spend by join rows | `SpendLocal / COUNT(l.LeadID)` instead of distinct leads | `legacy_cpl_factor` | UK: the divisor alone scales CPL by 0.621; with the spend issues, UK channel CPL is 0.609x to 21.547x the ported value | Channel CPL is wrong in both directions, so budget moves on it are unsafe |
| 4 | The conversion cast overflows | `CAST(... AS DECIMAL(9,6))` holds at most 999.999999 | `legacy_fan_out` | 2 groups have `ConvertedLeads` above 999 | On SQL Server with default `ANSI_WARNINGS ON`, error 8115 fails the whole procedure on this data; the literal Spark port returns NULL silently |
| 5 | Spend is raw local currency | `SUM(s.SpendLocal)` with no FX, no dedup, no minor-unit fix, no rate check | `legacy_spend_by_region`, `legacy_spend_and_period` | Naive over cleaned local spend: AU 16.129x, UK 13.434x (minor units), CA 1.380x (CAD June rows), US 1.013x; 2,224 rows against 2,001 | Spend and CPL wildly overstated in UK and AU; regions cannot be compared (AUD, CAD, GBP, USD) |
| 6 | Spend and leads cover different periods | Spend: `SpendDate >= DATEADD(day, -90, GETDATE())`, no upper bound; leads and calls: all time | `legacy_spend_and_period` | 10 leads outside the quarter counted; run on 2026-11-25 the window holds 0 spend rows (an empty report); a 07:00 run on 30 June drops 1 April (2,199 of 2,224 rows) | The report depends on the day and hour it runs, and is empty once the quarter is 90 days old |
| 7 | Duplicates are counted | No dedup of leads, calls or policies | `legacy_duplicates` | 47,997 raw lead ids against 47,201 surviving; 64,000 call rows against 63,460 calls; 4,521 linked non-NTU policy rows against 4,500 policies | Leads, calls and conversions inflated |
| 8 | Lead channel, not campaign channel | Leads join spend on the lead's own `ChannelCode` | `findings_reference.sql: lead_campaign_channel_disagreement` | As assumption 4 | Some leads land in a different channel than their spend |
| 9 | `WHERE l.LeadStatus <> 'DEAD'` on a LEFT JOIN | The filter is in `WHERE`, not `ON`: a spend row with no lead, and any lead with a NULL status, fail it, so the LEFT JOIN behaves as an inner join | `legacy_row_counts`, `legacy_spend_and_period` | 0 groups dropped, 0 DEAD, 0 NULL status on this data (18 groups both ways) | Latent: spend with no leads would vanish from the report |
| 10 | Exact string joins in Spark | SQL Server's default collation ignores case and trailing spaces; Spark does not | `legacy_channel_collation` | 0 non-canonical spend or lead codes, 0 groups lost; but 9 campaign rows carry variants such as `DISPLAY ` and `display` | Latent: a literal port would silently drop rows once a source sends a variant |
| 11 | `NOLOCK` and the region cursor | Dirty reads during the 02:00 to 04:00 reload; one `INSERT` per region | None (behavioural) | Not measurable here | The port reads a Delta snapshot and runs one set-based query (fixture test: same rows per region) |

NTU policies are excluded in both versions, so they are not a difference.
