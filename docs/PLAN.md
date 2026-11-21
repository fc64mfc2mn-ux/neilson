# Build plan

Each step has a check that must pass before the next begins. Most end in a commit.
Order is dependency order. Deadline Thu 26 Nov 2026; target submission Wed 25 Nov.

| # | Step | Check |
| --- | --- | --- |
| 1 | **Scaffold.** Add `CLAUDE.md`, verify `data/` hashes, set up venv, run the starter fixture. | An empty Delta-enabled test session starts. Commit. |
| 2 | **Decision log first.** Draft `docs/ASSUMPTIONS.md` and finalise the KPI decision log from `DEFECT_REGISTER.md`. | Every defect has a planned handling. Commit. |
| 3 | **Config and runner.** Fix the `calls_ca_us` delimiter; finish `config.py`, `spark_session.py` (UTC), `logging_setup.py`, `run_pipeline.py`. | One command runs end to end with empty stages. |
| 4 | **Bronze.** 11 tables as strings with `_ingest_ts`, `_source_file`, `_row_hash`; both call files into `bronze.calls_raw`. | Row counts equal the manifest (calls 41,000 + 23,000). A second run appends. Commit. |
| 5 | **DQ framework.** Implement `write_dq_exception`. | A unit test writes and reads a rejected row. |
| 6 | **Silver reference and dimensions.** Campaign (channel normalisation, orphans), FX (missing June CAD), agent (duplicates, null region), region, channel. | Every raw row is in silver or in `dq_exception`. |
| 7 | **Silver leads and consent.** Dedup map (48h email or phone rule, PK conflicts quarantined, unidentifiable leads flagged); latest-event consent with a phone flag. | Surviving plus mapped leads equal 47,997 distinct ids. Commit. |
| 8 | **Silver calls.** Union, units to seconds, timestamps to UTC per the timezone evidence, exact duplicates removed, bad durations quarantined. | Timezone test passes; rule logged. Commit. |
| 9 | **Silver policy and spend.** Policy duplicate coalescing, annual premium from monthly, minor-unit spend handling, spend duplicates and negatives. | Rows in, out and rejected reconcile per table. Commit. |
| 10 | **Gold dimensions.** Date (all 2026), region, channel, campaign, product, agent (current-state first), `UNKNOWN` members. | Every fact foreign key will resolve. |
| 11 | **PII policy and `dim_lead`.** Salted SHA-256 (salt from secrets), names dropped, 10-year age band, outward postcode, health flags excluded from the no-PII view. | A test scans every gold column: no raw identifier, call note or recording URL. Commit. |
| 12 | **Gold facts and funnel.** Spend, lead, call, policy, `fact_lead_funnel` (21 columns), both bridges. | Funnel rows equal distinct `lead_key`; GBP spend reconciles to silver. Commit. |
| 13 | **Tests, CI, DQ gate.** The six named tests; CI that lints, tests, and fails when error-severity exceptions exceed the runbook threshold. | Gate fails on a seeded breach and passes on clean data. Commit. |
| 14 | **Legacy port.** Spark SQL port of `sp_MarketingLeadSummary`, a naive translation beside it, parity test. | Test asserts the stated differences. Commit. |
| 15 | **Export for Power BI.** Gold to Parquet or CSV in a folder the VM can read. | The VM opens a gold table. |
| 16 | **Power BI model.** Star schema, marked date table, CPL parameter table, all ten measures plus MoM and target variance, descriptions, DAX mirrored in `powerbi/measures.dax`. | Measure values equal the Python queries. |
| 17 | **Report page and RLS.** Four visuals; dynamic `Regional_Analyst` role; view-as screenshots (two-region user, unmapped user). | Evidence saved in `powerbi/`. Commit. |
| 18 | **Conversion reconciliation.** Ops measure plus the step-by-step bridge between the two conversion numbers. | The bridge sums exactly. |
| 19 | **Performance artefact.** Tune one measure; before and after in DAX Studio. | Two screenshots and a stated change. Commit. |
| 20 | **Unity Catalog grants and Asset Bundle.** Include a row filter or mask and all four roles. | SQL reads correctly against the entitlements matrix. |
| 21 | **Documents.** All of `docs/`, plus README. | Required sections present; word counts in range. Commit. |
| 22 | **Final review.** Fresh clone, one-command run on the Mac, `data/` hashes, lint, tests, secrets check, then email Emily. | All pass. |

If time slips, cut from the bottom of the stretch list (20, then 19), never steps 1 to 13.

## Schedule (about 29 hours)

| Day | Focus | Steps | Hours |
| --- | --- | --- | --- |
| Sat 21 | VM test, scaffold, decisions, config, bronze | 1 to 4 | 5 |
| Sun 22 | DQ framework and silver | 5 to 9 | 6 |
| Mon 23 | Gold, PII control, tests, CI, legacy port | 10 to 14 | 6 |
| Tue 24 | Export, Power BI, RLS, bridge | 15 to 19 | 6 |
| Wed 25 | Grants, documents, final review, submit | 20 to 22 | 6 |

## Assumptions to log (first pass)

1. "International" is CA, AU, US; "UK and Ireland" is UK. No Ireland data exists.
2. Connected calls: CONNECTED, QUOTED, APPLICATION_STARTED, SOLD.
3. Policy status: exclude NTU; test the effect of also excluding CANCELLED and LAPSED.
4. Channel source of truth: the campaign's channel after label normalisation.
5. Call-file timezones: decided by evidence, logged with the evidence.
6. Minor-unit spend rows: rescale on a stated ratio rule; quarantine the ambiguous.
7. Missing CAD June rate: quarantine affected spend, fail the DQ gate loudly.
8. SCD2 effective dates: stated proxy (for example first-seen load date).
9. Business hours: weekday within the region's start and end in its own timezone.
10. Whether `attribution_window_days` limits which policies count for a lead: open.
11. Policies with no lead: kept in `fact_policy`, excluded from lead-attributed measures.
12. Fiscal quarter: calendar quarters, stated explicitly.

## Checks to run on day one

- `java -version` shows 17; `python3.11 --version` works.
- Windows VM test: blank Power BI report, CSV import from a shared Mac folder, one measure in a card, saved `.pbix`.
- Whether Power BI Desktop can read a local Delta folder. If not, use the Parquet or CSV export (step 15).
