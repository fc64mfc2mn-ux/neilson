# Defect register

Found by a first profiling pass in pandas. Counts are exact but must be re-confirmed inside
the pipeline. Each defect maps to a DQ check, an assumption, or a finding.

Status is one of: **Decided** (handling fixed); **Decide at step N** (the measurement that
settles it is named); **Finding** (written up for the owner, data not changed).

| # | Defect | Evidence | Planned handling | Status |
| --- | --- | --- | --- | --- |
| 1 | Starter config declares `sep: ";"` for `calls_ca_us` | File has no semicolons; comma-delimited | Fix config; record as a finding | Decided (fix at step 3) |
| 2 | `lead_id` primary-key violations | 3 ids appear twice, with `CMP-1099` and `conflict.` emails | Quarantine both rows; do not silently resolve | Decided |
| 3 | Near-duplicate leads | 480 flagged. The rule finds 758: 478 agree with the flag, 1 is reversed (the "duplicate" came first), 1 shares no key, 279 the source never flagged (`findings_leads.sql`) | Rule-based map in `lead_dedup_map`; earliest lead survives (assumption 19) | Decided |
| 4 | Leads with no identity key | 935 null email, 295 null phone, 35 both null | Fallback key; quarantine the 35 | Decided |
| 5 | Orphan campaign ids on leads | 310 on `CMP-9999`, 3 on `CMP-1099` | Map to `UNKNOWN` campaign; log exception | Decided; at step 10 they map to the `UNKNOWN` campaign member (304 surviving leads) |
| 6 | Leads outside the reporting window | 10 created 1 to 2 Jul 2026; all 10 are deduplicated into June survivors (`july_leads`) | Flagged, never quarantined; in `lead_dedup_map`, not `silver.lead` | Decided |
| 7 | Implausible dates of birth | Rule: age at creation under 18 or over 100. 60 nulled: before 1920, 20 of 20; 2008 to today, 20 of 272; future, 20 of 20. The other 252 (born 2008, aged 18) are valid (`dob_register_bands`) | DOB nulled, lead kept | Decided (assumption 18) |
| 8 | Channel label variants | 17 spellings for 8 channels (`PaidSearch`, `SOCIAL_PAID`, `DISPLAY `) | Mapping to `ref_channel` | Decided |
| 9 | Campaign date errors | 2 campaigns end before start (CMP-1051, CMP-1054); 325 kept spend rows outside their campaign's dates (246 after end, 79 before start) | Flag; spend still counts | Done at step 9: flagged (`spend_outside_campaign_dates`), counted (assumption 22) |
| 10 | Spend duplicates, negatives, zeros | 40 exact duplicate rows; 3 negatives; 204 zero rows (200 distinct and kept, 91 ORGANIC); 18 null clicks (17 kept) | Dedupe; negatives as adjustments or quarantine | Done at step 9: copies quarantined; negatives quarantined (none offsets a positive); zeros kept, 109 on paid channels flagged; null clicks kept null, flagged (assumption 21) |
| 11 | Spend in minor units | Two whole campaigns: CMP-1061 (AUD) and CMP-1062 (GBP), median CPM 92x and 99.8x their currency's median; every other campaign 0.88 to 1.05x | Detect and rescale, or quarantine; confirm rule | Done at step 9: 181 rows divided by 100 (`spend_minor_units_rescaled`); rule: CPM 20x or more and in band after /100, else quarantine; none ambiguous (assumption 6) |
| 12 | Missing FX rate | No CAD rate for 2026-06: 180 distinct spend rows (187 with copies) | Raise or quarantine, never a silent null | Done at step 9: quarantined, error (`spend_fx_rate_missing`; assumption 7) |
| 13 | Call duplicates | 346 + 194 exact duplicate rows | Quarantine every repeat of a byte-identical row; a `call_id` repeated with different content stops the stage | Done at step 8: 540 quarantined (`call_exact_duplicate`, info); no conflicting ids |
| 14 | Call units and formats | Seconds vs minutes; ISO vs `dd/mm/yyyy hh:mm` | Normalise to seconds and UTC | Done at step 8 (`normalise_calls.py`); unparseable times quarantined (none found) |
| 15 | AU call timestamps are Sydney local time, not UTC | As stored, AU first call a median 18.5h after lead creation vs 22 minutes for UK; policy application dates match the linked AU call date for 248 of 426 policies, all 426 after conversion | Convert AU from `Australia/Sydney` (DST-aware), per region in config | Done at step 8 (assumption 5). Residual: AU first calls still a median 512 min after the lead; flagged (`call_au_residual_offset`), open, not corrected |
| 16 | Calls before lead creation; negative durations | 380 distinct calls end before they start; 251 start before their lead after AU conversion (245 as stored), 2h to 24h before | Quarantine with severity | Done at step 8: `call_end_before_start` (error), then `call_start_before_lead` (warning) |
| 17 | Ghost leads and agents on calls | 1,099 call lead ids in no lead file, all `LD-GHOST-<region>-<n>`; 624 rows (620 distinct calls) on agents AG9001 to AG9004 | `UNKNOWN` member or quarantine | Decided at step 8: both kept and flagged, `UNKNOWN` lead and agent members (assumption 20); at step 10 the 615 AG9001 to AG9004 calls resolve to the `UNKNOWN` agent |
| 18 | Agent roster | 3 agents (AG0008, AG0023, AG0037) appear twice with different team or seniority and no dates; 2 null regions | SCD2 needs an effective-date assumption | Done at step 10: `gold.dim_agent` SCD2, 45 versions for 42 agents plus `UNKNOWN` (assumption 8); null region from team prefix (assumption 16) |
| 19 | Policy duplicates | 28 repeated rows: 12 exact, 16 conflicting. Of the conflicts: 6 lead and 11 call null-vs-value; 1 agent and 3 premium value-vs-value; 2 premium null-vs-value | Coalesce non-null fields; log conflicts | Done at step 9: copies merged, earliest copy wins value conflicts (matches the call's agent and monthly x 12), nulls filled; each later copy quarantined with the differing fields (assumption 23) |
| 20 | Policies with no lead | 410 null `lead_id` rows; 406 once copies are removed; 404 after coalescing | Keep in `fact_policy`; exclude from lead-attributed measures | Done at step 9: kept, flagged (`policy_lead_missing`; assumption 11) |
| 21 | Null annual premium | 620 rows (617 policies after dedupe), monthly present; on the 4,555 others annual = monthly x 12 (to 0.01) for 4,368; 187 sit at 12.6 to 15 | Derive annual from monthly x 12 | Done at step 9: 617 derived (`policy_annual_premium_derived`, corrected); the 187 above 12x flagged (`policy_premium_above_12x_monthly`, warning) for the owner (OPEN_QUESTIONS a) |
| 22 | Contradictory underwriting and status | 267 rows DECLINED yet INFORCE (265 policies) | Finding; confirm with PolicyCore owner | Finding (query `declined_yet_inforce`); data unchanged |
| 23 | Policy dates beyond the period | Starts run to 31 Jul; applications to 18 Jul | Expected by design; anchors matter | Decided |
| 24 | Lead channel vs campaign channel | About 19% disagree before label cleanup (8,970 raw). Pipeline re-measure: 23,644 of 47,687 raw, 600 after normalisation; the pandas figure could not be reproduced | Campaign channel is the source of truth | Decided (assumption 4); the 600 leads are flagged at step 7 |
| 25 | Consent integrity | 2,900 leads have events; 299 end WITHDRAWN yet 284 still show `consent_marketing = Y` | Governance finding; `consent_current` is the source of truth; both counts logged as flagged | Finding |
| 26 | Calls after a DO_NOT_CALL disposition | 6,427 distinct calls on 4,286 leads before quarantine, all four regions (UK and AU 4,461 on 2,897; CA and US 1,966 on 1,389). Headline: 6,311 calls on 4,223 leads in silver; the 116 gap is 38 calls quarantined themselves and 78 whose earlier DO_NOT_CALL call was quarantined | Candidate headline finding | Finding; flagged at step 8 (`call_after_do_not_call`) |
| 27 | Agent team does not match region | The later roster rows for AG0008 (CA, `UK-Team-Alpha`) and AG0037 (AU, `UK-Team-Gamma`) name UK teams | Quarantine those team values in `silver.dq_exception`; keep the earlier team (assumption 8) | Decided |
| 28 | Clicks exceed impressions on spend rows | 6 rows (CMP-1041, 1044, 1045, 1049, 1052, 1054) with clicks above impressions; spend and CPC normal for the campaign | Flag; spend still counts; impressions on those rows are not trusted | Decided at step 9 (found while testing the minor-unit rule) |

## Read-across

- Defects 5, 8, 24 decide whether channel CPL is trustworthy.
- Defects 11 and 12 decide whether any spend figure is.
- Defects 3, 19, 20 decide the conversion numerator and denominator.

## Candidate headline findings (exactly three are required)

1. Calls after a DO_NOT_CALL disposition (defect 26).
2. Withdrawn consent still shown as granted on the lead record (defect 25).
3. Minor-unit spend rows that would overstate CPL roughly 80-fold (defect 11).

Each needs a number, the method that produced it, and the business action it implies.
