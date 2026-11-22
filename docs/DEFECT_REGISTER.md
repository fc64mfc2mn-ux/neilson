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
| 5 | Orphan campaign ids on leads | 310 on `CMP-9999`, 3 on `CMP-1099` | Map to `UNKNOWN` campaign; log exception | Decided |
| 6 | Leads outside the reporting window | 10 created 1 to 2 Jul 2026; all 10 are deduplicated into June survivors (`july_leads`) | Flagged, never quarantined; in `lead_dedup_map`, not `silver.lead` | Decided |
| 7 | Implausible dates of birth | Rule: age at creation under 18 or over 100. 60 nulled: before 1920, 20 of 20; 2008 to today, 20 of 272; future, 20 of 20. The other 252 (born 2008, aged 18) are valid (`dob_register_bands`) | DOB nulled, lead kept | Decided (assumption 18) |
| 8 | Channel label variants | 17 spellings for 8 channels (`PaidSearch`, `SOCIAL_PAID`, `DISPLAY `) | Mapping to `ref_channel` | Decided |
| 9 | Campaign date errors | 2 campaigns end before start; 249 spend rows after end, 81 before start | Flag; spend still counts (log assumption) | Decided (assumption still to be written) |
| 10 | Spend duplicates, negatives, zeros | 40 exact duplicate rows; 3 negatives; 204 zero days; 18 null clicks | Dedupe; negatives as adjustments or quarantine | Decided for duplicates and zeros. Negatives: decide at step 9, by checking whether each negative offsets a matching positive row (adjustment) or stands alone (quarantine) |
| 11 | Spend in minor units | 180 GBP and AUD rows more than 20x the median (median ratio about 82x) | Detect and rescale, or quarantine; confirm rule | Decide at step 9: distribution of each row's ratio to its campaign median, to set the rescale threshold (assumption 6) |
| 12 | Missing FX rate | No CAD rate for 2026-06 | Raise or quarantine, never a silent null | Decided (assumption 7) |
| 13 | Call duplicates | 346 + 194 exact duplicate rows | Quarantine every repeat of a byte-identical row; a `call_id` repeated with different content stops the stage | Done at step 8: 540 quarantined (`call_exact_duplicate`, info); no conflicting ids |
| 14 | Call units and formats | Seconds vs minutes; ISO vs `dd/mm/yyyy hh:mm` | Normalise to seconds and UTC | Done at step 8 (`normalise_calls.py`); unparseable times quarantined (none found) |
| 15 | AU timestamps do not behave like UTC | AU first call a median 18.5h after lead creation vs 22 minutes for UK | Test hypotheses (Sydney local, DST-aware); log evidence | Decided at step 8: AU kept as stored, flagged (`call_au_timestamp_unverified`). Policy dates fit AU call times moved back 10h; 8.5h stays unexplained (assumption 5) |
| 16 | Calls before lead creation; negative durations | 380 distinct calls end before they start (the first count, 262, was UK/AU raw rows only); 245 start before their lead (40 min to 24h before) | Quarantine with severity | Done at step 8: `call_end_before_start` (error), then `call_start_before_lead` (warning) |
| 17 | Ghost leads and agents on calls | 1,099 call lead ids in no lead file, all `LD-GHOST-<region>-<n>`; 624 rows (620 distinct calls) on agents AG9001 to AG9004 | `UNKNOWN` member or quarantine | Decided at step 8: both kept and flagged, `UNKNOWN` lead and agent members (assumption 20) |
| 18 | Agent roster | 3 agents (AG0008, AG0023, AG0037) appear twice with different team or seniority and no dates; 2 null regions | SCD2 needs an effective-date assumption | Decided: both versions kept (assumption 8); null region from team prefix, corroborated by call regions (assumption 16) |
| 19 | Policy duplicates | 28 repeated rows: 12 exact, 16 conflicting (differ on lead, call, agent or premium) | Coalesce non-null fields; log conflicts | Exact duplicates: decided. Conflicting duplicates: decide at step 9, by checking which fields conflict, to set a survivorship rule |
| 20 | Policies with no lead | 410 null `lead_id` (406 after deduplicating policies) | Keep in `fact_policy`; exclude from lead-attributed measures | Decided (assumption 11) |
| 21 | Null annual premium | 620 rows, monthly premium present, ratio about 12 | Derive annual from monthly x 12 | Decided |
| 22 | Contradictory underwriting and status | 267 policies DECLINED yet INFORCE | Finding; confirm with PolicyCore owner | Finding |
| 23 | Policy dates beyond the period | Starts run to 31 Jul; applications to 18 Jul | Expected by design; anchors matter | Decided |
| 24 | Lead channel vs campaign channel | About 19% disagree before label cleanup (8,970 raw). Pipeline re-measure: 23,644 of 47,687 raw, 600 after normalisation; the pandas figure could not be reproduced | Campaign channel is the source of truth | Decided (assumption 4); the 600 leads are flagged at step 7 |
| 25 | Consent integrity | 2,900 leads have events; 299 end WITHDRAWN yet 284 still show `consent_marketing = Y` | Governance finding; `consent_current` is the source of truth; both counts logged as flagged | Finding |
| 26 | Calls after a DO_NOT_CALL disposition | 4,461 UK and AU calls on 2,897 leads; also 1,966 CA and US calls on 1,389 leads | Candidate headline finding | Finding; flagged at step 8 (`call_after_do_not_call`, 6,311 calls in silver) |
| 27 | Agent team does not match region | The later roster rows for AG0008 (CA, `UK-Team-Alpha`) and AG0037 (AU, `UK-Team-Gamma`) name UK teams | Quarantine those team values in `silver.dq_exception`; keep the earlier team (assumption 8) | Decided |

## Read-across

- Defects 5, 8, 24 decide whether channel CPL is trustworthy.
- Defects 11 and 12 decide whether any spend figure is.
- Defects 3, 19, 20 decide the conversion numerator and denominator.

## Candidate headline findings (exactly three are required)

1. Calls after a DO_NOT_CALL disposition (defect 26).
2. Withdrawn consent still shown as granted on the lead record (defect 25).
3. Minor-unit spend rows that would overstate CPL roughly 80-fold (defect 11).

Each needs a number, the method that produced it, and the business action it implies.
