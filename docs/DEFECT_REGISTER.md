# Defect register

Found by a first profiling pass in pandas. Counts are exact but must be re-confirmed inside
the pipeline. Each defect maps to a DQ check, an assumption, or a finding.

| # | Defect | Evidence | Planned handling |
| --- | --- | --- | --- |
| 1 | Starter config declares `sep: ";"` for `calls_ca_us` | File has no semicolons; comma-delimited | Fix config; record as a finding |
| 2 | `lead_id` primary-key violations | 3 ids appear twice, with `CMP-1099` and `conflict.` emails | Quarantine both rows; do not silently resolve |
| 3 | Near-duplicate leads | 480 flagged; deriving by email or phone within 48h finds 757; flag covers about 97% | Rule-based map in `lead_dedup_map`; earliest lead survives |
| 4 | Leads with no identity key | 935 null email, 295 null phone, 35 both null | Fallback key; quarantine the 35 |
| 5 | Orphan campaign ids on leads | 310 on `CMP-9999`, 3 on `CMP-1099` | Map to `UNKNOWN` campaign; log exception |
| 6 | Leads outside the reporting window | 10 created 1 to 2 Jul 2026 | Keep in silver; exclude from period measures |
| 7 | Junk dates of birth | 20 before 1920; 292 after 2008, some in 2027 | Quarantine or null before computing age band |
| 8 | Channel label variants | 17 spellings for 8 channels (`PaidSearch`, `SOCIAL_PAID`, `DISPLAY `) | Mapping to `ref_channel` |
| 9 | Campaign date errors | 2 campaigns end before start; 249 spend rows after end, 81 before start | Flag; spend still counts (log assumption) |
| 10 | Spend duplicates, negatives, zeros | 40 exact duplicate rows; 3 negatives; 204 zero days; 18 null clicks | Dedupe; negatives as adjustments or quarantine |
| 11 | Spend in minor units | 180 GBP and AUD rows more than 20x the median (median ratio about 82x) | Detect and rescale, or quarantine; confirm rule |
| 12 | Missing FX rate | No CAD rate for 2026-06 | Raise or quarantine, never a silent null |
| 13 | Call duplicates | 346 + 194 exact duplicate rows | Dedupe on `call_id` |
| 14 | Call units and formats | Seconds vs minutes; ISO vs `dd/mm/yyyy hh:mm` | Normalise to seconds and UTC |
| 15 | AU timestamps do not behave like UTC | AU first call a median 18.4h after lead creation vs 22 minutes for UK | Test hypotheses (Sydney local, DST-aware); log evidence |
| 16 | Calls before lead creation; negative durations | 262 calls end before they start; about 0.4% start before the lead exists | Quarantine with severity |
| 17 | Ghost leads and agents on calls | 1,099 call lead ids not in leads; 624 calls on agents AG9001 to AG9004 | `UNKNOWN` member or quarantine |
| 18 | Agent roster | 3 agents (AG0008, AG0023, AG0037) appear twice with different team or seniority and no dates; 2 null regions | SCD2 needs an effective-date assumption |
| 19 | Policy duplicates | 28 repeated rows: 12 exact, 16 conflicting (differ on lead, call, agent or premium) | Coalesce non-null fields; log conflicts |
| 20 | Policies with no lead | 410 null `lead_id` (406 after deduplicating policies) | Keep in `fact_policy`; exclude from lead-attributed measures |
| 21 | Null annual premium | 620 rows, monthly premium present, ratio about 12 | Derive annual from monthly x 12 |
| 22 | Contradictory underwriting and status | 267 policies DECLINED yet INFORCE | Finding; confirm with PolicyCore owner |
| 23 | Policy dates beyond the period | Starts run to 31 Jul; applications to 18 Jul | Expected by design; anchors matter |
| 24 | Lead channel vs campaign channel | About 19% disagree before label cleanup (8,970 raw) | Re-measure after normalising; pick one source of truth |
| 25 | Consent integrity | 2,900 leads have events; 299 end WITHDRAWN yet 284 still show `consent_marketing = Y` | Governance finding; `consent_current` is the source of truth |
| 26 | Calls after a DO_NOT_CALL disposition | 4,461 UK and AU calls on 2,897 leads | Candidate headline finding |
| 27 | Agent team does not match region | The later roster rows for AG0008 (CA, `UK-Team-Alpha`) and AG0037 (AU, `UK-Team-Gamma`) name UK teams | Quarantine those team values in `silver.dq_exception`; keep the earlier team (assumption 8) |

## Read-across

- Defects 5, 8, 24 decide whether channel CPL is trustworthy.
- Defects 11 and 12 decide whether any spend figure is.
- Defects 3, 19, 20 decide the conversion numerator and denominator.

## Candidate headline findings (exactly three are required)

1. Calls after a DO_NOT_CALL disposition (defect 26).
2. Withdrawn consent still shown as granted on the lead record (defect 25).
3. Minor-unit spend rows that would overstate CPL roughly 80-fold (defect 11).

Each needs a number, the method that produced it, and the business action it implies.
