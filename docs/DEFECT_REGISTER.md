# Defect register

Found by a first profiling pass in pandas. Counts are exact but must be re-confirmed inside
the pipeline. Each defect maps to a DQ check, an assumption, or a finding.

Status is one of: **Decided** (handling fixed); **Decide at step N** (the measurement that
settles it is named); **Finding** (written up for the owner, data not changed).

| # | Defect | Evidence | Planned handling | Status |
| --- | --- | --- | --- | --- |
| 1 | Starter config declares `sep: ";"` for `calls_ca_us` | File has no semicolons; comma-delimited | Fix config; record as a finding | Decided (fix at step 3) |
| 2 | `lead_id` primary-key violations | 3 ids appear twice, with `CMP-1099` and `conflict.` emails | Quarantine both rows; do not silently resolve | Decided; DQ gate line at step 13, expected 6 rows |
| 3 | Near-duplicate leads | 480 flagged. The rule finds 758: 478 agree with the flag, 1 is reversed (the "duplicate" came first), 1 shares no key, 279 the source never flagged (`findings_leads.sql`) | Rule-based map in `lead_dedup_map`; earliest lead survives (assumption 19) | Decided; step 14: the legacy procedure still counts these (`docs/LEGACY_PORT.md`, difference 7) |
| 4 | Leads with no identity key | 935 null email, 295 null phone, 35 both null | Fallback key; quarantine the 35 | Decided; DQ gate line at step 13, expected 35 |
| 5 | Orphan campaign ids on leads | 310 on `CMP-9999`, 3 on `CMP-1099` | Map to `UNKNOWN` campaign; log exception | Decided; at step 10 they map to the `UNKNOWN` campaign member (304 surviving leads) |
| 6 | Leads outside the reporting window | 10 created 1 to 2 Jul 2026; all 10 are deduplicated into June survivors (`july_leads`) | Flagged, never quarantined; in `lead_dedup_map`, not `silver.lead` | Decided |
| 7 | Implausible dates of birth | Rule: age at creation under 18 or over 100. 60 nulled: before 1920, 20 of 20; 2008 to today, 20 of 272; future, 20 of 20. The other 252 (born 2008, aged 18) are valid (`dob_register_bands`) | DOB nulled, lead kept | Decided (assumption 18); `UNKNOWN` age band in `gold.dim_lead` (step 11) |
| 8 | Channel label variants | 17 spellings for 8 channels (`PaidSearch`, `SOCIAL_PAID`, `DISPLAY `) | Mapping to `ref_channel` | Decided |
| 9 | Campaign date errors | 2 campaigns end before start (CMP-1051, CMP-1054); 325 kept spend rows outside their campaign's dates (246 after end, 79 before start) | Flag; spend still counts | Done at step 9: flagged (`spend_outside_campaign_dates`), counted (assumption 22) |
| 10 | Spend duplicates, negatives, zeros | 40 exact duplicate rows; 3 negatives; 204 zero rows (200 distinct and kept, 91 ORGANIC); 18 null clicks (17 kept) | Dedupe; negatives as adjustments or quarantine | Done at step 9: copies quarantined; negatives quarantined (none offsets a positive); zeros kept, 109 on paid channels flagged; null clicks kept null, flagged (assumption 21) |
| 11 | Spend in minor units | Two whole campaigns: CMP-1061 (AUD) and CMP-1062 (GBP), median CPM 92x and 99.8x their currency's median; every other campaign 0.88 to 1.05x | Detect and rescale, or quarantine; confirm rule | Done at step 9: 181 rows divided by 100 (`spend_minor_units_rescaled`); rule: CPM 20x or more and in band after /100, else quarantine; none ambiguous (assumption 6); step 14: the legacy procedure still counts these (`docs/LEGACY_PORT.md`, difference 5) |
| 12 | Missing FX rate | No CAD rate for 2026-06: 180 distinct spend rows (187 with copies) | Raise or quarantine, never a silent null | Done at step 9: quarantined, error (`spend_fx_rate_missing`; assumption 7); step 12: 675 policies keep a null GBP premium, flagged (`policy_fx_rate_missing`; assumption 27); DQ gate line at step 13, expected 1 rate + 180 spend rows |
| 13 | Call duplicates | 346 + 194 exact duplicate rows | Quarantine every repeat of a byte-identical row; a `call_id` repeated with different content stops the stage | Done at step 8: 540 quarantined (`call_exact_duplicate`, info); no conflicting ids; step 14: the legacy procedure still counts these (`docs/LEGACY_PORT.md`, difference 7) |
| 14 | Call units and formats | Seconds vs minutes; ISO vs `dd/mm/yyyy hh:mm` | Normalise to seconds and UTC | Done at step 8 (`normalise_calls.py`); unparseable times quarantined (none found); DQ gate line at step 13, expected 0 |
| 15 | AU call timestamps are Sydney local time, not UTC | As stored, AU first call a median 18.5h after lead creation vs 22 minutes for UK; policy application dates match the linked AU call date for 248 of 426 policies, all 426 after conversion | Convert AU from `Australia/Sydney` (DST-aware), per region in config | Done at step 8 (assumption 5). Residual: AU first calls still a median 512 min after the lead; flagged (`call_au_residual_offset`), open, not corrected |
| 16 | Calls before lead creation; negative durations | 380 distinct calls end before they start; 251 start before their lead after AU conversion (245 as stored), 2h to 24h before | Quarantine with severity | Done at step 8: `call_end_before_start` (error), then `call_start_before_lead` (warning); DQ gate line at step 13, expected 380 (end before start) |
| 17 | Ghost leads and agents on calls | 1,099 call lead ids in no lead file, all `LD-GHOST-<region>-<n>`; 624 rows (620 distinct calls) on agents AG9001 to AG9004 | `UNKNOWN` member or quarantine | Decided at step 8: both kept and flagged, `UNKNOWN` lead and agent members (assumption 20); at step 10 the 615 AG9001 to AG9004 calls resolve to the `UNKNOWN` agent; at step 12 the 1,139 calls with no surviving lead keep a null `lead_key` in `fact_call` and are not in `bridge_lead_call` |
| 18 | Agent roster | 3 agents (AG0008, AG0023, AG0037) appear twice with different team or seniority and no dates; 2 null regions | SCD2 needs an effective-date assumption | Done at step 10: `gold.dim_agent` SCD2, 45 versions for 42 agents plus `UNKNOWN` (assumption 8); null region from team prefix (assumption 16) |
| 19 | Policy duplicates | 28 repeated rows: 12 exact, 16 conflicting. Of the conflicts: 6 lead and 11 call null-vs-value; 1 agent and 3 premium value-vs-value; 2 premium null-vs-value | Coalesce non-null fields; log conflicts | Done at step 9: copies merged, earliest copy wins value conflicts (matches the call's agent and monthly x 12), nulls filled; each later copy quarantined with the differing fields (assumption 23); step 14: the legacy procedure still counts these (`docs/LEGACY_PORT.md`, difference 7) |
| 20 | Policies with no lead | 410 null `lead_id` rows; 406 once copies are removed; 404 after coalescing | Keep in `fact_policy`; exclude from lead-attributed measures | Done at step 9: kept, flagged (`policy_lead_missing`; assumption 11); step 12: 405 policies with a null `lead_key` in `fact_policy` (404 no lead, 1 quarantined lead) |
| 21 | Null annual premium | 620 rows (617 policies after dedupe), monthly present; on the 4,555 others annual = monthly x 12 (to 0.01) for 4,368; 187 sit at 12.6 to 15 | Derive annual from monthly x 12 | Done at step 9: 617 derived (`policy_annual_premium_derived`, corrected); the 187 above 12x flagged (`policy_premium_above_12x_monthly`, warning) for the owner (OPEN_QUESTIONS a) |
| 22 | Contradictory underwriting and status | 267 rows DECLINED yet INFORCE (265 policies) | Finding; confirm with PolicyCore owner | Finding (query `declined_yet_inforce`); data unchanged |
| 23 | Policy dates beyond the period | Starts run to 31 Jul; applications to 18 Jul | Expected by design; anchors matter | Decided |
| 24 | Lead channel vs campaign channel | About 19% disagree before label cleanup (8,970 raw). Pipeline re-measure: 23,644 of 47,687 raw, 600 after normalisation; the pandas figure could not be reproduced | Campaign channel is the source of truth | Decided (assumption 4); the 600 leads are flagged at step 7 |
| 25 | Consent integrity | 2,900 leads have events; 299 end WITHDRAWN yet 284 still show `consent_marketing = Y` | Governance finding; `consent_current` is the source of truth; both counts logged as flagged | Finding; `gold.dim_lead` takes consent from `consent_current` (step 11) |
| 26 | Calls after a DO_NOT_CALL disposition | 6,427 distinct calls on 4,286 leads before quarantine, all four regions (UK and AU 4,461 on 2,897; CA and US 1,966 on 1,389). Headline: 6,311 calls on 4,223 leads in silver; the 116 gap is 38 calls quarantined themselves and 78 whose earlier DO_NOT_CALL call was quarantined | Candidate headline finding | Finding; flagged at step 8 (`call_after_do_not_call`) |
| 27 | Agent team does not match region | The later roster rows for AG0008 (CA, `UK-Team-Alpha`) and AG0037 (AU, `UK-Team-Gamma`) name UK teams | Quarantine those team values in `silver.dq_exception`; keep the earlier team (assumption 8) | Decided |
| 28 | Clicks exceed impressions on spend rows | 6 rows (CMP-1041, 1044, 1045, 1049, 1052, 1054) with clicks above impressions; spend and CPC normal for the campaign | Flag; spend still counts; impressions on those rows are not trusted | Decided at step 9 (found while testing the minor-unit rule) |
| 29 | Lead postcode not in its region's format | 24 surviving leads: UK 11 with a four-digit (AU-style) postcode and 10 with a Canadian one; US 3 with four digits (a lost leading zero?). Every phone fits one of the UK, AU or NANP formats (`postcode_region_format` in `findings_leads.sql`; phones in `test_pii_policy.py`) | Outward postcode withheld (null), lead kept, flagged (`lead_postcode_region_mismatch`); a phone in no format gets no hash, flagged (`lead_phone_unparseable`) | Decided at step 11 |

## Read-across

- Defects 5, 8, 24 decide whether channel CPL is trustworthy.
- Defects 11 and 12 decide whether any spend figure is.
- Defects 3, 19, 20 decide the conversion numerator and denominator.

## Candidate headline findings (exactly three are required)

1. Calls after a DO_NOT_CALL disposition (defect 26).
2. Withdrawn consent still shown as granted on the lead record (defect 25).
3. Minor-unit spend rows that would overstate CPL roughly 80-fold (defect 11).

Each needs a number, the method that produced it, and the business action it implies.

## Assumption detail (moved from `ASSUMPTIONS.md`)

`docs/ASSUMPTIONS.md` keeps one short entry per assumption, to the brief's length. The full
evidence for each entry is kept here under the same number. Entries 17, 25 and 26 (access
and PII) are in `DATA_CLASSIFICATION.md`; entry 13 is in `KPI_DECISION_LOG.md`.

1\. **Region groupings.** *Ambiguity:* the director titles say "UK and Ireland" and
   "International"; `ref_region` holds only UK, CA, AU and US. *Options:* map by title;
   report Ireland as missing. *Choice:* UK and Ireland = UK; International = CA, AU, US.
   *Reason:* these are the only regions in the data. *Reverse:* edit the region-group
   column in `gold.dim_region`.

2\. **Connected calls.** *Ambiguity:* which dispositions mean a conversation. *Options:*
   CONNECTED only; CONNECTED plus later stages. *Choice:* CONNECTED, QUOTED,
   APPLICATION_STARTED, SOLD. *Reason:* matches the brief's `Contact Rate`; a quote or
   sale implies contact. *Reverse:* change the disposition list behind the connected flag
   in silver calls.

3\. **Policy status.** *Ambiguity:* which policies count as converted. *Options:* all;
   exclude NTU; also exclude CANCELLED and LAPSED. *Choice (decided, step 16):* exclude
   NTU only. *Reason:* the legacy procedure excludes NTU, and a cancelled or lapsed policy
   was still sold; conversion is the sale, retention is another measure. Measured
   (`findings_powerbi.sql: conversion_by_status_rule`): the stricter rule lowers the rate
   from 9.53% to 8.11% (1.42 points, 14.9% relative); by region 0.82 to 1.64 points, 13.4%
   to 15.9% relative, so the regional ranking is unchanged. *Reverse:* the status filter
   in `Policies Linked to Surviving Leads` (`powerbi/measures.dax`).

4\. **Channel source of truth.** *Ambiguity:* lead and campaign channel can disagree.
   *Options:* lead channel; campaign channel. *Choice:* campaign channel, after
   normalising labels to `ref_channel`. *Reason:* spend is recorded by campaign; after
   normalisation only 600 of 47,687 leads disagree, scattered across all channel pairs
   (`findings_reference.sql`). *Reverse:* switch the `channel_key` source in gold.

5\. **Call-file timezones.** *Ambiguity:* neither call file states its timezone.
   *Options:* UTC; region local time (DST-aware); a fixed offset. *Choice:* UK, CA and
   US as UTC; AU converted from `Australia/Sydney`. *Reason:* a policy's application
   date equals its linked call's date for every UK, CA and US policy as stored, and for
   all 426 AU policies only after the conversion (248 before); AU lead timestamps show
   no shift. After conversion AU first calls still come a median 512 min after the lead
   (22 to 45 elsewhere): flagged as an open observation, not corrected. *Reverse:*
   `calls.source_timezone` in config. Queries: `src/sql/findings_calls.sql`.

6\. **Minor-unit spend.** *Ambiguity:* some spend sits about 100x too high. *Options:*
   keep; divide by 100; quarantine. *Evidence:* two whole campaigns, CMP-1061 (AUD) and
   CMP-1062 (GBP), have median CPM 92x and 99.8x their currency's; every other campaign
   is 0.88 to 1.05x. Their CPC is 100x too, but click-through is normal, so spend is the
   wrong field. *Rule:* a row with CPM 20x or more its currency median is divided by 100
   if the result is 0.5 to 2x; otherwise quarantined as ambiguous. 181 rescaled, none
   ambiguous (normal rows reach 1.51x, minor rows start at 68x). Blended CPL: £319.95
   before, £34.39 after. *Reverse:* `spend.minor_units` in config.

7\. **Missing CAD rate for June.** *Options:* carry May forward; average the months;
   quarantine. *Choice:* quarantine CAD June spend at error severity, so the DQ gate
   fails. *Reason:* carrying May forward would invent a Treasury rate, and the brief
   forbids silent nulls. *Reverse:* load the rate once Treasury supplies it, then rerun.

8\. **SCD2 effective dates.** *Ambiguity:* some agents appear twice with a different team
   or seniority and no dates. *Options:* first-seen load date; file row order; current
   state only. *Choice (proxy):* versions in bronze `_source_row` order. One version:
   valid 2026-01-01 to 9999-12-31. Two: the earlier 2026-01-01 to 2026-03-31, the later
   current from 2026-04-01 (reporting start); `valid_to` inclusive. A call takes the
   version valid on its UTC date. Three or more versions stop the stage. The later
   AG0008 and AG0037 rows name UK teams; those team values are quarantined and the
   earlier team kept (defect 27). *Reason:* the source has no dates, so the proxy is
   stated, not hidden. *Reverse:* replace the dates with HR effective dates.

9\. **Business hours.** *Ambiguity:* Ops excludes leads "outside regional business
   hours" without defining weekends or holidays. *Options:* `ref_region` hours every day;
   weekdays only; weekdays minus holidays. *Choice:* Monday to Friday, start inclusive
   and end exclusive, in the region's own timezone. No holiday calendar. *Reason:* the Ops
   note names overnight and weekend; the pack has no holiday data. *Reverse:* change the
   flag logic or add a holiday table.
   Applied at step 12 as `lead_in_business_hours_flag` (`fact_lead`, `fact_lead_funnel`);
   the weekend and holiday rule is open question c.

10\. **Attribution window.** *Ambiguity:* `ref_channel` gives `attribution_window_days`
    (0 to 30) but nothing says whether it limits which policies count. *Options:* ignore
    it; count only policies within the window. *Choice (decided):* not applied; impact 164
    of 4,767 lead-attributed policies (3.4%) apply after their channel's window
    (`findings_policy.sql: policies_outside_attribution_window`). Only relevant to
    `Cost Per Policy (GBP)` (measure 10), not core. *Reverse:* one filter in the
    lead-attributed policy measures. Open question h asks Marketing to confirm.

11\. **Policies with no lead.** *Options:* drop; keep against an `UNKNOWN` lead; keep with
    a NULL key. *Choice:* keep them in `gold.fact_policy` with a NULL `lead_key`, and
    exclude them from lead-attributed measures. Power BI shows them as blank. *Reason:*
    they are real sales but cannot be attributed. `gold.dim_lead` is one row per
    deduplicated lead, so an `UNKNOWN` member would add a lead that does not exist.
    *Reverse:* a measure filter.

12\. **Fiscal quarter.** *Ambiguity:* the pack gives no fiscal calendar. *Choice:*
    calendar quarters, so the reporting period is Q2 2026. *Reverse:* add fiscal columns
    to `gold.dim_date`.

14\. **Dependency addition: PyYAML.** *Ambiguity:* the stack is fixed, but the brief requires
    a YAML config, and neither the starter `requirements.txt` nor PySpark includes a YAML
    parser. *Options:* add a parser; write one by hand; switch the config to JSON.
    *Choice:* add `pyyaml==6.0.2`, pinned, used only by `src/common/config.py`. *Reason:*
    the brief names `config/config.yaml`; a hand-written parser would be fragile.
    *Reverse:* remove the pin and load a JSON config instead.

15\. **Bronze reruns.** *Ambiguity:* bronze must be append-only, but a second run of the one
    command must not double every row. *Options:* drop and reload each run; append
    blindly; append only changed files. *Choice:* each landing is a batch with one
    `_ingest_ts`; its content fingerprint goes in `bronze.ingest_log`. An unchanged file
    appends nothing, a changed file appends a new batch, and silver reads the latest
    batch per file. *Reason:* keeps bronze append-only and auditable, and makes reruns
    idempotent. Bronze also stores `_source_row`, the record's position in its file
    (from 1, header excluded), outside `_row_hash`. *Reverse:* delete the file's
    `bronze.ingest_log` rows to force a reload.

16\. **Agents with no region.** *Ambiguity:* AG0007 and AG0035 have no roster region.
    *Options:* leave `UNKNOWN`; use the team prefix. *Choice:* team prefix (UK). *Reason:*
    both take calls only in the UK, and the prefix matches the region on 41 of 43 rows
    (`findings_reference.sql`). *Reverse:* drop the fallback in `conform_agents`.

18\. **Implausible dates of birth.** *Ambiguity:* the register flags about 312 DOBs.
    *Options:* quarantine the lead; null the DOB. *Choice:* null a DOB when the whole-year
    age at lead creation is under 18 or over 100; keep the lead. That nulls 60: before
    1920, 20 of 20; 2008 to today, 20 of 272 (the other 252 were born in 2008 and are
    18); future, 20 of 20 (`dob_register_bands`). *Reason:* the leads are otherwise
    normal. *Reverse:* `MIN_AGE`/`MAX_AGE` in `src/silver/leads.py`.

19\. **Dedup detail.** *Ambiguity:* "within 48 hours" of the previous match or of the
    first lead; phone formats vary. *Choice:* chained gaps, linked across email and phone;
    phone compared on digits only. *Reason:* gives 758 duplicates and 47,201 survivors.
    *Reverse:* `src/silver/dedupe_leads.py`.

20\. **Calls on unresolvable leads (defect 17).** *Ambiguity:* some calls name a lead that
    is not a surviving lead. *Options:* quarantine; an `UNKNOWN` lead; a null key.
    *Choice:* keep the calls, flagged in silver. In `gold.fact_call` they have a null
    `lead_key` and `lead_match_status` says why: 1,094 `lead_not_found` (ids in no lead
    file, all `LD-GHOST-<region>-<n>`) and 45 `lead_quarantined` (leads quarantined at
    step 7). They are not in `gold.bridge_lead_call` (61,690 = 62,829 - 1,139), so they
    never reach the lead-grain funnel. AG9001 to AG9004 map to the `UNKNOWN` agent.
    *Reason:* `dim_lead` is one row per real lead (as assumption 11), and the calls are
    real agent work for handle time and grade of service. *Reverse:* quarantine the
    flagged rows in `src/silver/calls.py`.

21\. **Negative, zero and click-less spend (defect 10).** *Negatives (3):* none offsets a
    positive of its campaign, each is that day's only row at under 0.3x the median:
    quarantined, not netted. *Zeros (200):* kept; 91 are ORGANIC (no media cost), the
    rest have tiny delivery; the 109 on paid channels are flagged. *Null clicks (17):*
    kept as null, never 0, flagged; spend and impressions are present. *Reverse:*
    `src/silver/spend.py`.

22\. **Spend outside campaign dates (defect 9).** *Choice:* the spend counts and is
    flagged (325 rows), because the media cost was incurred whatever the plan said; two
    campaigns end before they start. *Reverse:* a measure filter on
    `outside_campaign_dates`.

23\. **Conflicting policy copies (defect 19).** *Choice:* merge; the earliest copy wins a
    value conflict, a null is filled from the other copy. *Reason:* the earliest matches
    the linked call's agent (1 of 1) and monthly x 12 (2 of 3; the later copy 0 of 3).
    *Reverse:* `merge_conflicting` in `src/silver/policy.py`.

24\. **Surrogate key width.** *Choice:* the first 52 bits of SHA-256 of the natural key,
    below 2^53 so Power BI's double-precision numbers hold every key exactly (60 bits did
    not); deterministic, with a collision test per dimension. *Reverse:* `KEY_HEX_DIGITS`
    in `src/gold/build_dimensions.py`.

27\. **Policy premium in GBP.** *Ambiguity:* which month's rate. *Choice (decided):* the
    application month (the sale event; spend also uses its own activity month). No rate
    is invented: 675 policies keep a null `annual_premium_gbp`, flagged
    `gbp_rate_missing` and in `dq_exception` (`policy_fx_rate_missing`, warning). By
    cause and quarter (`findings_policy.sql: policy_gbp_missing_by_cause`): 517 CAD June,
    all inside Q2 (Q2 coverage 89.7% of 5,014 policies); 158 July, any currency, all
    outside Q2 (0% coverage). `Average Annual Premium (GBP)` must state its coverage, the
    share of policies with a GBP premium. *Reverse:* load the rates and rerun, or switch
    to the policy start month in `policy_gbp` (`src/gold/build_facts.py`).

28\. **Funnel definitions (step 12).** First-touch campaign: the surviving lead is the
    earliest in its cluster, so its campaign. `last_call_agent_key`: the last call on or
    before the application date, else the last call. Never called: null call keys and
    timings, zero counts, SLA flag false. `quoted_flag` and `application_flag` also count
    a linked policy. `days_lead_to_policy` runs to the application date. *Reverse:*
    `lead_funnel` in `src/gold/build_lead_funnel.py`.
