# Assumptions

## Assumptions

Entries marked **pending evidence** have a working choice that a pipeline measurement will
confirm or overturn. The query and the result will be recorded against the entry.

1. **Region groupings.** *Ambiguity:* the director titles say "UK and Ireland" and
   "International"; `ref_region` holds only UK, CA, AU and US. *Options:* map by title;
   report Ireland as missing. *Choice:* UK and Ireland = UK; International = CA, AU, US.
   *Reason:* these are the only regions in the data. *Reverse:* edit the region-group
   column in `gold.dim_region`.

2. **Connected calls.** *Ambiguity:* which dispositions mean a conversation. *Options:*
   CONNECTED only; CONNECTED plus later stages. *Choice:* CONNECTED, QUOTED,
   APPLICATION_STARTED, SOLD. *Reason:* matches the brief's `Contact Rate`; a quote or
   sale implies contact. *Reverse:* change the disposition list behind the connected flag
   in silver calls.

3. **Policy status.** *Ambiguity:* which policies count as converted. *Options:* all;
   exclude NTU; also exclude CANCELLED and LAPSED. *Choice:* exclude NTU only. *Reason:*
   the legacy procedure excludes NTU, and a cancelled policy was still sold. The stricter
   rule's effect will be measured and reported. *Reverse:* change the status filter in the
   policy measures.

4. **Channel source of truth.** *Ambiguity:* lead and campaign channel can disagree.
   *Options:* lead channel; campaign channel. *Choice:* campaign channel, after
   normalising labels to `ref_channel`. *Reason:* spend is recorded by campaign; after
   normalisation only 600 of 47,687 leads disagree, scattered across all channel pairs
   (`findings_reference.sql`). *Reverse:* switch the `channel_key` source in gold.

5. **Call-file timezones.** *Ambiguity:* neither call file states its timezone.
   *Options:* UTC; region local time (DST-aware); a fixed offset. *Choice:* UK, CA and
   US as UTC; AU converted from `Australia/Sydney`. *Reason:* a policy's application
   date equals its linked call's date for every UK, CA and US policy as stored, and for
   all 426 AU policies only after the conversion (248 before); AU lead timestamps show
   no shift. After conversion AU first calls still come a median 512 min after the lead
   (22 to 45 elsewhere): flagged as an open observation, not corrected. *Reverse:*
   `calls.source_timezone` in config. Queries: `src/sql/findings_calls.sql`.

6. **Minor-unit spend.** *Ambiguity:* some spend sits about 100x too high. *Options:*
   keep; divide by 100; quarantine. *Evidence:* two whole campaigns, CMP-1061 (AUD) and
   CMP-1062 (GBP), have median CPM 92x and 99.8x their currency's; every other campaign
   is 0.88 to 1.05x. Their CPC is 100x too, but click-through is normal, so spend is the
   wrong field. *Rule:* a row with CPM 20x or more its currency median is divided by 100
   if the result is 0.5 to 2x; otherwise quarantined as ambiguous. 181 rescaled, none
   ambiguous (normal rows reach 1.51x, minor rows start at 68x). Blended CPL: £319.95
   before, £34.39 after. *Reverse:* `spend.minor_units` in config.

7. **Missing CAD rate for June.** *Options:* carry May forward; average the months;
   quarantine. *Choice:* quarantine CAD June spend at error severity, so the DQ gate
   fails. *Reason:* carrying May forward would invent a Treasury rate, and the brief
   forbids silent nulls. *Reverse:* load the rate once Treasury supplies it, then rerun.

8. **SCD2 effective dates.** *Ambiguity:* some agents appear twice with a different team
   or seniority and no dates. *Options:* first-seen load date; file row order; current
   state only. *Choice (proxy):* versions in bronze `_source_row` order. One version:
   valid 2026-01-01 to 9999-12-31. Two: the earlier 2026-01-01 to 2026-03-31, the later
   current from 2026-04-01 (reporting start); `valid_to` inclusive. A call takes the
   version valid on its UTC date. Three or more versions stop the stage. The later
   AG0008 and AG0037 rows name UK teams; those team values are quarantined and the
   earlier team kept (defect 27). *Reason:* the source has no dates, so the proxy is
   stated, not hidden. *Reverse:* replace the dates with HR effective dates.

9. **Business hours.** *Ambiguity:* Ops excludes leads "outside regional business
   hours" without defining weekends or holidays. *Options:* `ref_region` hours every day;
   weekdays only; weekdays minus holidays. *Choice:* Monday to Friday, start inclusive
   and end exclusive, in the region's own timezone. No holiday calendar. *Reason:* the Ops
   note names overnight and weekend; the pack has no holiday data. *Reverse:* change the
   flag logic or add a holiday table.

10. **Attribution window (pending evidence).** *Ambiguity:* `ref_channel` gives
    `attribution_window_days` (0 to 30) but nothing says whether it limits which policies
    count. *Options:* ignore it; count only policies within the window. *Choice:* none
    yet; settled at step 12, when the funnel fact is built. *Evidence needed:* how many
    linked policies fall outside it. *Reverse:* one filter in the lead-attributed policy
    measures.

11. **Policies with no lead.** *Options:* drop; keep against an `UNKNOWN` lead; keep with
    a NULL key. *Choice:* keep them in `gold.fact_policy` with a NULL `lead_key`, and
    exclude them from lead-attributed measures. Power BI shows them as blank. *Reason:*
    they are real sales but cannot be attributed. `gold.dim_lead` is one row per
    deduplicated lead, so an `UNKNOWN` member would add a lead that does not exist.
    *Reverse:* a measure filter.

12. **Fiscal quarter.** *Ambiguity:* the pack gives no fiscal calendar. *Choice:*
    calendar quarters, so the reporting period is Q2 2026. *Reverse:* add fiscal columns
    to `gold.dim_date`.

13. **Funnel conversion rate (DRAFT, to be rewritten by Jason).** *Ambiguity:* the two
    directors define conversion on four axes. Only the denominator truly collides:
    Marketing uses all leads (lead grain), Ops uses connected calls (call grain).
    Population, attribution and period are different scopes or date roles, not
    conflicts. *Options:* A, Marketing's definition for all; B, Ops's for all; C, two
    certified, explicitly named measures plus a reconciliation; D, one measure with a
    definition slicer. *Choice:* C. `Lead to Policy Conversion Rate` (lead grain,
    lead-created date, first-touch campaign) and `Connected Call to Policy Conversion
    Rate` (call grain, business-hours leads, policy-start date, last-call agent). A
    reconciliation view steps between the two one axis at a time. No measure is called
    plain "Conversion Rate". *Reason:* forcing either grain on the other changes what the
    number means. A and B override one director, and D puts two answers behind one name.
    The BI Manager's note allows named variants. *Reverse:* every axis is a model column
    or relationship, so a change is one DAX measure, its description and its
    `KPI_DEFINITIONS.md` row. No data rebuild.

14. **Dependency addition: PyYAML.** *Ambiguity:* the stack is fixed, but the brief requires
    a YAML config, and neither the starter `requirements.txt` nor PySpark includes a YAML
    parser. *Options:* add a parser; write one by hand; switch the config to JSON.
    *Choice:* add `pyyaml==6.0.2`, pinned, used only by `src/common/config.py`. *Reason:*
    the brief names `config/config.yaml`; a hand-written parser would be fragile.
    *Reverse:* remove the pin and load a JSON config instead.

15. **Bronze reruns.** *Ambiguity:* bronze must be append-only, but a second run of the one
    command must not double every row. *Options:* drop and reload each run; append
    blindly; append only changed files. *Choice:* each landing is a batch with one
    `_ingest_ts`; its content fingerprint goes in `bronze.ingest_log`. An unchanged file
    appends nothing, a changed file appends a new batch, and silver reads the latest
    batch per file. *Reason:* keeps bronze append-only and auditable, and makes reruns
    idempotent. Bronze also stores `_source_row`, the record's position in its file
    (from 1, header excluded), outside `_row_hash`. *Reverse:* delete the file's
    `bronze.ingest_log` rows to force a reload.

16. **Agents with no region.** *Ambiguity:* AG0007 and AG0035 have no roster region.
    *Options:* leave `UNKNOWN`; use the team prefix. *Choice:* team prefix (UK). *Reason:*
    both take calls only in the UK, and the prefix matches the region on 41 of 43 rows
    (`findings_reference.sql`). *Reverse:* drop the fallback in `conform_agents`.

17. **Row-level security source.** *Ambiguity:* where region entitlements live. *Options:*
    per-report role rules; a wide denormalised table; one bridge. *Choice:*
    `gold.bridge_user_region` drives both the Power BI role and the Unity Catalog row
    filter, matched on `region_key`; `ALL` is unrestricted, unmapped users see nothing.
    *Reason:* one source, star schema kept. *Reverse:* edit the bridge.

18. **Implausible dates of birth.** *Ambiguity:* the register flags about 312 DOBs.
    *Options:* quarantine the lead; null the DOB. *Choice:* null a DOB when the whole-year
    age at lead creation is under 18 or over 100; keep the lead. That nulls 60: before
    1920, 20 of 20; 2008 to today, 20 of 272 (the other 252 were born in 2008 and are
    18); future, 20 of 20 (`dob_register_bands`). *Reason:* the leads are otherwise
    normal. *Reverse:* `MIN_AGE`/`MAX_AGE` in `src/silver/leads.py`.

19. **Dedup detail.** *Ambiguity:* "within 48 hours" of the previous match or of the
    first lead; phone formats vary. *Choice:* chained gaps, linked across email and phone;
    phone compared on digits only. *Reason:* gives 758 duplicates and 47,201 survivors.
    *Reverse:* `src/silver/dedupe_leads.py`.

20. **Calls on unknown leads (defect 17).** *Ambiguity:* 1,100 calls name a lead id found
    in no lead file. *Options:* quarantine; keep against an `UNKNOWN` lead. *Choice:*
    keep, flag `call_lead_not_found`, map to `UNKNOWN` in gold; the same for 45 calls
    on leads quarantined at step 7, and for AG9001 to AG9004 (`UNKNOWN` agent).
    *Reason:* all 1,099 ids follow `LD-GHOST-<region>-<n>`, none matches a quarantined,
    PK-conflict or out-of-period lead, and no policy links to them. The calls are real
    agent work for handle time and grade of service; they cannot enter the lead-grain
    funnel either way. *Reverse:* quarantine the flagged rows in `src/silver/calls.py`.

21. **Negative, zero and click-less spend (defect 10).** *Negatives (3):* none offsets a
    positive of its campaign, each is that day's only row at under 0.3x the median:
    quarantined, not netted. *Zeros (200):* kept; 91 are ORGANIC (no media cost), the
    rest have tiny delivery; the 109 on paid channels are flagged. *Null clicks (17):*
    kept as null, never 0, flagged; spend and impressions are present. *Reverse:*
    `src/silver/spend.py`.

22. **Spend outside campaign dates (defect 9).** *Choice:* the spend counts and is
    flagged (325 rows), because the media cost was incurred whatever the plan said; two
    campaigns end before they start. *Reverse:* a measure filter on
    `outside_campaign_dates`.

23. **Conflicting policy copies (defect 19).** *Choice:* merge; the earliest copy wins a
    value conflict, a null is filled from the other copy. *Reason:* the earliest matches
    the linked call's agent (1 of 1) and monthly x 12 (2 of 3; the later copy 0 of 3).
    *Reverse:* `merge_conflicting` in `src/silver/policy.py`.

24. **Surrogate key width.** *Choice:* the first 52 bits of SHA-256 of the natural key,
    below 2^53 so Power BI's double-precision numbers hold every key exactly (60 bits did
    not); deterministic, with a collision test per dimension. *Reverse:* `KEY_HEX_DIGITS`
    in `src/gold/build_dimensions.py`.

25. **Lead PII generalisation (step 11).** *Postcode, by region, from the formats seen:*
    UK `A9 9AA`, `AA9 9AA`, `AA99 9AA` keep the outward code (before the space); CA
    `A9A 9A9` keeps the forward sortation area (3 characters); US `99999` keeps ZIP3; AU
    `9999` has no outward part and the full code would be the raw value, so the first 3
    digits. A postcode not in its region's format is withheld, not guessed (defect 29).
    *Phone:* E.164 from the digits (UK, AU, NANP national and international forms), so
    one number written four ways hashes the same. *Age band:* decades of whole-year age at
    lead creation, `18-19` first, `90+` last, `UNKNOWN` for a null DOB. *Consent:* the
    latest event in `silver.consent_current`; a lead with no event keeps its capture flag,
    labelled `lead_capture`. *Reverse:* `POSTCODE_RULES`, `phone_e164`, `age_band` in
    `src/gold/apply_pii_policy.py`.

26. **Consent labelling.** *Choice:* `gold.dim_lead.consent_marketing_current` takes the
    latest consent event; with no event, the capture-time flag, and
    `consent_marketing_source` says which (`consent_events` or `lead_capture`). The AU
    postcode rule is in assumption 25. *Reason:* the capture flag is the only evidence for
    leads with no events, but it is weaker (defect 25 shows it can be stale), so measures
    that use consent filter on the source. *Reverse:* null consent for `lead_capture`.
