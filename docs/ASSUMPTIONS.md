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

5. Call-file timezones (evidence recorded; AU unresolved).
   Ambiguity: neither call file states its timezone.
   Options: treat as UTC; convert from the region's local zone (DST-aware); fixed offset.
   Choice: UK, CA and US call timestamps are treated as UTC. Median first-call gap after
   lead creation is 22 min (UK), 44 min (CA) and 40 min (US) as UTC, but about 280 min for
   CA and US if read as local time. AU is unexplained: 18.5 h as UTC, 8.4 h as Sydney local
   time, against about 22 min expected. AU timestamps are kept as UTC, flagged in the data
   quality output, and not corrected.
   Reason: only the UK, CA and US hypotheses are supported by the data. A Sydney conversion
   would look fixed but leaves an 8 h error.
   Reverse: change the per-region timezone in config and rerun. Next test: check whether AU
   lead timestamps are the shifted side.

6. **Minor-unit spend (pending evidence).** *Ambiguity:* some GBP and AUD spend rows sit
   far above their campaign's median, consistent with pence or cents. *Options:* keep;
   divide by 100; quarantine. *Choice:* divide by 100 where the ratio to the median
   clearly fits that rule; quarantine the ambiguous rows. *Reason:* left as they are,
   they distort CPL badly. *Evidence needed:* the ratio distribution, to set the
   threshold. *Reverse:* change or disable the rule; every affected row is logged in
   `silver.dq_exception`.

7. **Missing CAD rate for June.** *Options:* carry May forward; average the months;
   quarantine. *Choice:* quarantine CAD June spend at error severity, so the DQ gate
   fails. *Reason:* carrying May forward would invent a Treasury rate, and the brief
   forbids silent nulls. *Reverse:* load the rate once Treasury supplies it, then rerun.

8. **SCD2 effective dates.** *Ambiguity:* some agents appear twice with a different team
   or seniority and no dates. *Options:* use the first-seen load date; use file row
   order; keep current state only. *Choice:* an approximation. The earlier file row (lower
   bronze `_source_row`) is valid from 2026-01-01 to 2026-03-31; the later row is current from 2026-04-01, the
   start of the reporting period. The later rows for AG0008 (CA) and AG0037 (AU) name UK
   teams, which do not match their region. Those team values are quarantined and the
   earlier team is kept (defect 27). *Reason:* the source has no dates, so the proxy is
   stated rather than hidden. *Reverse:* replace the dates with HR effective dates.

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
