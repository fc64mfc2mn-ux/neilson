# Assumptions

Each entry gives the ambiguity, the options, the choice and reason, and how to reverse it.
The evidence and queries for each are in `DEFECT_REGISTER.md` ("Assumption detail"), and
for 17, 25 and 26 in `DATA_CLASSIFICATION.md`, under the same number.

## Assumptions

1\. **Regions.** "UK and Ireland" vs "International", but only UK, CA, AU, US exist. Options: map by title or report Ireland missing. Choice: UK = UK and Ireland; CA, AU, US = International. Reverse: `dim_region.region_group`.

2\. **Connected call.** Options: CONNECTED only, or with later stages. Choice: CONNECTED, QUOTED, APPLICATION_STARTED, SOLD; a quote implies contact. Reverse: the disposition list in silver calls.

3\. **Policy status.** Options: all; exclude NTU; also CANCELLED and LAPSED. Choice: exclude NTU only; a cancelled policy was still sold. Reverse: the measure filter.

4\. **Channel source.** Lead vs campaign channel. Choice: campaign channel, as spend is by campaign. Reverse: gold `channel_key` source.

5\. **Call timezones.** Options: UTC, local, fixed offset. Choice: UK, CA, US as UTC; AU from Australia/Sydney, the only rule that matches all policy dates. Reverse: `calls.source_timezone`.

6\. **Minor-unit spend.** Options: keep, divide by 100, quarantine. Choice: divide rows 20x or more their currency's CPM by 100 when that lands in band, else quarantine. Reverse: `spend.minor_units`.

7\. **No CAD June rate.** Options: carry May, average, quarantine. Choice: quarantine; a carried rate invents Treasury data. Reverse: load the rate, rerun.

8\. **SCD2 dates.** The roster has none. Choice: versions in file order, the later from 2026-04-01. Reverse: HR effective dates.

9\. **Business hours.** Choice: Monday to Friday, region timezone, no holidays. Reverse: flag logic.

10\. **Attribution window.** Choice: not applied (3.4% of policies affected). Reverse: one measure filter.

11\. **Policies with no lead.** Options: drop, `UNKNOWN`, NULL key. Choice: NULL key, excluded from lead measures. Reverse: a filter.

12\. **Fiscal quarter.** Choice: calendar quarters; Q2 2026. Reverse: fiscal columns in `dim_date`.

13. **Two conversion definitions, both kept (option C).**
The Regional Marketing Director (UK and Ireland) counts policies over all leads captured,
by the month the lead was created and its first-touch campaign: a measure of acquisition
efficiency. The Sales Operations Director (International) counts policies over connected
calls, excluding duplicate leads and leads created outside business hours, by the month the
policy started and the last-call agent: a measure of agent effectiveness. These are
different questions at different grains, so I did not pick one. The model has two certified
measures, `Lead to Policy Conversion Rate` and `Connected Call to Policy Conversion Rate`,
and nothing is called plain "Conversion Rate". For Q2 2026 they give 9.53% and 18.56%.
`docs/KPI_RECONCILIATION.md` walks from one to the other in six steps with no unexplained
difference, and `docs/KPI_DECISION_LOG.md` records the options rejected and why.
Reversing the decision means editing one measure, not rebuilding data.

14\. **YAML parser.** Choice: add pinned `pyyaml`. Reverse: JSON config.

15\. **Bronze reruns.** Choice: append a batch only when a file's fingerprint changes. Reverse: clear `bronze.ingest_log`.

16\. **Agents with no region.** Choice: team prefix (UK). Reverse: `conform_agents`.

17\. **RLS source.** Choice: `gold.bridge_user_region` for Power BI and Unity Catalog. Reverse: edit the bridge.

18\. **Implausible DOB.** Choice: null when age is under 18 or over 100; keep the lead. Reverse: `MIN_AGE`, `MAX_AGE`.

19\. **Dedup window.** Choice: chained 48-hour gaps across email and phone digits. Reverse: `dedupe_leads.py`.

20\. **Calls on unknown leads.** Choice: keep with a NULL `lead_key` and a match status. Reverse: quarantine in silver.

21\. **Negative, zero, click-less spend.** Choice: quarantine negatives; keep zeros and null clicks, flagged. Reverse: `spend.py`.

22\. **Spend outside campaign dates.** Choice: counted and flagged. Reverse: a measure filter.

23\. **Conflicting policy copies.** Choice: merge, earliest copy wins. Reverse: `merge_conflicting`.

24\. **Key width.** Choice: 52-bit hash keys, exact in Power BI. Reverse: `KEY_HEX_DIGITS`.

25\. **PII generalisation.** Choice: outward postcode (AU first 3 digits), E.164 phone hash, decade age band. Reverse: `apply_pii_policy.py`.

26\. **Consent.** Choice: latest consent event, else the capture flag labelled `lead_capture`. Reverse: null consent for `lead_capture`.

27\. **Premium FX month.** Choice: application month; no rate invented. Reverse: `policy_gbp`.

28\. **Funnel rules.** Choice: first-touch campaign; last call on or before application. Reverse: `build_lead_funnel.py`.
