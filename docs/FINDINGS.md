# Findings

Three findings, each reproducible from a named query in `src/sql/` and checked by a slow
test on the real data.

## Finding 1: calls continue after a DO_NOT_CALL disposition

**Number.** 6,311 calls on 4,223 leads in `silver.call` start after that lead's first
DO_NOT_CALL disposition. It happens in all four regions. Before quarantine there were
6,427 distinct calls on 4,286 leads: UK and AU 4,461 on 2,897, and CA and US 1,966 on
1,389. The register named only UK and AU.

**Method.** `findings_calls.sql: calls_after_do_not_call` and `do_not_call_reconciliation`
(AU times converted from Sydney first; test `test_silver_calls.py::test_finding_calls_after_do_not_call`).
For each lead, the query finds the earliest DO_NOT_CALL call and counts the calls that
start after it. The gap of 116 calls between the two counts is explained in full: 38
flagged calls were quarantined themselves, and 78 lost their DO_NOT_CALL anchor to
quarantine. The calls carry `after_do_not_call` in `gold.fact_call`.

**Business action.** Contact Centre Operations should check with Compliance whether
these calls break the opt-out rules in each market, and make the dialler suppress a
number once it has a DO_NOT_CALL disposition. Until then, report the flag monthly.

## Finding 2: withdrawn consent is still shown as granted

**Number.** 299 leads have WITHDRAWN as their latest consent event. On 284 of them
(95%), the lead record still says `consent_marketing = Y`.

**Method.** `findings_leads.sql: withdrawn_consent_still_y`
(test `test_silver_leads.py::test_finding_withdrawn_consent`). The query compares the
latest event per lead in `silver.consent_current` with the lead file's flag. Gold takes
consent from the events, so `dim_lead` shows these leads as not consented, and its
`consent_marketing_source` column says where each value came from.

**Business action.** Any marketing list built from the CRM flag would contact people
who withdrew. The CRM owner should sync the flag from the consent events. The Data
Protection Officer should decide whether past contact needs review.

## Finding 3: spend recorded in minor units overstates cost per lead

**Number.** Two campaigns, CMP-1061 (AUD) and CMP-1062 (GBP), record spend in minor
units (cents and pence): 181 rows. Their cost per lead is overstated 100 times:
£9,691 against £96.91 for CMP-1061, and £3,161 against £31.61 for CMP-1062. Left
uncorrected, these two campaigns alone raise the blended cost per lead across all
campaigns 9.3 times, from £34.39 to £319.95.

**Method.** `findings_spend.sql: minor_unit_evidence`, `minor_unit_row_bands` and
`cpl_minor_unit_effect` (tests in `test_silver_spend.py`). Each campaign's median CPM is
compared with its currency's median. These two are 92x and 99.8x, and every other
campaign is between 0.88x and 1.05x. Click-through is normal for both, so the spend
field is wrong, not the delivery. Row by row, normal rows stay below 2x the median and
minor-unit rows are 68x or more, with nothing in between. Silver divides these rows by
100, logged as `spend_minor_units_rescaled` (assumption 6). The legacy procedure still
counts them unscaled (`LEGACY_PORT.md`).

**Business action.** Marketing Finance should fix the two campaigns at source and add a
currency-unit check to the spend feed. Any report built on the legacy procedure has been
overstating these campaigns' cost per lead 100 times.
