# KPI decision log: funnel conversion rate

Status: **final** (option C, confirmed by the human). This log is the reasoning behind
`ASSUMPTIONS.md` entry 13; the two must agree. The final wording of entry 13,
`KPI_DEFINITIONS.md` and the recommendation to both directors is written by the human.

## What the disagreement actually is

The Regional Marketing Director (UK and Ireland) and the Sales Operations Director
(International) define conversion on four independent axes. Only the first and part of the
second genuinely collide. Marketing measures acquisition efficiency by spend cohort. Sales
Ops measures agent effectiveness on workable opportunities, by completion month.

| Axis | Marketing Director | Sales Ops Director | Real conflict? | Handling |
| --- | --- | --- | --- | --- |
| Denominator | All leads captured | Connected calls | Yes: lead grain vs call grain | Two separately named measures |
| Population | All leads, no dedup, no consent filter | Exclude duplicates; exclude leads created outside regional business hours | Partly: dedup is a data-quality fact; business hours is an Ops-only scope | Dedup everywhere (the brief's measure 2 requires it). Business hours is a flag on `fact_lead_funnel`, used only by the Ops measure |
| Attribution | Campaign on the lead at creation (first touch) | Agent on the last call before the application date | No: different dimensions | Both keys on `fact_lead_funnel` (`campaign_key`, `last_call_agent_key`) |
| Period | Month the lead was created | Month the policy started | No: two roles of one date | Role-playing date dimension; `USERELATIONSHIP` for the policy-start role |

A call-grain ratio counts a lead once per connected call, so it is a productivity ratio,
not a share of opportunities. Forcing either grain on the other director would silently
change what the number means, which the BI Manager's note forbids.

## Indicative size of the gap (early pandas pass, Q2 2026, all regions; superseded by `KPI_RECONCILIATION.md`)

- Marketing-style, raw leads: 9.9% (4,766 linked policies / 47,987 leads).
- Marketing-style, deduplicated leads: 10.1% (757 duplicate leads removed, 1.6%).
- Ops-style (policies started in Q2 / connected calls): 20.5%. Not reproduced: the built
  measure gives 18.56% (see `KPI_RECONCILIATION.md` for the variants tried).
- Leads created inside weekday business hours: about 38%; the Ops filter removes about 62%.
- April-created leads whose policy starts in April: 470 of 1,492.

## Options considered

| Option | Verdict |
| --- | --- |
| A. Adopt Marketing's definition for everyone | Rejected: overrides Ops on a metric tied to commission and coaching |
| B. Adopt Ops's definition for everyone | Rejected: dilutes channel accountability; breaks the brief's `Leads (Deduplicated)` base |
| C. Two certified, explicitly named measures at their own grains, plus a published reconciliation | **Chosen** (confirmed by the human). Allowed by the BI Manager's note |
| D. One measure with a slicer switching definitions | Rejected: two answers behind one name |

## Decision (option C)

- `Lead to Policy Conversion Rate`: lead grain. Policies linked to a surviving lead, over
  `Leads (Deduplicated)`; anchored on the lead-created date; first-touch campaign. The
  funnel headline. It shares its denominator with `Cost Per Lead (GBP)`.
- `Connected Call to Policy Conversion Rate`: call grain. Connected dispositions
  (CONNECTED, QUOTED, APPLICATION_STARTED, SOLD; assumption 2); non-duplicate leads created
  in business hours (weekdays, region's own timezone; assumption 9); anchored on the
  policy-start date; last-call agent attribution.
- A reconciliation view stepping from one number to the other, one axis at a time:
  `KPI_RECONCILIATION.md` and `powerbi/kpi_reconciliation.csv` (step 18, built).
- Step 18 confirmed with the human: the business-hours scope applies to both the policies
  and the connected calls (same leads on both sides), and the date axis is policy start for
  policies and call date for calls under one date slicer. Q2 2026 result: 1,324 policies /
  7,135 connected calls = 18.56% (all dates: 1,760 / 7,329 = 24.01%).
- Both measures exclude NTU policies (assumption 3) and policies with a NULL `lead_key`
  (assumption 11).
- BI Manager rules: one certified measure per KPI; both grains named and documented; the
  words "Conversion Rate" never appear on their own in a measure name.

## Policies counted, not converted leads

`Lead to Policy Conversion Rate` counts policies linked to a surviving lead, as the brief
says: 4,499 lead-attributed policies that are not NTU, over 47,201 surviving leads (9.53%).
The legacy port's `ConvertedLeads` counts leads with at least one such policy, also
4,499. They agree today only because no surviving lead has more than one policy (4,767
linked policies on 4,767 distinct leads; a test asserts it). A lead that bought two
policies would count twice in the measure and once in the port, and the measure could in
principle pass 100% for a small slice.

## How to reverse it

Every axis is a column or relationship in the model, not baked into the pipeline.
Changing a definition means editing one DAX measure, its description, its row in
`KPI_DEFINITIONS.md`, and the assumptions log entry. No data rebuild.

## Inputs that affect both numbers

Defect register references (27 defects in total; see `DEFECT_REGISTER.md` for status):

| Item | Where | Status |
| --- | --- | --- |
| Lead dedup (numerator and denominator) | Defects 2, 3, 4 | Decided |
| Policy duplicates, conflicting fields | Defect 19 | Decide at step 9 |
| Policies with no lead | Defect 20; assumption 11 | Decided: NULL `lead_key`, excluded |
| DECLINED yet INFORCE policies | Defect 22 | Finding; counted as their `policy_status` says until PolicyCore confirms |
| NTU, CANCELLED, LAPSED | Assumption 3 | Decided: exclude NTU only. Also excluding CANCELLED and LAPSED: 9.53% to 8.11% overall, 0.82 to 1.64 points by region, ranking unchanged (`conversion_by_status_rule`) |
| `attribution_window_days` | Assumption 10 | Decide at step 12 |
| "International" = CA, AU, US | Assumption 1 | Decided |
| Call-file timezones (business-hours flag, first call) | Defect 15; assumption 5 | Decide at step 8 |

## Earlier draft of `ASSUMPTIONS.md` entry 13 (AI-drafted, kept for reference)

Entry 13 in `ASSUMPTIONS.md` is now a placeholder for the human's own wording. The earlier
AI draft, unchanged:

13\. **Funnel conversion rate (DRAFT, to be rewritten by Jason).** *Ambiguity:* the two
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
