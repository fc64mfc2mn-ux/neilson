# KPI definitions

## KPI definitions table

All 16 measures in `powerbi/measures.dax` (the `Key Measures` table), taken from the DAX and
its comment blocks. Tier: core and stretch are the brief's measures; helpers are not in the
brief but are used by core measures or visuals. Every measure is enforced in the Power BI
model (`powerbi/measures.dax`); the rules it relies on (deduplication, FX, connected flag,
NTU, business hours) are applied upstream in the gold tables named. Policy status rule:
exclude NTU only (assumption 3). Owning role is proposed, from the brief's stakeholders.

| Measure | Tier | Business definition | Numerator | Denominator | Grain | Filters applied | Period anchor | Attribution basis | Owning role | Where enforced | Format |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Marketing Spend (GBP)` | core 1 | Spend converted to GBP at its month's rate | Sum of `fact_marketing_spend_daily[spend_gbp]` | n/a | Spend day x campaign | Quarantined rows excluded upstream (copies, negatives, CAD June) | Spend date | Campaign, channel, region on the spend row | Regional Marketing Director | DAX; FX in silver spend | `\£#,0.00` |
| `Leads (Deduplicated)` | core 2 | Distinct surviving leads | Distinct `fact_lead_funnel[lead_key]` | n/a | Surviving lead | Dedup: same email or phone within 48 h, earliest survives | Lead-created date | First-touch campaign | Regional Marketing Director | DAX; dedup in silver | `#,0` |
| `Cost Per Lead (GBP)` | core 3 | Spend per surviving lead | Marketing Spend (GBP) | Leads (Deduplicated) | Mixed: spend day / lead | Blank with no leads or no spend | Spend date / lead-created date | Campaign | Regional Marketing Director | DAX | `\£#,0.00` |
| `Contact Rate` | stretch 4 | Share of leads with a connected call | Leads with `connected_call_count > 0` | Leads (Deduplicated) | Surviving lead | Connected = CONNECTED, QUOTED, APPLICATION_STARTED, SOLD | Lead-created date | First-touch campaign | Sales Operations Director | DAX; flag in `fact_lead_funnel` | `0.0%` |
| `Lead to Policy Conversion Rate` | core 5 | Policies per surviving lead (acquisition) | Policies Linked to Surviving Leads | Leads (Deduplicated) | Lead | Not NTU; policies without a lead excluded | Lead-created date | First-touch campaign | Regional Marketing Director | DAX | `0.00%` |
| `Connected Call to Policy Conversion Rate` | core (step 18) | Policies per connected call (agent productivity) | Lead-linked policies, not NTU, of business-hours leads | Connected calls on the same leads | Call | Business-hours leads only, on both sides; not NTU | Policy-start date (policies), call date (calls) | Last-call agent | Sales Operations Director | DAX; business-hours flag in `fact_lead_funnel` | `0.00%` |
| `Average Annual Premium (GBP)` | stretch 6 | Mean annual premium in GBP | Sum of `fact_policy[annual_premium_gbp]` | Policies with a GBP premium | Policy | Not NTU; no-rate policies excluded (coverage 86.8%) | Policy-start date | Selling agent, product | Sales Operations Director | DAX; FX in gold | `\£#,0.00` |
| `Average Annual Premium (GBP) Coverage %` | helper | Share of counted policies with a GBP premium | Policies with a GBP premium | Policies counted (not NTU) | Policy | Not NTU | Policy-start date | n/a | BI Manager | DAX | `0.0%` |
| `Average Handle Time (Seconds)` | core 7 | Talk plus hold time per connected call | Talk + hold seconds, connected calls | Connected calls | Call | Connected calls only, both sides | Call date | Call's agent | Sales Operations Director | DAX; `is_connected` in silver calls | `#,0` |
| `Grade of Service` | stretch 8 | Inbound calls answered within the SLA | Inbound calls with `answered_within_sla_flag` | Inbound calls | Call | Inbound only; each region's own target | Call date | Call's agent | Sales Operations Director | DAX; SLA flag in gold | `0.0%` |
| `Time to First Call (Median Minutes)` | core 9 | Median minutes from lead creation to first call | Median of `minutes_to_first_call` | n/a | Surviving lead | Never-called leads excluded; AU residual gap flagged, not corrected | Lead-created date | First-call agent | Sales Operations Director | DAX; minutes in `fact_lead_funnel` | `#,0.0` |
| `Cost Per Policy (GBP)` | stretch 10 | Spend per lead-linked policy | Marketing Spend (GBP) | Policies Linked to Surviving Leads | Mixed: spend day / policy | Not NTU; attribution window not applied (assumption 10) | Spend date / lead-created date | Campaign | Regional Marketing Director | DAX | `\£#,0.00` |
| `Cost Per Lead (GBP) MoM %` | stretch | Month-over-month change in CPL | CPL minus previous month's CPL | Previous month's CPL | Month | One month selected; blank for the first month | Marked date table (`DATEADD`) | Campaign | Regional Marketing Director | DAX | `+0.0%;-0.0%;0.0%` |
| `CPL vs Target Variance (GBP)` | stretch | CPL minus the selected target | Cost Per Lead (GBP) − selected `Target CPL` | n/a | Selection | Blank with no single target selected | As CPL | Campaign | Regional Marketing Director | DAX; `Target CPL` table (£20 to £120) | `+\£#,0.00;-\£#,0.00;\£0.00` |
| `Policies Linked to Surviving Leads` | helper | Lead-linked policies, not NTU | Count of `fact_policy` rows | n/a | Policy | `is_lead_attributed`; not NTU | Lead-created date | Selling agent | BI Manager | DAX | `#,0` |
| `Agents in Group` | helper | Distinct agents behind a selection (not a KPI) | Distinct `dim_agent[agent_id]` through `fact_call` | n/a | Agent | Used as a visual filter `>= 3` on the seniority visual | Call date | n/a | BI Manager | DAX; visual-level filter | `#,0` |

## Recommendation to the Regional Marketing Director and the Sales Operations Director

> **Draft for Jason to edit.**

Both definitions stay, as two separately certified, explicitly named measures. Nothing is
called plain "Conversion Rate".

- `Lead to Policy Conversion Rate` is the Regional Marketing Director's view: policies linked
  to a surviving lead over deduplicated leads, by lead-created date and first-touch campaign.
  It measures acquisition efficiency and shares its denominator with `Cost Per Lead (GBP)`.
- `Connected Call to Policy Conversion Rate` is the Sales Operations Director's view: policies
  of business-hours leads over connected calls on those leads, by policy-start date and the
  last-call agent. It measures agent productivity.

For Q2 2026 they give 9.53% and 18.56%. The difference is not an error: the two count
different things at different grains. `docs/KPI_RECONCILIATION.md` walks from one to the other
in six steps that add up exactly, and `docs/KPI_DECISION_LOG.md` records the options
rejected and why. Either definition can be changed later by editing one measure, without
rebuilding data.
