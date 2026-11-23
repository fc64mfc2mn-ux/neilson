# Source system owners

## Source system owners table

The six feeds and their owning roles, as named in the brief. Questions come from the defect
register and `docs/OPEN_QUESTIONS.md` (letters in the last column).

| Source system | What it feeds | Owner | Question I would ask | What must be agreed before go-live | What breaks without that conversation | Open question |
| --- | --- | --- | --- | --- | --- | --- |
| AdHub | `dim_campaign`, `fact_marketing_spend_daily`: campaigns, daily spend, impressions, clicks | Marketing Operations Manager | Are CMP-1061 (AUD) and CMP-1062 (GBP) recorded in minor units (cents, pence)? Their cost per thousand impressions is 92 and 99.8 times their currency median | The currency unit of `spend_local`, and a rule for spend outside a campaign's dates | Cost per lead for those campaigns is overstated 100 times (blended 9.3 times) if the rescale rule is wrong or stops matching | none (finding 3) |
| LeadFlow CRM | `dim_lead`, `fact_lead`, `fact_lead_funnel`, `consent_current`: leads and consent events | Digital Acquisition Lead, with the Data Protection Officer for consent | Why do 284 leads with a withdrawn consent event still show `consent_marketing = Y`? Which is right for 24 postcodes in the wrong format for their region? | The consent event log as the source of truth; the postcode generalisation rules; how a duplicate lead is identified | Marketing lists include people who withdrew consent; leads land in the wrong region | g, i |
| Aurora Dialler | `fact_call`, call side of `fact_lead_funnel`: calls, dispositions, QA scores | Contact Centre Systems Manager | What timezone are AU call times in? After the Sydney conversion the AU first call is still a median 8.5 hours after the lead | The timezone of each call file; what a DO_NOT_CALL disposition must suppress | Time to first call is wrong for AU; calls after a DO_NOT_CALL go unchecked (6,311 calls) | d |
| PolicyCore | `fact_policy`: policies, premiums, underwriting | Finance Systems Manager | For 265 DECLINED yet INFORCE policies, which field is right? Is an annual premium above 12 times the monthly (187 policies) a loading, a fee or an error? | Which policy statuses count as a sale (assumption 3: exclude NTU only); how duplicate copies are resolved | Conversion and premium measures count the wrong policies | a, b |
| Workforce extract | `dim_agent` (SCD2), `dim_agent_seniority`: roster, teams, seniority | Operations Resource Planning Manager | When did each team or seniority change take effect? The roster has versions with no dates. What are the weekend and holiday rules for business hours? | Effective dates for agent changes; the business-hours rule per region | Calls are credited to the wrong team or seniority band; the business-hours flag misclassifies leads | c |
| Finance Treasury | `silver.fx_rate`, GBP values in spend and policies: monthly FX rates | Finance Treasury (the brief names no role) | Can you supply the CAD rate for June 2026 and July rates for every currency? | A rate for every currency and month before a period is reported; no rate carried forward | 180 CAD June spend rows are quarantined and 675 policies have no GBP premium, so June CPL and average premium are understated or incomplete | e |

Open questions f (is Ireland inside UK?) and h (is the attribution window acceptable?) are
for the Regional Marketing Director, as the business owner of the marketing measures, not
for a source system. The reference files (`ref_channel`, `ref_region`, `sec_user_region`)
have no owner in the brief; `sec_user_region` drives row-level security, so its owner needs
naming before go-live.
