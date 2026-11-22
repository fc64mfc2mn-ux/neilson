# Open questions for data owners

For the submission email. One line each: the question, the owner, the evidence query
(named queries in `src/sql/`). Add new questions as they come up.

| # | Question | Owner | Evidence |
| --- | --- | --- | --- |
| a | 187 policies have a supplied annual premium above 12 x monthly (12.6 to 15x). Loading, fee, or error? Flagged, values unchanged. | Finance Systems Manager (PolicyCore) | `findings_policy.sql: premium_above_12x_monthly` |
| b | 265 policies are underwriting DECLINED yet status INFORCE. Which field is right? | Finance Systems Manager (PolicyCore) | `findings_policy.sql: declined_yet_inforce` |
| c | `ref_region` gives daily business hours but no weekend or holiday rule. We assume Monday to Friday, no holidays (assumption 9). What are the thresholds? | Operations Resource Planning Manager | `findings_reference.sql: ref_region_business_hours` |
| d | After converting AU call times from Sydney time, the AU first call still comes a median 8.5 h after the lead (22 to 45 min elsewhere). Is there a further offset, or does AU work leads later? | Contact Centre Systems Manager | `findings_calls.sql: first_call_gap_in_silver` |
| e | No CAD rate for June 2026, so 180 CAD spend rows are quarantined. Can Treasury supply it? | Finance Treasury | `findings_spend.sql: spend_row_reconciliation` |
| f | The Marketing brief covers UK and Ireland, but no source holds an Ireland region. Is Ireland inside UK, or missing? | Regional Marketing Director (UK and Ireland) | `findings_reference.sql: region_codes_by_source` |
| g | 24 leads have a postcode in the wrong format for their region (defect 29): 11 UK with a four-digit AU-style code, 10 UK with a Canadian code, 3 US with four digits. Is the region or the postcode wrong, and are US four-digit codes ZIPs that lost a leading zero? Outward code withheld meanwhile. | Digital Acquisition Lead (LeadFlow CRM) | `findings_leads.sql: postcode_region_format` |
