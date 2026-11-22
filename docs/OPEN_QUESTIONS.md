# Open questions for data owners

For the submission email. One line each: the question, the owner, the evidence query
(named queries in `src/sql/`), the status. Add new questions as they come up.

| # | Question | Owner | Evidence | Status |
| --- | --- | --- | --- | --- |
| a | 187 policies have an annual premium above 12 x monthly (12.6 to 15x). Loading, fee, or error? Flagged, values unchanged. | Finance Systems Manager (PolicyCore) | `findings_policy.sql: premium_above_12x_monthly` | Open |
| b | 265 policies are underwriting DECLINED yet status INFORCE. Which field is right? | Finance Systems Manager (PolicyCore) | `findings_policy.sql: declined_yet_inforce` | Open |
| c | `ref_region` gives daily business hours but no weekend or holiday rule. We assume Monday to Friday, no holidays (assumption 9). What is the rule? | Operations Resource Planning Manager | `findings_reference.sql: ref_region_business_hours` | Open |
| d | After the Sydney conversion, the AU first call still comes a median 8.5 h after the lead (22 to 45 min elsewhere). A further offset, or does AU work leads later? | Contact Centre Systems Manager (Aurora Dialler) | `findings_calls.sql: first_call_gap_in_silver` | Open |
| e | No CAD rate for June 2026 (180 spend rows quarantined) and no July rate for any currency, so 675 policies have no GBP premium (517 CAD June, 158 July). Can Treasury supply the rates? | Finance Treasury | `findings_spend.sql: spend_row_reconciliation`; `findings_policy.sql: policy_gbp_missing_by_cause` | Open |
| f | The Marketing brief covers UK and Ireland, but no source holds an Ireland region. Is Ireland inside UK, or missing? | Regional Marketing Director (UK and Ireland) | `findings_reference.sql: region_codes_by_source` | Open |
| g | 24 leads have a postcode in the wrong format for their region (defect 29): 11 UK with an AU-style code, 10 UK with a Canadian code, 3 US with four digits. Is the region or the postcode wrong? | Digital Acquisition Lead (LeadFlow CRM) | `findings_leads.sql: postcode_region_format` | Open |
| h | `attribution_window_days` is not applied to the core measures; 164 of 4,767 linked policies (3.4%) apply after the window. Is that acceptable to Marketing? | Regional Marketing Director (UK and Ireland) | `findings_policy.sql: policies_outside_attribution_window` | Open |
| i | For confirmation: AU postcodes are cut to their first 3 digits (assumption 25), and leads with no consent event carry their capture flag labelled `lead_capture` (assumption 26). | Digital Acquisition Lead, with the Data Protection Officer | `findings_leads.sql: postcode_region_format`, `withdrawn_consent_still_y` | Open |
