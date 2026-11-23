# DRAFT submission email (for Jason to edit; not part of the submission)

> **TODO (Jason):** fill in the hours and the repo URL, check the cut list against the
> README, and rewrite the email in your own words. Delete this file before you submit, or
> keep it out of the final commit.

---

**Subject:** Northgate Life funnel: take-home submission

Hello,

The repo is at **<private repo URL>**, and I have added you as a collaborator. I spent
about **<N> hours**. That covers the core scope: bronze, silver and gold on local Delta;
gold PII handling; the Power BI model with the `Regional_Analyst` RLS role and view-as
evidence; the six tests and the DQ gate in CI; and the documents. I also did some stretch
work: the legacy port with a parity test, SCD2 `dim_agent`, the Connected Call measure with
its reconciliation, and a walkthrough video. I cut the performance artefact, the Unity
Catalog grants script, the Asset Bundle beyond its stub, and the Power BI roles other than
`Regional_Analyst`. The README says why, in priority order. One command runs everything:
`python -m src.run_pipeline --config config/config.yaml` (Java 17).

**Questions for the data owners** (evidence queries in `docs/OPEN_QUESTIONS.md`):

1. **PolicyCore:** 187 policies have an annual premium of 12.6 to 15 times the monthly
   premium. Is that a loading, a fee, or an error?
2. **PolicyCore:** 265 policies are underwriting DECLINED yet INFORCE. Which field is right?
3. **Resource Planning:** what are the weekend and holiday rules for business hours? We
   assumed Monday to Friday with no holidays.
4. **Aurora Dialler:** after converting AU calls from Sydney time, AU first calls still come
   a median 8.5 hours after the lead (22 to 45 minutes elsewhere). Is there a further
   offset, or does AU work leads later?
5. **Treasury:** there is no CAD rate for June 2026 and no July rate for any currency. That
   quarantines 180 spend rows and leaves 675 policies with no GBP premium. Can you supply
   the rates?
6. **Marketing (UK and Ireland):** no source holds an Ireland region. Is Ireland inside UK,
   or missing?
7. **LeadFlow CRM:** 24 leads have a postcode in the wrong format for their region. Is the
   region wrong, or the postcode?
8. **Marketing:** the channel attribution window is not applied; 164 of 4,767 linked
   policies (3.4%) fall outside it. Is that acceptable?
9. **CRM, with the DPO:** please confirm two rules. AU postcodes are cut to their first 3
   digits, and leads with no consent event keep their capture flag, labelled
   `lead_capture`.

**What I would add with more time**, in this order:

1. The Unity Catalog grants, row filter and column mask in `unity_catalog_grants.sql`, with
   SQL tests against the entitlements matrix. This puts the access rules in the data layer,
   not only in the model.
2. The `Ops_Team_Manager` and `Marketing_Analyst_NoPII` roles, with object-level security,
   so every role in the matrix is enforced.
3. The DAX Studio before-and-after performance artefact for one measure.
4. Deploying the Asset Bundle to a dev workspace with the DQ gate as a dependent task.

Best regards,
Jason
