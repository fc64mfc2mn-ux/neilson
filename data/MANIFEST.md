# MANIFEST

Row count, column count and SHA-256 hash for every file in `data/`.
This lets a reviewer confirm the input files were not edited after issue. `data/` is read-only.

| File | Source system | Rows | Columns | SHA-256 |
| --- | --- | --- | --- | --- |
| `reference/ref_region.csv` | Reference | 4 | 8 | `3bbe34d07b0e6179299ab13208c3f21308a133a5f1f42c6c5defcb6178379c33` |
| `reference/ref_channel.csv` | Reference | 8 | 5 | `12e5507997637c3d429057d9bf175d470d4191544b91731929e4c31056a56fa7` |
| `reference/sec_user_region.csv` | Reference | 7 | 3 | `7ba50c041b97c51397a797913581f947e4933838252584e659497a1ece6046ae` |
| `marketing/dim_campaign.csv` | AdHub | 24 | 9 | `b9f5ce26df396cc9c173b1e3f46db29ae868e1523f40414e17dfa7d1223b6ae8` |
| `marketing/marketing_spend_daily.csv` | AdHub | 2224 | 8 | `87a43f58a5b556f76ed200b2fa71b9cecad9935e5a5e483775665e5e0ee2e94e` |
| `marketing/leads.csv` | LeadFlow CRM | 48000 | 22 | `27e95008cb2499bcad2ce1bd0a5910d8bc33249b3ea26366c76d0a3029a2117f` |
| `marketing/consent_events.json` | LeadFlow CRM | 3400 | 6 | `aa3ec7f49bb0b2bdd0a6d132734775db732402a20f2620411d8cb05e375772a9` |
| `contact_centre/calls_uk_au.csv` | Aurora Dialler | 41000 | 19 | `2a5fe87adfd036cfd143dc775c4fae7ee60729807a469eefc2f67343fd800add` |
| `contact_centre/calls_ca_us.csv` | Aurora Dialler | 23000 | 18 | `af6d55398ce7bfa56d13d3f0a539e79ba77f6be2e219cbf02c7729347aed64f3` |
| `contact_centre/dim_agent.csv` | Workforce extract | 45 | 10 | `7450d80719d2dc503605c9f80b56003948375f4af0f880b63d4c7ed85ee7fcdb` |
| `finance/policies.csv` | PolicyCore | 5200 | 16 | `d9489e8107db69680a9366da8109f308f1b63e2dafbfd32953da9f92782addbd` |
| `finance/fx_rates_monthly.csv` | Finance Treasury | 11 | 3 | `5d1ec3793270c77bb376db1e78084566a0637f51ef46eb9be0c72e78f059d974` |
