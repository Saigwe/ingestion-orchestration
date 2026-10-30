# Ingestion & orchestration: run report

**Summary for leadership:** customer health now reaches the CRM **every hour instead of overnight**. Change data
capture picked up **74 updates and 4 account deletions** as they
happened. An incremental load keyed on `updated_at` would never see the deletions, because a deleted row leaves
nothing behind to pick up. The billing API failed
**4 times** and every failure was retried automatically, with no manual reruns. Only changed
accounts are sent to the CRM (**63%** of rows on average), which keeps API costs and CRM noise down.

## Hourly runs

| hour | cdc_events | cdc_inserts | cdc_updates | cdc_deletes | api_pages | api_retries | health_rows | crm_upserts | crm_deletes | crm_unchanged | checks_passed | success |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 44 | 40 | 4 | 0 | 2 | 0 | 14 | 14 | 0 | 0 | True | True |
| 1 | 46 | 31 | 14 | 1 | 2 | 2 | 23 | 17 | 0 | 6 | True | True |
| 2 | 42 | 28 | 14 | 0 | 2 | 0 | 31 | 21 | 0 | 10 | True | True |
| 3 | 49 | 34 | 14 | 1 | 2 | 1 | 41 | 27 | 1 | 14 | True | True |
| 4 | 41 | 26 | 14 | 1 | 1 | 0 | 51 | 26 | 1 | 25 | True | True |
| 5 | 47 | 32 | 14 | 1 | 2 | 1 | 62 | 34 | 1 | 28 | True | True |

Every run's data checks passed: True. A rerun with no new source activity applied
**0 change events** and sent **0 CRM updates**: the pipeline is idempotent.
