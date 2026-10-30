# 3-minute walkthrough: Ingestion & Orchestration

A timed script for a Loom recording of about 3 minutes (screen plus camera bubble, 1080p). Say it in your own words; the lines are a guide, not a teleprompter.

**Open these tabs before you press record:** README, `pipeline/sources.py`, `pipeline/ingest.py`, `pipeline/transform.py`, `pipeline/definitions.py`, `output/pipeline_report.md`, `tests/test_pipeline.py`, GitHub Actions.

| Time | On screen | What to say |
|---|---|---|
| 0:00–0:20 | Portfolio card, then the README headline | Customer success teams often see account health a day late. This pipeline gets it into the CRM every hour, and it handles the things that usually break: deletes, flaky APIs and reruns. |
| 0:20–0:40 | README diagram and `pipeline/sources.py` | Two sources, both simulated: an application database emitting a Debezium-style change log, and a billing API that pages and sometimes returns 503s. |
| 0:40–1:10 | `pipeline/ingest.py`: CDC merge | Change data capture reads every event past a stored watermark and merges inserts, updates and deletes inside one transaction. Across six hours it picked up 74 updates and 4 deletions; an updated_at load would never see a deletion. |
| 1:10–1:30 | `pipeline/ingest.py`: API extraction | The API is read with cursor pagination, an updated_since high-water mark and exponential backoff. It failed four times, and every failure retried on its own. |
| 1:30–1:55 | `pipeline/transform.py` | The customer_health model combines orders, revenue and unpaid invoices into at-risk, healthy or new. Reverse ETL diffs against the last sync and sends only changed accounts, plus deletions, in Salesforce-style upserts. |
| 1:55–2:20 | `pipeline/definitions.py` | It's all Dagster software-defined assets, with checks that the warehouse matches the source and that there's one row per customer, and an hourly schedule. |
| 2:20–2:40 | `output/pipeline_report.md` | The report shows six hourly runs: events, retries and CRM volumes, with every check passing. A rerun with nothing new applied 0 events and sent 0 updates. That's idempotency. |
| 2:40–3:00 | `tests/test_pipeline.py`, then the README top | Six tests cover deletes, idempotency, retries, pagination, change-only sync and the Dagster run end to end. Links are under the video. |

## Recording checklist
- Close email, chat and notifications. Zoom the browser to 125% so code is readable.
- Run `python -m pipeline.run` once before recording so the outputs are fresh.
- Keep to one take per section; Loom lets you trim. Aim for 2:45 to 3:10.
- Show real numbers on screen as you say them; never estimate on camera.
- Say that the sources are simulated stand-ins for Postgres with Debezium and a SaaS billing API, so it runs locally.

## After recording
1. Copy the Loom share link.
2. In `README.md`, replace the walkthrough line with `- [x] [3-minute Loom walkthrough](<your Loom link>)` and add the link under the title.
3. Add the link to this project's card on saigwe.github.io.
