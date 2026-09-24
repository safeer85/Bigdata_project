# Fleet pipeline — instructions for Claude Code

University data-engineering mini-project built by a 3-person team: a **Lambda-architecture**
pipeline for ride-hailing fleet operations (Kafka → Spark Structured Streaming + Airflow/Spark
batch → PostgreSQL → FastAPI/Grafana, with Prometheus observability).

The full specification is in `docs/SPEC.md`. Read the section for the component you are working
on before you touch it. The architecture decisions in SPEC §2 are final; do not change them
without asking.

## Rules that always apply

- **Viva-proof code.** Every team member must be able to explain every line of core pipeline logic.
  Prefer plain, readable code over clever code. Comment the *why* of every transformation,
  window, watermark, state timeout, threshold and upsert key.
- **Build phase by phase** (SPEC §12). After each phase: run it, show the acceptance checks passing,
  then commit. Never start a phase while the previous one is broken.
- **Proceed with reasonable defaults.** Stop and ask only for genuine blockers. Record every
  non-trivial choice in `docs/decisions.md` (decision, alternatives considered, why). That file
  feeds the written report.
- **Never claim something works that you have not run.** If something can't be verified here, say so.
- **Shared logic lives in `common/`** and is imported by both the speed and batch layers. Never
  duplicate zone mapping, validation, distance or earnings logic. This is our main mitigation for
  Lambda's two-codebase problem, and we defend it in the report.
- **Pin every version** (images and packages). No `latest` tags.
- **One command to run everything:** `make up` on a 16 GB laptop, from a fresh clone.
- **Structured JSON logging** everywhere via `common/logging.py`. No bare `print()`.
- **Config via environment variables**, documented in `.env.example`. No secrets committed.
- Commit after each phase with a clear message (e.g. `phase-2: speed layer with idle alerts`).

## Commands (keep this list accurate as they are built)

```
make up             # build + start the full stack
make down           # stop
make reset          # stop, wipe volumes, lake, checkpoints and sim clock
make logs s=<svc>   # tail one service's logs
make test           # unit + PySpark tests
make smoke          # end-to-end smoke test against the running stack
make demo-idle      # force one vehicle idle → idle alert
make demo-outage    # pause the telemetry producer → no-data alert
make demo-resubmit  # drop a corrected v2 expense file for a past day → batch recompute
make demo-late-file # delay the next expense file past its SLA → late-file alert
```

## Repository layout

```
common/          shared package: simclock, geo/zones, schemas, validation, earnings, logging, metrics
simulators/      telemetry/ (Kafka producer) and expenses/ (daily CSV dropper)
streaming/       Spark Structured Streaming apps: archiver + speed layer
batch/           Spark batch jobs called by Airflow
airflow/dags/    fleet_daily_reconciliation DAG
api/             FastAPI serving layer
db/init/         PostgreSQL schema (tables, views, indexes)
observability/   prometheus/ (config, alert rules), alertmanager/, grafana/ (provisioning, dashboards)
scripts/         demo helpers, smoke test
tests/           pytest (unit, PySpark, API)
docs/            SPEC.md, decisions.md, runbook.md, walkthrough.md, report-assets/
```

## Team ownership (for the individual-contribution statement)

- Member 1: `simulators/`, Kafka setup, `common/schemas` + `common/validation`
- Member 2: `streaming/`, `api/`, real-time tables
- Member 3: `batch/`, `airflow/`, `observability/`

When working inside one area, don't refactor another area's code without saying so.