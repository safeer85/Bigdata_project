# Fleet pipeline — a Lambda architecture for ride-hailing operations

A data-engineering mini-project: a working **Lambda-architecture** pipeline that answers two
questions about a ride-hailing fleet that need two different kinds of answer.

> **What is fleet utilization and earnings by area and time of day *right now*, and which
> vehicles are becoming unprofitable once yesterday's fuel and maintenance costs are factored in?**

The first half needs an answer in seconds and can tolerate being slightly wrong.
The second half is a financial figure: it must be exact, computed over a complete day, and
recomputable when a partner resubmits a corrected file. That split is the whole reason this
project has two processing layers rather than one.

```
                                  ┌──────────────────────────────────────┐
  telemetry simulator ──Kafka──┬─▶│ archiver  (no business logic)        │──▶ Parquet lake
   50 vehicles, sim clock      │  └──────────────────────────────────────┘      (master
   faults injected on purpose  │                                                 dataset)
                               │  ┌──────────────────────────────────────┐            │
                               └─▶│ speed layer (Structured Streaming)   │            │
                                  │  validate→DLQ, dedup, zones,         │            │
                                  │  idle alerts, hourly & daily aggs    │            │
                                  └───────────────┬──────────────────────┘            │
                                                  │ rt_* tables                       │
  expense dropper ──CSV──▶ /landing               ▼                                   ▼
   one file per sim day         ┌────────────▶ PostgreSQL ◀──────── batch layer (Airflow + Spark)
   resubmittable as v2          │              batch_* tables       exact, complete, rerunnable
                                │                   │
                       FastAPI ─┘                   └─▶ Grafana ─▶ Prometheus / Alertmanager
                    merges both layers,
                  labelling every figure
```

---

## Quick start

**Requirements:** Docker Desktop with **at least 8 GB** allocated, and ~12 GB of disk.
Nothing else — no local Python, Java or Spark.

```bash
git clone <this repo> && cd Bigdata_project
make up            # builds three images, starts 13 services, waits for health
```

On Windows, GNU `make` is usually not installed. Use the shim, which mirrors every target:

```powershell
.\make.ps1 up
```

`make up` prints the URLs when it finishes:

| What | URL | Login |
|---|---|---|
| **API docs (OpenAPI)** | <http://localhost:8000/docs> | — |
| **Grafana** | <http://localhost:3000> | `admin` / `admin` |
| **Airflow** | <http://localhost:8088> | `admin` / `admin` |
| Prometheus | <http://localhost:9090> | — |
| Alertmanager | <http://localhost:9093> | — |
| Spark master | <http://localhost:8080> | — |
| Kafka UI (optional) | <http://localhost:8090> | `make up-tools` |

> **Port already in use?** Every published port is configurable in `.env`
> (`GRAFANA_HOST_PORT`, `SPARK_MASTER_UI_PORT`, `PROMETHEUS_HOST_PORT`, …). Ports *inside*
> the Docker network never change, so nothing in the code depends on these.

### Commands

```
make up             # build + start the full stack, wait for it to be healthy
make down           # stop, keep the data
make reset          # stop and wipe volumes, lake, checkpoints and the sim clock
make logs s=speed   # tail one service
make ps             # container status and health
make test           # unit + PySpark + API tests (186 of them)
make smoke          # end-to-end checks against the running stack
make demo-idle      # force a vehicle idle       → idle alert
make demo-outage    # pause the producer          → TelemetryNotProduced
make demo-resubmit  # corrected v2 expense file   → batch recompute
make demo-late-file # delay the next file         → ExpenseFileLate
```

---

## The simulated clock — read this first

Nothing in this project runs in real time. **One real second is one simulated minute**
(`COMPRESSION=60`), so:

| Simulated | Real |
|---|---|
| 1 minute | 1 second |
| 1 hour | 1 minute |
| **1 day** | **24 minutes** |

That is what makes a *daily* reconciliation demonstrable in a lab session. Consequences you
will actually notice:

- The first profitability report appears roughly **25–30 real minutes** after `make up`.
- `SIM_EPOCH` is a Monday at **06:00**, so the morning demand peak arrives within a minute
  of starting — but **simulated day 1 is a partial day** (18 hours, not 24). Its figures are
  correspondingly smaller. Day 2 onwards are full days.
- The `becoming_unprofitable` flag needs **three** reconciled days, so it only starts
  appearing about 75 real minutes in.

A single one-shot service, `simclock-init`, writes `/shared/simclock.json` **once**; every
other container reads the clock from that file. If each service computed its own epoch at
startup they would disagree by their start times and no two windows would ever line up.
`make reset` deletes the file, and only then does the timeline restart.

**Every threshold in the system is expressed in *simulated* minutes** and converted by
`common/simclock.py`. Changing `COMPRESSION` is a config change and nothing else — there is a
test (`test_thresholds_are_compression_independent`) that pins this.

---

## Assumptions and changes to the brief

The brief allows adding or changing fields. Everything we changed is listed here.

### Added telemetry fields

| Field | Why we added it |
|---|---|
| `event_id` (uuid) | Deduplication. Without a stable id, removing a duplicate means comparing whole payloads. The simulator re-sends ~1% of ids on purpose. |
| `event_type` (`ping` / `trip_start` / `trip_end`) | Lets trips and revenue be counted with a plain aggregation. Without it, counting trips means detecting status *transitions* and then aggregating — a chained stateful operator, which Structured Streaming does not allow. |
| `schema_version` (int, starts at 1) | Lets a future v2 producer coexist with v1 consumers. |

### Interpretation of existing fields

- **`fare` is cumulative within a trip**, and is final on `trip_end` and `0` elsewhere.
  Revenue is therefore `SUM(fare) WHERE event_type = 'trip_end'` in **both** layers. Summing
  `fare` over all rows would multiply revenue by roughly the number of pings per trip.
- **`status = offline` is not emitted.** An off-shift vehicle sends nothing at all. The speed
  layer infers `offline` from a state timeout after `OFFLINE_SIM_MIN` of silence.

### Other assumptions

- **Currency** is generic "currency units" (`CU`), configurable via `CURRENCY_LABEL`.
- **`maintenance_cost` has three components**: a flat daily standing charge (lease/insurance),
  per-km wear, and an occasional lumpy service visit. The standing charge is what makes a
  *low-utilization* vehicle genuinely unprofitable rather than merely quiet — which is the
  business insight the report exists to surface.
- **The city is a 4×4 grid** over a configurable bounding box, zones `Z01`–`Z16`, with 3 hot
  zones and 4 cold ones. Real road routing is out of scope, so vehicles move in straight lines.
- **MinIO is not used.** Its official community Docker images are no longer published, so the
  lake is a Parquet tree on a Docker volume. Every path is built from `LAKE_ROOT`, so swapping
  in an `s3a://` bucket is a one-variable change.

### Faults injected on purpose

The simulator deliberately corrupts its own output so the pipeline's error handling is
demonstrable rather than theoretical:

| Fault | Rate | What it proves |
|---|---|---|
| Malformed JSON | 0.5% | The archiver keeps unparseable bytes (`sim_date=unknown`) instead of losing them; the speed layer dead-letters rather than crashing. |
| Schema-invalid values | 0.5% | `common/validation.py` rejects identically in both layers. |
| Duplicate `event_id` | 1% | Dedup works in both layers and duplicates never inflate counts. |
| **Late events (1–20 sim min)** | **2%** | **The consistency argument.** See below. |
| Bad expense rows | ~4% | Quarantine with reason codes, without failing the whole file. |
| Late expense file | 5% | The SLA alert. |

> **The late events are the point, and they are not a bug.**
> The speed layer's watermark is **10** simulated minutes; injected lateness runs to **20**.
> So the speed layer *intentionally drops* events that the batch layer *includes*. The
> resulting non-zero `fleet_speed_batch_drift_ratio` is the measured evidence for why a
> Lambda architecture has two layers at all. Raising the watermark above 20 would make the
> two layers agree perfectly and destroy the demonstration — there is a test
> (`test_watermark_is_smaller_than_max_lateness_on_purpose`) that fails if someone "fixes" it.

---

## A 7-minute demo

**Prerequisite:** the stack has been up for ~30 real minutes, so at least one simulated day
has been reconciled. Check with `curl localhost:8000/pipeline/status`.

**0:00 — The question and the clock (1 min)**
Open <http://localhost:8000/docs>. Run `GET /health` — point out `sim_time` and
`compression: 60`. "One real second is one simulated minute; a full day takes 24 real minutes,
which is why we can show a *daily* reconciliation at all."

**1:00 — The live answer, from the speed layer (1.5 min)**
Grafana → **Fleet operations**. The geomap is live; the zone table shows hot zones earning
far more than cold ones; the heatmap shows the morning and evening peaks.
Then `GET /fleet/live` — same numbers, in JSON, labelled `"source": "speed"`.

**2:30 — A threshold alert (1 min)**
```bash
make demo-idle
```
Wait ~45 real seconds (= 45 simulated minutes). The vehicle appears in
**Open idle alerts** on the dashboard and in `GET /alerts/idle?status=open`.
Point out that this needed *per-vehicle state carried across micro-batches*, not a window.

**3:30 — The financial answer, from the batch layer (1.5 min)**
`GET /vehicles/unprofitable` and open `GET /reports/{sim_date}` in a browser.
Walk through the flags. Note the lemons — the simulator guarantees a handful of genuinely
bad vehicles, each bad for a *different* reason (thirsty, breaks down, or low demand).

**5:00 — Why there are two layers (1 min)** ← *the marks are here*
`GET /vehicles/V001`. Three blocks: `live` and `today` are `"source": "speed"`, `history` is
`"source": "batch"`, each with its own `as_of`. Then Grafana → **Pipeline health** →
**Rows dropped by the watermark** (non-zero) and **Speed vs batch revenue drift**.
"The speed layer is *knowingly* approximate. The batch layer recomputes the day from the
complete archive. We report both and label which is which, rather than pretending one number
is both fast and exact."

**6:00 — Recomputability (1 min)**
```bash
make demo-resubmit
```
A corrected `v2` expense file lands. Within 2 real minutes Airflow picks the date up again.
Show `batch_runs` gaining a second row for the same date and `batch_vehicle_daily` keeping
**the same row count with different values** — the delete-then-insert is idempotent.
"This is the thing Kappa would have made expensive: replaying a past day from Kafka would
need retention measured in months."

---

## What is where

```
common/          shared by BOTH layers: simclock, geo/zones, schemas, validation,
                 earnings, profitability, JSON logging, metrics, upsert helpers
simulators/      telemetry/ (Kafka producer + fleet state machine), expenses/ (CSV dropper)
streaming/       archiver.py, speed.py, state.py, sinks.py, listener.py, watchdog.py
batch/           runs.py, expenses.py, vehicle_day.py, reconcile.py, report.py
airflow/dags/    fleet_daily_reconciliation.py  (9 tasks)
api/             FastAPI: app.py, queries.py, models.py, batch_metrics.py
db/init/         PostgreSQL schema, indexes and views
observability/   prometheus/ (config + 10 alert rules), alertmanager/, grafana/
scripts/         simclock_init, create_topics, wait_for_stack, demo, smoke_test
tests/           202 tests: unit, PySpark, API
docs/            SPEC.md, report.md, decisions.md, runbook.md, walkthrough.md,
                 report-assets/
```

### Which document to read for what

| Document | Purpose |
|---|---|
| [`docs/report.pdf`](docs/report.pdf) | **The submitted report (PDF, 13 pages).** Rebuild with `python scripts/build_report_pdf.py`. |
| [`docs/demo-script.md`](docs/demo-script.md) | **Shot-by-shot script for recording the 7-minute demo video**, including how to hide the pipeline's real waits. |
| [`docs/report.md`](docs/report.md) | **The written report.** The architecture argument and the measured evidence, structured against the marking criteria. |
| [`docs/decisions.md`](docs/decisions.md) | Every non-trivial choice, alternatives considered, and the six defects found by running it. |
| [`docs/walkthrough.md`](docs/walkthrough.md) | Module-by-module explanation in plain English, for viva preparation. |
| [`docs/runbook.md`](docs/runbook.md) | One entry per alert: what it means, how to reproduce, how to fix. |
| [`docs/report-assets/`](docs/report-assets/) | A real generated report, real API responses, SQL snapshots, screenshot checklist. |

**`common/` is the answer to Lambda's best-known weakness.** The standard criticism is that
the same business rule gets written twice — once for streaming, once for batch — and the two
copies drift apart. Zone mapping, validation, distance, fare and profitability logic all live
in `common/` and are imported by both layers. Where performance forced a second implementation
(the Spark-SQL validation expression and haversine, which must not run a Python UDF per row),
a test pins the two together — see `test_spark_validation_matches_the_python_rules` and
`test_gps_km_matches_the_python_haversine`. Read `docs/walkthrough.md` for the module-by-module
explanation.

---

## Troubleshooting

**`make up` fails with "port is already allocated".**
Something else on your machine owns that port. Override it in `.env` — every published port
is a variable (`GRAFANA_HOST_PORT`, `PROMETHEUS_HOST_PORT`, `SPARK_MASTER_UI_PORT`,
`POSTGRES_HOST_PORT`, …) — then `make up` again.

**Everything is up but `rt_*` tables are empty.**
Check the speed layer: `make logs s=speed`. The most common cause is the Spark worker not
having enough memory for both streaming applications; the worker is sized at 2 GB with the
archiver taking 640 MB and the speed layer 900 MB. If Docker Desktop has less than 8 GB,
raise it in Settings → Resources.

**The archiver or speed layer stopped producing micro-batches.**
They should restart themselves. Both run a progress watchdog that fails the process when a
query stops advancing, because a Spark driver that loses its connection to the master keeps
its queries "active" forever while doing nothing — `awaitTermination()` never returns and the
container looks healthy. If you see repeated restarts, check the Spark master UI
(<http://localhost:8080>) for whether both applications actually have cores.

**No profitability report yet.**
A simulated day takes 24 real minutes, and the batch layer waits a further 60 simulated
minutes for the expense file plus a 30-simulated-minute grace period. First report at roughly
**T+30 real minutes**. Check `curl localhost:8000/pipeline/status` — `reconciled_dates` tells
you what is done, and the Airflow UI shows whether the DAG is short-circuiting (which it does,
correctly, whenever no date is pending).

**Airflow shows no DAG runs.**
The DAG polls every 2 real minutes and `find_pending_date` short-circuits when there is
nothing to do — a run that stops at the first task is the *normal* state, not a failure.

**`make reset` and start over.**
Wipes volumes, the lake, all checkpoints and the simulated clock, so the next `make up`
begins at simulated day 1 again.

---

## Notes for markers

- Every image tag and every Python package is pinned. There is no `latest` anywhere.
- Every service has a Docker healthcheck and a `/health` endpoint; `make up` blocks until the
  stack is genuinely healthy rather than merely started.
- All logs are JSON lines with `ts`, `level`, `service`, `stage`, `event`, `msg` and
  **`sim_time`** — wall-clock time alone tells you nothing on a compressed timeline.
- `docs/decisions.md` records every non-trivial choice, including the ones we got wrong first
  and had to fix.
