# Fleet pipeline — build specification

## 1. Context

This is a university mini-project, marked out of 100:

| Criterion | Marks |
|---|---|
| Architecture decision (Lambda vs Kappa) | 20 |
| Tech stack justification | 10 |
| Ingestion | 15 |
| Processing | 15 |
| Storage and serving | 10 |
| Observability | 10 |
| Report | 15 |
| Code quality and documentation | 5 |

**Business question:** What is fleet utilization and earnings by area and time of day right now,
and which vehicles are becoming unprofitable once yesterday's fuel and maintenance costs are
factored in?

**Required outputs:**
1. An API returning real-time fleet utilization: active vehicles, idle ratio, trips/hour, earnings by zone.
2. Threshold alerts when a vehicle stays idle for too long.
3. A daily per-vehicle profitability reconciliation report.

The brief allows us to add or change fields. Every change listed in §4 must also be listed as an
assumption in the README.

## 2. Architecture (final — do not change without asking)

**Lambda architecture:**

- **Sources.** The telemetry simulator publishes to Kafka topic `fleet.telemetry`. The expense
  dropper writes one CSV per simulated day into `/landing/expenses/`.
- **Speed layer.** Spark Structured Streaming, run as two separate Spark applications:
  - `archiver` copies raw events from Kafka into the Parquet master dataset. It contains no
    business logic. It is a separate app so that bugs in the speed logic can never corrupt the
    master dataset.
  - `speed` handles validation → DLQ, deduplication, zone enrichment, per-vehicle state with idle
    alerts, hourly zone aggregates, and running daily earnings per vehicle.
- **Batch layer.** Airflow DAG `fleet_daily_reconciliation` runs PySpark batch jobs over the raw
  Parquet data and the expense files, then produces the profitability views and the HTML report.
- **Serving.** PostgreSQL holds both the speed views (`rt_*` tables) and the batch views
  (`batch_*` tables). FastAPI merges the two layers. Grafana provides the dashboards.
- **Storage.** A Parquet data lake on a shared Docker volume mounted at `/lake`, with the root
  configurable through `LAKE_ROOT`.
  - MinIO is not used: its official community Docker images are no longer published.
  - Keep all paths behind `LAKE_ROOT` so an `s3a://` bucket could replace the volume in
    production. Record this in `docs/decisions.md`.
- **Observability.** Prometheus, Alertmanager and Grafana, plus structured JSON logs.

**Why Lambda (so code and comments stay consistent with the report):**
- Live utilization needs seconds of latency but tolerates approximation.
- Profitability is a financial figure. It must be computed over a complete day, must be exact,
  and must be recomputable when a partner resubmits a corrected file.
- Expense data arrives only once per day, so streaming the reconciliation would gain nothing.
- Kappa was rejected: it would need long Kafka retention and replay to recompute a day.

## 3. Simulated clock

- `COMPRESSION = 60`. One real second is one simulated minute, one simulated hour is one real
  minute, and one simulated day is 24 real minutes.
- `SIM_EPOCH` defaults to a Monday at 06:00 simulated time, so the morning peak arrives quickly
  in demos. Day 1 is therefore partial; state this in the README.
- **Clock authority.** A one-shot `simclock-init` service writes `/shared/simclock.json` with
  `real_start_utc`, `sim_epoch` and `compression`, only if the file is absent. `make reset`
  deletes it. Every component reads the clock through `common/simclock.py`, which provides:
  - `now_sim()`
  - `sim_date(ts)`
  - `day_bounds(date)`
  - `sim_minutes_to_real_seconds(n)`
- Every event `timestamp` is simulated event time (ISO-8601, UTC). All windows, watermarks and
  thresholds are configured in **simulated minutes** and converted by helpers. Changing the
  compression must require only a config change.

## 4. Data contracts

Define each schema once in `common/schemas.py`, as a JSON Schema plus the matching Spark
`StructType`. Both layers import it from there.

### 4.1 Telemetry event

The Kafka value is JSON. The Kafka key is `vehicle_id`.

| Field | Type | Notes |
|---|---|---|
| `event_id` | uuid string | **Added.** Used for deduplication. |
| `event_type` | `ping` \| `trip_start` \| `trip_end` | **Added.** Lets trips and earnings be counted without chained stateful operators. |
| `trip_id` | string or null | Null when the vehicle is idle. |
| `driver_id` | string | |
| `vehicle_id` | string | e.g. `V001` |
| `lat`, `lon` | float | Must fall inside the configured city bounding box. |
| `speed` | float | km/h, ≥ 0 |
| `status` | `idle` \| `enroute` \| `on_trip` | |
| `fare` | float | Cumulative fare for the current trip. Final value on `trip_end`; 0 otherwise. |
| `timestamp` | ISO-8601 | Simulated event time. |
| `schema_version` | int | **Added.** Starts at 1. |

### 4.2 Expense file

- File name: `expenses_<sim_date>_v<n>.csv`.
- Written atomically: write a `.tmp` file, then rename it, so a sensor never sees a partial file.
- Columns: `date`, `vehicle_id`, `fuel_cost`, `maintenance_cost`, `distance_covered` (km),
  `service_flag` (bool: the vehicle was serviced or in the garage that day).
- Currency is generic "currency units", with the label configurable.

## 5. Simulators

### 5.1 Telemetry simulator (`simulators/telemetry`)

**Fleet**
- `N_VEHICLES=50` by default. Each vehicle has a fixed driver and a profile (fuel efficiency,
  maintenance propensity, demand multiplier).
- About 5 vehicles are deliberately "lemons": poor efficiency, low demand or high maintenance.
  This guarantees the profitability report has real positives to flag.

**City and zones**
- The city bounding box is configurable. It is split into a `ZONE_ROWS × ZONE_COLS` grid
  (default 4×4, zones `Z01`–`Z16`).
- Zone lookup lives in `common/geo.py`.

**Vehicle state machine**
- `offline → idle → enroute → on_trip → idle`. A vehicle goes back to `offline` at shift end and
  emits no events while offline.
- Shift patterns should put most vehicles offline at night.

**Demand**
- Demand varies by simulated hour (morning and evening peaks, quiet night) and by zone (2–3 hot
  zones).
- Tune it so that idle alerts are meaningful (a handful per simulated day), not constant.

**Movement**
- Moving vehicles head toward a target point at 15–45 km/h with noise.
- Idle vehicles have speed 0 with tiny GPS jitter.

**Fares**
- Fare = base + per km + per minute, accumulated during the trip.
- Emit `trip_start` at pickup and `trip_end` with the final fare at drop-off.

**Emission**
- Each online vehicle emits one event every `EMIT_INTERVAL_REAL_S=2` (about 2 simulated minutes).

**Fault injection** (configurable rates; every injected fault is logged and counted):
- Malformed JSON: about 0.5%.
- Schema-invalid values (e.g. lat outside the box, negative speed): about 0.5%.
- Duplicate `event_id`: about 1%.
- Late or out-of-order events, delayed 1–20 simulated minutes: about 2%.
  - The speed-layer watermark is 10 simulated minutes, so some late events are **intentionally
    dropped by the speed layer but included by the batch layer**.
  - This is the evidence for the consistency argument in the report. Do not "fix" it.

**Demo controls**
- Provide a small HTTP control endpoint or a watched control file with:
  - `force_idle <vehicle_id> <sim_minutes>`
  - `pause` / `resume`
- The `make demo-*` targets call these.

**Odometer ledger**
- At each simulated day end, write `/shared/odometer/<sim_date>.json` mapping each vehicle to its
  true km driven. This is the ground truth the expense dropper uses.

**Kafka producer**
- Settings: `acks=all`, idempotence on, retries with backoff, key = `vehicle_id`.
- Survive broker restarts: log and back off, don't crash.

**Metrics**: Prometheus on `:8001` (see §10).

### 5.2 Expense dropper (`simulators/expenses`)

**When it writes**
- For each closed simulated day D, write `expenses_D_v1.csv` after `EXPENSE_DELAY_SIM_MIN=60`.
  This models files arriving around 01:00 the next day.

**What it writes**
- `fuel_cost` = true km × the vehicle's fuel rate × price, with noise.
- `maintenance_cost` is usually small, with occasional service events that set
  `service_flag=true` and carry a large cost.
- `distance_covered` = true km ± 3%. About 3% of rows get a ±25% discrepancy to exercise the
  distance check.

**Faults**
- Occasional rows with missing values, negative values or an unknown `vehicle_id`.
- A configurable chance of a late file that misses its SLA.

**Demo commands**
- `resubmit <sim_date>` writes a corrected `v2` file.
- `delay-next` makes the next file late.

**Metrics**: Prometheus on `:8002`.

## 6. Ingestion (Kafka)

- Use the official `apache/kafka` image with a pinned version, as a single KRaft broker (no
  ZooKeeper).
- A `kafka-init` service creates the topics:
  - `fleet.telemetry`: 6 partitions, RF 1, retention 7 days.
    - Partitioning by `vehicle_id` keeps each vehicle's events in order, which the stateful idle
      logic requires.
    - Retention covers speed-layer recovery and archiver catch-up only. Long-term history lives
      in the lake, which is part of the Lambda argument.
  - `fleet.telemetry.dlq`: 1 partition.
- Optional: `kafbat/kafka-ui` with a pinned version, for the demo.

## 7. Speed layer (Spark 3.5.x, PySpark)

### 7.1 Spark setup

- Build **one custom Spark image** from the pinned `apache/spark` Python image, adding `pandas`,
  `pyarrow` and the `common` package.
- Use it for the master, the worker and the submit containers. The driver and executors must run
  the same Python minor version.
- Include the matching `spark-sql-kafka-0-10` package.
- Run a standalone master plus 1 worker, and size the cores so both apps fit.

### 7.2 App `archiver` (`streaming/archiver.py`)

- Read from Kafka and parse permissively. Keep the raw `value`, plus Kafka `partition`, `offset`
  and `timestamp`, plus the parsed fields wherever parsing succeeded.
- Write Parquet to `${LAKE_ROOT}/raw/telemetry/`, partitioned by `sim_date` derived from the event
  `timestamp`. Unparseable records go to `sim_date=unknown`.
- Append mode, with a trigger of about every 30 real seconds and its own checkpoint.

### 7.3 App `speed` (`streaming/speed.py`)

Several queries, each with its own checkpoint.

**Parse and validate.** Parse with the shared schema and apply `common.validation` rules.

**Query `dlq`.** Invalid records go to `fleet.telemetry.dlq`, keeping the original payload and an
`error_reason`.

**Common preparation for valid rows**
- `withWatermark("timestamp", 10 simulated minutes)`.
- `dropDuplicatesWithinWatermark(["event_id"])`.
- Add `zone_id`.

**Query `vehicle_state`**
- Use `applyInPandasWithState` grouped by `vehicle_id`, with `EventTimeTimeout`.
- State holds: last status, position, zone, `idle_since`, and whether an alert is open.
- Idle alerts:
  - Open an alert when the vehicle has been idle for at least `IDLE_ALERT_SIM_MIN=45`.
  - Close it when the vehicle leaves idle.
- Mark a vehicle offline on timeout after `OFFLINE_SIM_MIN` with no events.
- Sink through `foreachBatch`:
  - Upsert `rt_vehicle_state`.
  - Insert or update `idle_alerts`.

**Query `zone_hourly`**
- Tumbling window of 1 simulated hour, grouped by `zone_id`.
- Compute: `trips_started`, `trips_completed`, `earnings` (sum of `fare` on `trip_end`), and ping
  counts by status (used as a utilization proxy).
- Update mode. Upsert on `(zone_id, window_start)`.

**Query `vehicle_daily_running`**
- Window of 1 simulated day, grouped by `vehicle_id`.
- Compute running revenue and trips.
- Upsert `rt_vehicle_daily`. The API uses this for the Lambda merge, and the batch layer uses it
  for the drift metric.

**Sink correctness**
- All sinks upsert on natural keys, so replays after a restart don't double count. Comment on
  why this matters.
- Note that exact `countDistinct` is not supported in streaming aggregations. "Active vehicles" is
  derived from `rt_vehicle_state` in SQL instead.

**Metrics**
- A Python `StreamingQueryListener` exports Prometheus metrics from query progress (§10).

## 8. Batch layer (Airflow + PySpark)

### 8.1 Airflow setup

- Pinned Airflow version, `LocalExecutor`, metadata stored in a separate `airflow` database on the
  same Postgres.
- Custom image with Java, PySpark and `common`.
- Batch Spark jobs run in local mode inside the Airflow worker. The data is small, and local mode
  avoids client-mode networking issues. Record this in `docs/decisions.md`.

### 8.2 DAG `fleet_daily_reconciliation`

**Scheduling**
- Runs every 2 real minutes, with `max_active_runs=1` and `catchup=False`.
- Each run picks the oldest simulated date that satisfies both conditions:
  - The day is closed plus `LATE_GRACE_SIM_MIN=30`. This is greater than the maximum injected
    lateness, so the archive is complete.
  - It has an expense file version newer than the last one processed, per `batch_runs`.
- A manual trigger with conf `{"sim_date": "...", "force": true}` recomputes that date. Used for
  the backfill demo.

**Tasks**

1. `find_pending_date`: a ShortCircuit that exits if nothing is pending.
2. `check_expense_file`: waits for the file.
   - If it is missing after `EXPENSE_SLA_SIM_MIN` past day close, set the late flag in
     `batch_runs` (surfaced as a metric) and log it.
3. `validate_expenses`: splits rows into clean and quarantined.
   - Quarantined rows go to `dq_issues` with reason codes: missing value, negative value,
     unknown vehicle, duplicate.
   - Fail the task if more than 20% of rows are invalid.
4. `compute_vehicle_day` (Spark): read the raw Parquet for the date, re-apply `common.validation`,
   and deduplicate on `event_id`. Then compute per vehicle:
   - `trips` and `revenue` (sum of `fare` on `trip_end`).
   - `gps_km`: haversine distance between consecutive valid pings ordered by timestamp, ignoring
     physically impossible jumps.
   - `online_min`, `on_trip_min`, `idle_min` and `utilization`.
5. `reconcile_profitability`: join with expenses on `vehicle_id` and compute:
   - `cost = fuel + maintenance`, `net_profit`, `margin`, `revenue_per_km`, `cost_per_km`.
   - `distance_mismatch_pct = |gps_km − distance_covered| / distance_covered`.
   - Flags:
     - `unprofitable` (net < 0)
     - `low_margin` (margin < `MARGIN_THRESHOLD=0.10`)
     - `becoming_unprofitable` (net profit declining for 3 consecutive days, or unprofitable on
       2 of the last 3 days)
     - `distance_mismatch` (> 15%)
     - `in_service`
     - `missing_costs` (telemetry exists but no expense row)
     - `missing_telemetry` (expense row exists but no telemetry)
6. `load_batch_views`: in one transaction, delete then insert that date's rows in
   `batch_vehicle_daily`. This makes reruns idempotent.
   - Optionally also write `${LAKE_ROOT}/curated/vehicle_daily/sim_date=...`.
7. `compute_drift`: compare fleet revenue in batch vs `rt_vehicle_daily` for the date.
   - Store the result in `speed_batch_drift`.
   - A non-zero drift is expected because of watermark drops. Explain this in the report.
8. `generate_report`: Jinja2 HTML plus a CSV at `/reports/profitability_<sim_date>.html`,
   containing:
   - Fleet summary.
   - Most and least profitable vehicles.
   - Unprofitable and becoming-unprofitable vehicles, with reasons.
   - Distance mismatches.
   - Data-quality section: quarantined rows, late events, drift.
   - Metadata: run id, file version, generation time.
9. `record_run`: write a `batch_runs` row with date, file version, status, per-task durations and
   row counts.

## 9. Serving layer

### 9.1 PostgreSQL

- Pinned PostgreSQL 16, with the schema in `db/init/*.sql`.
- Tables:
  - `rt_vehicle_state`
  - `rt_zone_hourly`
  - `rt_vehicle_daily`
  - `idle_alerts`
  - `batch_vehicle_daily`
  - `batch_runs`
  - `dq_issues`
  - `speed_batch_drift`
  - `alert_notifications` (receives Alertmanager webhooks)
- Views:
  - `v_fleet_live`: active vehicles (seen in the last 5 simulated minutes), idle ratio, on-trip count.
  - `v_zone_live`
- Add sensible indexes.

### 9.2 FastAPI (`api/`)

Pinned version. Pydantic response models. The OpenAPI docs at `/docs` are used in the demo.
"Today" always means the current simulated date.

| Endpoint | Returns |
|---|---|
| `GET /health` | Service and DB health |
| `GET /fleet/live` | Active vehicles, idle ratio, trips in the current and last simulated hour, earnings by zone |
| `GET /zones/live` | Per zone: active vehicles, idle ratio, trips this hour, earnings this hour |
| `GET /zones/{zone_id}/hourly?sim_date=` | Hour-of-day profile |
| `GET /alerts/idle?status=open` | Idle alerts |
| `GET /vehicles/{vehicle_id}` | **Lambda merge:** live state and today's running earnings (speed layer) plus the last 7 days of profitability (batch layer). Every block is labelled with `source: speed\|batch` and `as_of`. |
| `GET /vehicles/unprofitable?sim_date=` | Defaults to the latest reconciled date |
| `GET /reports/{sim_date}` | Serves the HTML report |
| `GET /pipeline/status` | Latest event time seen, last batch run, drift, open alerts |
| `POST /alerts/webhook` | Alertmanager receiver: logs the alert and stores it in `alert_notifications` |
| `GET /metrics` | Prometheus |

### 9.3 Grafana dashboards

Pinned version. Datasources and dashboards are provisioned from JSON committed to the repo.

- **Fleet operations:**
  - Geomap of vehicles, coloured by status.
  - Zone table: active vehicles, idle ratio, trips/hour, earnings.
  - Earnings by zone × hour-of-day heatmap.
  - Open idle alerts.
  - Latest profitability table with flags.
- **Pipeline health:** see §10.

## 10. Observability

### 10.1 Logs

- JSON lines to stdout.
- Required fields: `ts`, `level`, `service`, `stage` (`ingestion|processing|storage|serving|orchestration`),
  `event` (snake_case), `msg`, `sim_time`.
- Context IDs where relevant: `event_id`, `vehicle_id`, `batch_id`, `run_id`, `sim_date`.
- Don't log every event at INFO. Sample per-event logs, and log counts per batch instead.
- Stretch goal: Loki and Promtail so logs are searchable in Grafana.

### 10.2 Metrics

All metric names are prefixed `fleet_`.

**Ingestion**
- `events_produced_total{event_type}`
- `produce_errors_total`
- `faults_injected_total{kind}`
- `expense_files_written_total`

**Processing** (from the query listener)
- `stream_input_rows_per_second{query}`
- `stream_processed_rows_per_second{query}`
- `stream_batch_duration_seconds{query}`
- `stream_last_progress_timestamp{query}`
- `stream_offsets_behind_latest{query}`
  - Read this from the Kafka source metrics in query progress.
  - Spark does not commit consumer-group offsets, so standard Kafka lag exporters cannot see
    Spark's lag.
- `late_rows_dropped_total` (from the state operators' `numRowsDroppedByWatermark`)
- `dlq_events_total`

**Storage**
- `db_upsert_rows_total{table}`
- `db_write_errors_total`

**Batch** (exposed by the API from `batch_runs` and `speed_batch_drift`, so no Pushgateway is needed)
- `batch_task_duration_seconds{task}`
- `batch_last_success_timestamp`
- `batch_quarantined_rows`
- `expense_file_late`
- `speed_batch_drift_ratio`

**Serving**
- HTTP request count and latency
- `open_idle_alerts`

### 10.3 Alert rules

Put the rules in `observability/prometheus/alerts.yml`. Each rule needs a `for:` duration, a
severity and a summary. Add a runbook entry for each one in `docs/runbook.md`.

| Rule | Condition |
|---|---|
| `TelemetryNotProduced` | No events produced for 1 real minute |
| `StreamStalled` | No streaming progress for 2 minutes, or processed rate is 0 while produced rate is > 0 |
| `StreamLagHigh` | Offsets behind latest above threshold for 2 minutes |
| `DLQRateHigh` | DLQ events / produced events > 2% over 5 minutes |
| `ExpenseFileLate` | Expense file missed its SLA |
| `BatchRunFailed` | Last batch run failed |
| `BatchNotRunRecently` | No successful batch run recently |
| `SpeedBatchDriftHigh` | Drift > 5% |
| `ApiDown` | API not reachable |
| `IdleVehiclesHigh` | Business alert: too many open idle alerts |

- Alertmanager routes alerts to the API's `/alerts/webhook`, so delivery is visible in the demo.
- Every alert must be triggerable with a `make demo-*` target or a documented manual step.

### 10.4 Pipeline health dashboard

Panels:
- Throughput per stage.
- Offsets behind latest.
- DLQ rate.
- Watermark drops.
- Batch task durations.
- Drift.
- Alert states.

### 10.5 Health checks

- A Docker Compose healthcheck on every service.
- A `/health` endpoint on every custom service.

## 11. Testing

- **pytest unit tests:**
  - `simclock`
  - `geo` (zone mapping, haversine)
  - `validation`
  - earnings and profitability rules
  - the simulator state machine
  - expense validation
- **PySpark tests** with a local SparkSession and small fixtures for `compute_vehicle_day` and
  reconciliation. Include duplicate, late and invalid events.
- **API tests** with the FastAPI `TestClient`.
- **`scripts/smoke_test.py`**, run against the live stack:
  - Within a few minutes: events are in Kafka, the `rt_*` tables are populated, raw Parquet
    partitions exist, and all metrics endpoints are up.
  - After one simulated day closes: `batch_vehicle_daily` has rows and the report file exists.

## 12. Build phases and acceptance checks

Every check below must actually be run before moving on.

**Phase 0 — Scaffold**
- Build: repo tree, `docker-compose.yml` with all infrastructure and healthchecks,
  `.env.example`, Makefile, `common` package skeleton, DB init scripts.
- Accept: `make up` brings everything up healthy; topics exist; Postgres, Airflow, Grafana and
  Prometheus UIs are reachable.

**Phase 1 — Simulators and ingestion**
- Accept:
  - Consuming the topic shows valid, correctly keyed events.
  - Faults appear at roughly the configured rates.
  - An expense file appears for each closed simulated day.
  - Simulator metrics are served.

**Phase 2 — Speed layer**
- Accept:
  - The `rt_*` tables update within about 10 real seconds.
  - Invalid events reach the DLQ.
  - Duplicates don't inflate counts (proven by a test).
  - `make demo-idle` opens an idle alert, and it closes afterwards.
  - Raw Parquet partitions appear.

**Phase 3 — Batch layer**
- Accept:
  - After a day closes, the DAG succeeds, `batch_vehicle_daily` is populated and the report is
    generated.
  - Lemon vehicles are flagged.
  - `make demo-resubmit` triggers a clean recompute: same row count, updated values.
  - A manual trigger with conf recomputes a chosen date.

**Phase 4 — Serving**
- Accept:
  - All endpoints return correct data.
  - `/vehicles/{id}` shows both layers with source labels.
  - The Fleet operations dashboard is live.

**Phase 5 — Observability**
- Accept:
  - Every alert rule fires via its demo step and appears in Alertmanager and `alert_notifications`.
  - The pipeline health dashboard is populated.
  - Logs have all the required fields.

**Phase 6 — Hardening and docs**
- Accept:
  - Tests pass.
  - A fresh clone followed by `make up` works.
  - The docs below are complete:
    - `README.md`: architecture summary, assumptions and field changes, simulated clock, setup,
      run, a 7-minute demo script, troubleshooting.
    - `docs/decisions.md`
    - `docs/runbook.md`
    - `docs/walkthrough.md`: a plain-English, module-by-module explanation of the core logic,
      written for viva preparation.
    - `docs/report-assets/`: a sample report, sample API responses, and a screenshot checklist.

## 13. Out of scope

- Authentication.
- Multi-broker Kafka or replication.
- Kubernetes.
- ML predictions.
- Real road routing or map tiles beyond Grafana's built-in geomap.