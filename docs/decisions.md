# Design decisions

Every non-trivial choice, what else we considered, and why we chose what we chose. This file
feeds the written report. Decisions marked **[corrected]** are ones we got wrong first and
changed after running the thing — those are the most useful ones to be able to talk about.

---

## 1. Lambda, not Kappa

**Decision.** Two processing layers over one immutable master dataset.

**Alternatives considered.**

*Kappa (one streaming layer, recompute by replaying Kafka).* Rejected. The business question
has two halves with genuinely different requirements:

| | Live utilization | Daily profitability |
|---|---|---|
| Latency needed | seconds | next morning is fine |
| Accuracy needed | approximate is fine | must be exact — it is money |
| Completeness | best effort | must cover the whole day |
| Recomputable? | no need | **yes** — partners resubmit corrected files |

Kappa can do the second half, but only by keeping Kafka retention long enough to replay any
day we might be asked to recompute. A partner can resubmit an expense file for last month, so
"long enough" means months of retention for the sole purpose of periodic recomputation — and
each recompute re-reads every event from the start of the window. A Parquet lake partitioned
by `sim_date` lets the batch layer read exactly one day. Our Kafka retention is 7 days and
covers only speed-layer recovery and archiver catch-up.

*Batch only.* Rejected: cannot answer "which vehicles are idle right now".

**What it costs us, and how we mitigate it.** Lambda's well-known weakness is two codebases
that drift. Mitigation: everything both layers need lives in `common/` and is imported by
both — see decision 3.

---

## 2. The archiver is a separate Spark application

**Decision.** `streaming/archiver.py` runs as its own application, with its own checkpoint,
and contains **no business logic** at all.

**Why.** The master dataset is the source of truth: the batch layer recomputes every past day
from it. If it is corrupted, every historical figure is wrong and there is nothing to recover
from. Keeping the archiver separate means a bug in the speed layer's validation, windowing or
state handling *cannot* reach it — the worst a speed-layer bug can do is produce wrong
dashboards, which the next batch run overwrites.

**Alternative.** One application writing both Parquet and the `rt_*` tables. Simpler to
deploy, and one shared checkpoint — which is exactly the problem: a poison record that kills
the speed queries would also stop archiving.

**Consequence accepted.** Both applications read the same Kafka topic, so the broker serves
each event twice. At our volume that is free.

---

## 3. Shared logic in `common/`, and the two places we deliberately duplicated

**Decision.** Zone mapping, event validation, haversine, fare maths and every profitability
flag live in `common/` and are imported by the simulators, the speed layer, the batch layer
and the API.

**The two deliberate exceptions**, both for performance:

| Logic | Python version | Spark version | What keeps them honest |
|---|---|---|---|
| Event validation | `validate_event()` | `spark_validation_expr()` | `test_spark_validation_matches_the_python_rules` feeds identical rows through both and asserts identical reason codes |
| Haversine | `geo.haversine_km()` | inline Spark SQL in `vehicle_day.py` | `test_gps_km_matches_the_python_haversine` |

A Python UDF runs per row and serialises every record to a Python worker. On every streaming
event and every archived ping, that is the single most expensive thing we could do. The
duplication is a considered trade, and it is only acceptable *because* the tests pin the two
implementations together.

---

## 4. Simulated time, and one clock authority

**Decision.** `COMPRESSION=60`. A one-shot `simclock-init` service writes
`/shared/simclock.json` once; every container derives "now" from that file.

**Why a file.** The clock needs `real_start_utc` — the wall-clock instant the simulated
timeline began. An environment variable would be fixed at image build time, and each container
computing it at its own startup would give every service a different epoch, so no two windows
would ever align.

**Why thresholds are in simulated minutes.** Every threshold (watermark, idle alert, offline
timeout, SLA, grace period) is expressed in simulated minutes and converted by
`common/simclock.py`. Changing `COMPRESSION` must not change what a business rule *means*.
`test_thresholds_are_compression_independent` enforces this.

**Consequence accepted.** `SIM_EPOCH` is 06:00, so simulated day 1 is partial (18 hours). We
state it in the README rather than starting at midnight, because starting at midnight would
mean waiting 8 real minutes for the first interesting demand.

---

## 5. `EventTimeTimeout`, not `ProcessingTimeTimeout`

**Decision.** The per-vehicle state machine times out in **event time**.

**Why.** A processing-time timeout fires after N *real* seconds, which is N/60 simulated
minutes — so changing `COMPRESSION` would silently change the business rule "a vehicle is
offline after 20 simulated minutes of silence". With an event-time timeout, the timeout is set
at a simulated timestamp and fires when the watermark passes it.

**Consequence accepted, and worth knowing for the viva.** An event-time timeout only fires
when the watermark advances, and the watermark only advances when *other* events arrive. If
the entire fleet went silent at once, no timeout would ever fire. That is the standard
trade-off of event-time processing, and it is covered by a different control: the
`TelemetryNotProduced` alert, which watches the producer directly.

---

## 6. Watermark = 10 simulated minutes, deliberately smaller than the injected lateness

**Decision.** The speed layer's watermark is 10 simulated minutes. The simulator injects
lateness of up to 20.

**Why not just set it to 25 and catch everything?** Because then the two layers would agree
perfectly and the project would have no evidence for why the batch layer exists. The gap is
the point: the speed layer *knowingly* drops late events, the batch layer includes them, and
`fleet_speed_batch_drift_ratio` measures the difference. It is also a defensible operational
choice on its own — a smaller watermark bounds state size and keeps the dashboard within
seconds of live.

`test_watermark_is_smaller_than_max_lateness_on_purpose` fails if someone raises it, with a
comment explaining why.

**Related:** `dropDuplicatesWithinWatermark`, not plain `dropDuplicates`. The plain version
must remember every `event_id` it has ever seen, so its state grows without bound and
eventually kills the executor. The batch layer, reading one finite day, uses plain
`dropDuplicates` and therefore catches duplicates the speed layer cannot.

### What the drift actually measures — the numbers from a real run **[corrected understanding]**

We expected a visibly non-zero revenue drift and got `0.000000` on the first reconciled day.
That is not a bug, and working out why sharpened the argument rather than weakening it.

Measured over one simulated day:

| Quantity | Value |
|---|---|
| Events produced | 11,972 |
| Late events injected | 225 (1.9%, as configured) |
| Rows dropped by the watermark | 15 |
| Share of events that are `trip_end` (revenue-bearing) | 3.4% |
| **Expected dropped `trip_end` events** | **15 x 0.034 = about 0.5** |

So on a typical day the watermark drops **about half of one** revenue-bearing event. A revenue
drift of exactly zero is therefore the *most likely* single outcome, and a non-zero drift shows
up only on some days.

**Two metrics, two different jobs**, and the report should use both:

- `fleet_late_rows_dropped_total` shows the **mechanism**. It is reliably non-zero, because it
  counts all dropped rows regardless of type.
- `fleet_speed_batch_drift_ratio` shows the **financial materiality** of that mechanism, which
  on a typical day is genuinely near zero.

**Why this is a better argument than a large drift would have been.** The case for computing
money in the batch layer was never "the speed layer is wildly wrong" — it is that the speed
layer's error is *unbounded in principle and unknowable in advance*. A watermark is a bet on
how late data gets. Most days you win the bet by a wide margin. But a partner can resubmit a
corrected expense file for last month, and no watermark value answers that at all. The small
drift is the honest number; the recomputability demo (`make demo-resubmit`) is the argument.

We deliberately did **not** tune the fault rates upward to manufacture a more dramatic figure.
Reporting `0.000000` with the arithmetic above is more defensible than reporting 8% obtained by
quietly raising the late-event rate.

---

## 7. Every sink upserts on a natural key

**Decision.** `INSERT ... ON CONFLICT (natural key) DO UPDATE` everywhere.

**Why.** `foreachBatch` is **at-least-once**. If the driver dies after writing a micro-batch
but before committing offsets, Spark re-runs that batch on restart and the sink sees the same
rows twice. With plain inserts every dashboard figure would ratchet upward on every restart
and never come back. With an upsert the second write sets the same values again, so the sink
is idempotent — which is the practical stand-in for exactly-once.

| Table | Natural key |
|---|---|
| `rt_vehicle_state` | `(vehicle_id)` |
| `rt_zone_hourly` | `(zone_id, window_start)` |
| `rt_vehicle_daily` | `(vehicle_id, sim_date)` |
| `idle_alerts` | `(vehicle_id, opened_at)` — `opened_at` is in the key so alert *history* is kept |
| `batch_vehicle_daily` | delete-then-insert per `sim_date`, in one transaction |

---

## 8. `UPDATE` output mode for the windowed aggregations

**Decision.** `zone_hourly` and `vehicle_daily_running` write in `update` mode, not `append`.

**Why.** Append emits a window only once the watermark has passed its end — so the *current*
simulated hour, which is exactly what a live dashboard is about, would never be shown. Update
re-emits the growing window every micro-batch. That is only safe because the sink upserts on
the window key (decision 7); with plain inserts, an hour's earnings would be written once per
micro-batch and the zone table would show roughly 12x the true figure.

---

## 9. No `countDistinct` in the streaming aggregations

**Decision.** `rt_zone_hourly` stores ping counts by status as a utilization *proxy*;
"active vehicles" is derived in SQL from `rt_vehicle_state` instead.

**Why.** Exact `countDistinct` is unsupported in a streaming aggregation — it would need
unbounded state. `approx_count_distinct` exists but putting an approximation into a figure the
business reads was not worth it when the exact answer is one SQL query away from a table we
already maintain.

---

## 10. Batch Spark runs in local mode inside the Airflow worker

**Decision.** `compute_vehicle_day` creates a `local[2]` SparkSession in the Airflow task.

**Why.** One simulated day is roughly 12k events — a single JVM handles it in about 10
seconds. Submitting to the standalone cluster in client mode would require the driver inside
the Airflow container to be routable from the Spark executors, which is the networking problem
that eats an afternoon in a Docker Compose demo, for no throughput benefit at this scale.

**Consequence accepted.** It does not demonstrate distributed batch processing. The speed
layer does run on the standalone cluster, so the project still shows both.

---

## 11. Airflow in one container, `LocalExecutor`

**Decision.** A single container runs `airflow db migrate`, then the scheduler and the
webserver side by side.

**Why.** The stock Airflow compose file uses four containers (init, webserver, scheduler,
triggerer) — roughly 1.5 GB of overhead for a DAG with nine short tasks, on a laptop already
running Kafka and two Spark applications. `LocalExecutor` still gives real task parallelism.
Airflow's metadata lives in a **separate `airflow` database** on the same PostgreSQL server:
one server to run, but an `airflow db reset` can never touch the fleet tables.

---

## 12. The DAG polls every 2 real minutes; the real decision is inside it

**Decision.** `schedule=timedelta(minutes=2)`, `catchup=False`, `max_active_runs=1`, and a
`ShortCircuitOperator` first task that picks the pending simulated date.

**Why.** Airflow's scheduler lives in real time and knows nothing about our compressed clock,
so the schedule can only be a poll. `find_pending_date` makes the actual decision against the
simulated clock: the oldest date that is (a) closed plus `LATE_GRACE_SIM_MIN` and (b) has an
expense file version newer than the last one processed successfully.

**Why the grace period is 30 and the injected lateness is 20.** The grace must exceed the
worst lateness, or the batch layer could reconcile a day while events for it were still
arriving — and its "exact" claim would be false.
`test_grace_period_exceeds_max_injected_lateness` enforces the relationship.

**Why `record_run` is the last task.** A date counts as processed only once every task has
finished. A run that dies halfway leaves the date pending and the next poll retries it.

---

## 13. Batch metrics are exported by the API, not pushed to a Pushgateway

**Decision.** The API reads `batch_runs` and `speed_batch_drift` on each `/metrics` scrape.

**Why.** Airflow tasks are short-lived processes; Prometheus pulls, so by the time it scrapes,
the task that computed the number is gone. The usual answer is a Pushgateway, but it is a
separate service that holds job state forever with no notion of staleness, and is explicitly
discouraged for exactly this pattern. The batch layer already writes its outcomes to
PostgreSQL because the API and the report need them there — re-exporting is one fewer service,
and the metric and the report can never disagree because they read the same row.

---

## 14. Kafka: KRaft, 6 partitions, keyed by `vehicle_id`

**Decision.** Single broker in KRaft mode (no ZooKeeper), `fleet.telemetry` with 6 partitions
and RF 1, `fleet.telemetry.dlq` with 1.

**Why keyed by `vehicle_id`.** This is a *correctness* requirement, not a throughput one.
Kafka guarantees order only within a partition, and the per-vehicle state machine
(`idle_since`, `alert_open`) assumes it sees one vehicle's events in the order they happened.

**Why 1 partition for the DLQ.** It needs no ordering and carries a trickle of traffic; one
partition keeps every rejected event in a single readable stream for the demo.

**Why RF 1.** Multi-broker replication is explicitly out of scope.

---

## 15. Parquet on a Docker volume, not MinIO

**Decision.** The lake is a Parquet tree under `LAKE_ROOT`, partitioned by `sim_date`.

**Why.** MinIO's official community Docker images are no longer published. Rather than pin an
unmaintained image, we use a volume — and route **every** path through `LAKE_ROOT` so that
replacing it with an `s3a://` bucket in production is a one-variable change.

**Why partition by `sim_date` derived from the EVENT timestamp**, not ingestion time: a late
event lands in the day it actually belongs to, which is precisely why the batch layer can be
complete where the speed layer cannot.

---

## 16. Jars baked into the Spark image, not `--packages`

**Decision.** `spark-sql-kafka-0-10`, `spark-token-provider-kafka-0-10`, `kafka-clients
3.4.1` and `commons-pool2 2.11.1` are downloaded at image build time.

**Why.** `--packages` makes every container start depend on Maven Central being reachable and
on a warm Ivy cache. Baking them in means `make up` works offline and the versions are pinned
in git. The versions are the exact transitive set Spark 3.5.3 expects — a mismatched
`kafka-clients` is the classic `NoSuchMethodError` in Structured Streaming.

**Related:** one Spark image for the master, the worker and both submit containers. PySpark
serialises Python objects between driver and executors; a Python-minor-version mismatch fails
at runtime with an opaque pickle error. One image makes that impossible.

---

## 17. **[corrected]** A progress watchdog, because `awaitTermination()` is not enough

**What happened.** Restarting the Spark worker killed the archiver's executor. The driver
logged `Lost executor 0: Worker shutting down` and `Disconnected from Spark cluster!` — and
then sat there. The query was still "active", so `awaitTermination()` never returned. The
container stayed up, its healthcheck stayed **green** (the metrics endpoint is served by the
driver, which was alive), and the archiver silently stopped writing the master dataset for 26
minutes. The batch layer then reconciled a day from an incomplete archive.

**Why it matters beyond the bug.** Blocking on `awaitTermination` assumes a broken query
*terminates*. In Spark standalone mode a disconnected driver does not. A healthcheck that only
proves the process is alive proves nothing about whether it is doing its job.

**Decision.** `streaming/watchdog.py` checks that every query is both active and progressing,
and calls `os._exit(1)` when one is not. Docker's `restart: unless-stopped` brings the
application back and it resumes from its checkpoint; Kafka retention far exceeds any restart
gap, so nothing is lost. `os._exit` rather than `sys.exit` because `SystemExit` on a
background thread is swallowed without stopping the JVM.

---

## 18. **[corrected]** Cost calibration

**What happened.** The first full reconciliation flagged **35 of 50 vehicles** as unprofitable,
with fleet costs at 1.9x revenue. A report whose answer to "which vehicles are unprofitable?"
is "most of them" answers nothing.

**Root causes, both calibration rather than logic.**
1. Fuel at 105 CU/litre x 0.07 L/km = 7.35 CU/km, against revenue of about 9.25 CU per *driven*
   km — vehicles earn only on the `on_trip` leg but burn fuel on the `enroute` leg too.
2. Service visits cost 1800-6500 CU against a daily revenue of 550-1100. Every serviced
   vehicle became a catastrophic outlier, drowning the genuine lemons.

**Decision.** Fuel price 105 -> **45**; service cost 1800-6500 -> **400-1400**; and an explicit
**daily standing charge** of 90 CU folded into `maintenance_cost`. The standing charge is not
just a fudge factor — it is what makes a *low-utilization* vehicle unprofitable rather than
merely low-earning, which is exactly the business insight the report is supposed to surface.

**Result after recalibration**, on a full reconciled day: 50 vehicles, revenue 49,434 CU, cost
29,663 CU, net +19,771 CU (40% fleet margin), with **8 vehicles flagged unprofitable** rather
than 35. A believable fleet with a handful of real problems in it.

**Lesson for the report.** The pipeline was correct throughout; the *economics* were not. A
data pipeline can be perfectly engineered and still produce a useless answer, and only running
it end to end on real volumes surfaced that.

---

## 19. **[corrected]** The odometer ledger must survive a restart

**What happened.** The telemetry simulator holds each vehicle's true km in memory and writes
the ledger at simulated midnight. A mid-day container restart reset every odometer to zero, so
the ledger under-reported the day while the Parquet archive still held all of it — and the
batch layer flagged half the fleet for a distance mismatch.

**Why it matters.** The distance check exists to catch a *partner* misreporting distance. A
check that fires because of our own restart is worse than no check: it trains the reader to
ignore it.

**Decision.** The open day's odometers are checkpointed to
`/shared/odometer/<date>.partial.json` on every emission tick and reloaded on startup. The
partial file is deleted when the day is sealed.

**Then it happened again, less badly, and the second cause is the more interesting one.**
The first fix checkpointed every **30 real seconds** — which, at `COMPRESSION=60`, is **30
simulated minutes**. A restart during simulated day 2 therefore still lost about 90 km of
fleet distance: reported distance came out ~7% below the archive's `gps_km`, and the batch
layer flagged **21 of 50** vehicles for a distance mismatch the partner had not caused. (Day
1, which had no mid-day restart, showed 3 of 50 — right at the configured 3% discrepancy rate.)

The checkpoint now happens on every tick (~1 KB every 2 real seconds).

**The general lesson, and it applies to every interval in this project:** any duration chosen
in *real* seconds is **60x longer** in the units the business logic actually cares about. "Every
30 seconds" sounded conservative and was in fact half a simulated hour. This is the same class
of mistake the `spark_interval` / `sim_minutes_to_real_seconds` split in `common/simclock.py`
exists to prevent, and it slipped through in a place that was not routed through those helpers.

---

## 20. **[corrected]** `simclock-init` prepares the shared volumes, as root

**What happened.** A clean `make up` failed with `PermissionError: /shared/simclock.json.tmp`.
The app, Spark and Airflow images run as **three different uids** (10001, 185, 50000), and
Docker initialises a fresh named volume from whichever container mounts it first. Which
service happened to start first decided whether the others could write — a start-order race.

**Decision.** `simclock-init` runs as root, mounts all four shared volumes, and opens their
permissions before anything else starts. That is what init containers are for.

**Alternative considered.** A shared group id baked into all three base images. More correct
in production, considerably more fragile here — it means patching three upstream images.

---

## 21. **[corrected]** Lemon failure modes assigned round-robin

**What happened.** A test asserted that the five "lemon" vehicles fail for three *different*
reasons and it failed. The failure mode was derived from the vehicle number (`i % 3`), so a
random sample of five ids could easily miss a whole mode — leaving the profitability report
with nothing to say about, for example, maintenance.

**Decision.** Assign the mode round-robin over the chosen lemons, which guarantees all three
appear whenever `N_LEMONS >= 3`. A small thing, but it is the difference between a demo that
reliably shows three kinds of unprofitability and one that shows whatever the seed happened to
pick.

---

## 21b. **[corrected]** The expense SLA applies to the first delivery, not to corrections

**What happened.** After `make demo-resubmit`, the recomputed run came back with
`expense_file_late = true`. The check compared the file's timestamp against
`day_close + EXPENSE_SLA_SIM_MIN`, and a v2 correction is *always* sent long after that.

**Why it matters.** `ExpenseFileLate` would have fired on every single resubmission. An alert
that fires on a normal, expected, deliberately-demonstrated operation is noise, and noise is
how alerts get ignored.

**Decision.** Only version 1 is assessed against the SLA. The SLA is a promise about
*delivery*; a correction is a different event with no SLA of its own.

---

## 22. Every published host port is configurable

**Decision.** `GRAFANA_HOST_PORT`, `PROMETHEUS_HOST_PORT`, `SPARK_MASTER_UI_PORT`,
`POSTGRES_HOST_PORT` and the rest, all with conventional defaults.

**Why.** Three separate collisions on the development machine (5432, 8081, then 3000/8080/9090
at once, held by an unrelated Docker project). "One command from a fresh clone" cannot be true
if it depends on the marker's laptop having a specific set of free ports. Ports *inside* the
Compose network never change, so no code depends on these.

---

## 23. Structured JSON logs with `sim_time` on every line

**Decision.** `common/logging.py`; no bare `print()` anywhere. Required fields: `ts`, `level`,
`service`, `stage`, `event`, `msg`, `sim_time`.

**Why `sim_time` specifically.** On a compressed timeline a wall-clock timestamp tells you
nothing about which simulated hour a log line belongs to. Every line carries both.

**Why sampling.** At 50 vehicles emitting every 2 real seconds, an INFO line per event would
bury everything else. Per-event logs are sampled 1-in-`LOG_SAMPLE_EVERY`; the useful
granularity is one line per micro-batch with counts.

---

## 24. Known limitations

Stated plainly, because a report that claims no weaknesses is not believed.

- **Simulated day 1 is partial** (06:00 to midnight). Vehicles on the night shift work only
  two of its hours and are charged a full day's standing cost, so day 1 flags more
  low-utilization vehicles than a full day does. Day 2 onwards are representative.
- **The revenue drift is usually zero**, for the reasons measured in decision 6. The mechanism
  is visible in `fleet_late_rows_dropped_total`; its financial impact usually is not.
- **`becoming_unprofitable` needs three reconciled days**, so it is empty for the first ~75
  real minutes of a run. That is correct behaviour (we refuse to call a trend from two points)
  but it does mean the flag cannot be demonstrated in a short session.
- **Batch Spark is local mode**, so the project demonstrates distributed *streaming* but not
  distributed *batch*. See decision 10.
- **The idle-alert timeout cannot fire if the whole fleet goes silent at once**, because an
  event-time timeout needs the watermark to advance. Covered by `TelemetryNotProduced`
  instead. See decision 5.
- **One Kafka broker, RF 1.** Replication is out of scope, so broker loss means data loss for
  anything not yet archived.
