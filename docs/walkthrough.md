# Walkthrough — the core logic, module by module

Written for viva preparation. Every team member should be able to explain everything here in
plain English. The order follows an event's journey through the system.

---

## 0. The one-paragraph version

A simulator pretends to be 50 vehicles, emitting GPS pings onto a Kafka topic on a clock that
runs 60× fast. Two independent Spark applications read that topic: one archives raw events to
Parquet and does nothing else, the other computes live dashboards. Once a simulated day
closes, Airflow reads the archived day *plus* a partner's expense CSV and recomputes the day
exactly, producing the profitability report. PostgreSQL holds both layers' output; a FastAPI
service serves them, always labelling which layer a number came from.

---

## 1. `common/simclock.py` — why time is the hardest part

**The problem.** A daily reconciliation cannot be demonstrated in a lab if a day takes a day.
So we compress: `COMPRESSION=60` means one real second is one simulated minute, and a
simulated day passes in 24 real minutes.

**The trap.** If every container worked out "now" at its own startup, they would all disagree
by their start times. A window opened by the speed layer would never line up with a day
boundary computed by the batch layer.

**The fix: one clock authority.** A one-shot service writes `/shared/simclock.json` exactly
once, containing three numbers:

```json
{"real_start_utc": "...", "sim_epoch": "2024-01-01T06:00:00Z", "compression": 60}
```

Every container computes `now_sim()` from that same triple, so they all agree to the
microsecond:

```python
elapsed_real = now() - real_start_utc
sim_now      = sim_epoch + elapsed_real * compression
```

**The rule that follows.** Every threshold — watermark, idle timeout, SLA, grace period — is
written in **simulated minutes**. Two conversions exist and it matters which you use:

| Helper | When |
|---|---|
| `sim_minutes_to_real_seconds(n)` | "how long should I `sleep`?" — divides by compression |
| `spark_interval(n)` → `"n minutes"` | Spark windows and watermarks — **no** conversion |

The second is the one people trip over. Event timestamps are *already* simulated time, so
"45 simulated minutes" is literally `"45 minutes"` to Spark. `test_thresholds_are_compression_independent`
pins this: change `COMPRESSION` and the Spark interval must not move.

**Half-open day bounds.** `day_bounds()` returns `[start, end)`. Midnight belongs to the *new*
day, so reruns of adjacent days can never double-count an event.

---

## 2. `simulators/telemetry/` — where the data comes from

### The state machine

```
offline ──shift starts──▶ idle ──trip offered──▶ enroute ──arrives at pickup──▶ on_trip
   ▲                        ▲                    (no fare yet)                     │
   └──── shift ends ────────┴─────────────────────── drop-off, FARE emitted ───────┘
```

`enroute` is the leg to the pickup; `on_trip` is the leg with a passenger. **Only `on_trip`
time counts toward utilization and only `on_trip` distance earns a fare — but fuel is burned
on both.** That asymmetry is exactly what makes some vehicles unprofitable, so it is the heart
of the simulation rather than a detail.

A vehicle mid-trip finishes it before going off shift. Truncating would emit a `trip_start`
with no `trip_end` and skew every trip count downstream.

### Three added fields, and why each was necessary

- **`event_id`** — dedup needs a stable identity. Comparing whole payloads would work but is
  slow and fragile.
- **`event_type`** (`ping` / `trip_start` / `trip_end`) — *this one is architectural.* Without
  it, counting trips means detecting a *status transition* and then aggregating, which is a
  chained stateful operator; Structured Streaming forbids an aggregation after one. With an
  explicit marker, "trips completed" is `count(WHERE event_type='trip_end')` — a plain
  aggregation that works in both layers.
- **`schema_version`** — lets a future v2 producer coexist with v1 consumers.

### The `fare` contract

`fare` accumulates during a trip and is **final on `trip_end`, zero everywhere else**. So
revenue is `SUM(fare) WHERE event_type='trip_end'` — in *both* layers. Summing `fare` over all
rows would multiply revenue by roughly the number of pings per trip. This is the single
easiest way to get revenue wrong, so it has a test in the simulator, the speed layer and the
batch layer.

### Fault injection — every fault earns its place

| Fault | Rate | Proves |
|---|---|---|
| Malformed JSON | 0.5% | the archiver keeps unparseable bytes; the speed layer dead-letters instead of crashing |
| Schema-invalid | 0.5% | `common/validation.py` rejects identically in both layers |
| Duplicate `event_id` | 1% | dedup actually works; both copies are *valid*, so nothing else can stop them |
| **Late by 1–20 sim min** | **2%** | **the consistency argument** — see §5 |

A late event is held back and released later **carrying its original timestamp**. That is what
makes it *late* rather than merely delayed: it arrives announcing an old event time. If we
rewrote the timestamp on release it would just be a normal event and the watermark would never
drop anything.

---

## 3. `streaming/archiver.py` — the boring application, on purpose

Reads Kafka, parses **permissively**, writes Parquet partitioned by `sim_date`. That is all.

Three things to be able to defend:

1. **It has no business logic and its own checkpoint.** The master dataset is the source of
   truth — the batch layer recomputes every past day from it. A bug in the speed layer's
   validation or state handling must not be able to reach it.
2. **Unparseable records are kept**, under `sim_date=unknown`, with their raw bytes. An archive
   that silently dropped malformed records makes "how much bad data did we receive?"
   unanswerable *forever*.
3. **`sim_date` comes from the EVENT timestamp**, not ingestion time. So a late event lands in
   the day it actually belongs to — which is precisely why the batch layer can be complete
   where the speed layer cannot.

The 30-second trigger is a deliberate small-files trade: faster would produce thousands of tiny
Parquet files per simulated day and make the batch *read* slower than the batch *compute*.

---

## 4. `streaming/speed.py` + `state.py` — the live answer

Five queries, each with its own checkpoint, all reading the same topic. Separate because
Structured Streaming allows only one stateful operator chain per query, and we need an
`applyInPandasWithState` **and** two different aggregations.

### Preparation order, and why it is that order

```python
valid.withWatermark("event_time", "10 minutes")   # 1. dedup requires a watermark
     .dropDuplicatesWithinWatermark(["event_id"]) # 2. before enrichment, so we don't do it twice
     .withColumn("zone_id", ...)                  # 3. enrich last
```

`dropDuplicatesWithinWatermark`, not plain `dropDuplicates`: the plain version remembers every
`event_id` it has *ever* seen, so its state grows without bound and eventually kills the
executor. The watermark-scoped version remembers ids only as long as a duplicate could
plausibly still arrive.

### The idle-alert state machine — the piece most worth understanding

"Has this vehicle been idle for 45 simulated minutes?" cannot be answered by a window or an
aggregation. It needs (a) memory of *when* the vehicle last stopped being idle, carried across
micro-batches, and (b) the ability to fire when **no** new event arrives — a silent vehicle is
exactly the case we care about. `applyInPandasWithState` is the only construct giving both.

The state per vehicle is a flat tuple:
`(driver_id, status, zone_id, lat, lon, speed, trip_id, last_event_ts, idle_since_ts, alert_open, alert_opened_ts)`

Four subtleties, each of which is a bug if you get it wrong:

1. **The stopwatch starts on the *transition* into idle**, not on every idle ping.
   ```python
   if new_status == "idle":
       if status != "idle" or idle_since_ts is None:
           idle_since_ts = event_ts      # only on the transition
   ```
   Resetting per ping means the timer never advances and **no alert can ever fire**.

2. **Events are sorted by event time before folding.** A micro-batch may mix Kafka partitions,
   and "the last status change" is meaningless over unordered rows.

3. **A stale event does not rewind the state.** An out-of-order event older than what we have
   already folded is skipped *for state purposes*. It is not lost — the archiver has it and
   the batch layer counts it — but rewinding would corrupt the idle measurement.

4. **`EventTimeTimeout`, not `ProcessingTimeTimeout`.** A processing-time timeout fires after
   N *real* seconds, so changing `COMPRESSION` would silently change the business rule. The
   known consequence: an event-time timeout only fires when the watermark advances, and the
   watermark only advances when *other* events arrive — so if the entire fleet went silent at
   once, no timeout would fire. That case is covered by `TelemetryNotProduced` instead.

**The bug we fixed here.** When an alert closes, the same event that closes it also clears
`idle_since` — so a naive implementation writes `idle_sim_minutes = 0.0` into the alert
history and the row becomes useless for any "how bad was it?" question. The duration is now
captured *at the transition*. Two tests cover it.

### `sinks.py` — why every write is an upsert

`foreachBatch` is **at-least-once**. If the driver dies after writing a micro-batch but before
committing offsets, Spark re-runs that batch and the sink sees the same rows twice. With plain
inserts every dashboard figure would ratchet upward on every restart and never come back. With
`INSERT ... ON CONFLICT (natural key) DO UPDATE` the second write sets the same values again —
idempotent, which is the practical stand-in for exactly-once.

`idle_alerts` keys on `(vehicle_id, opened_at)`, not just `vehicle_id`, so a vehicle that goes
idle, recovers and goes idle again gets **two rows** — alert history is preserved.

### Why `update` output mode

`append` emits a window only once the watermark has passed its end — so the *current*
simulated hour, the thing a live dashboard is about, would never appear. `update` re-emits the
growing window every batch. That is only safe *because* of the upsert.

---

## 5. The consistency argument — §5 is the one to memorise

This is where the marks for "architecture decision" live.

```
                        speed layer                    batch layer
watermark               10 simulated minutes           none
sees late events?       only if < 10 min late          ALL of them
dedup scope             within the watermark           the whole day
latency                 ~5 real seconds                after the day closes + 30 sim min
answer                  approximate                    exact
```

The simulator injects lateness of up to **20** simulated minutes against a **10**-minute
watermark. So the speed layer *knowingly drops* events the batch layer includes. Two metrics
measure it:

- `fleet_late_rows_dropped_total` — rows the watermark discarded (read from the state
  operators' `numRowsDroppedByWatermark`).
- `fleet_speed_batch_drift_ratio` — `(batch_revenue − speed_revenue) / batch_revenue` for a
  reconciled day.

**How big is the drift actually?** Measured on a real run: 11,972 events, 225 injected late,
**15 rows dropped by the watermark**, and `trip_end` is only **3.4%** of events. So the
expected number of dropped *revenue-bearing* events is about `15 x 0.034 = 0.5` per day — and
the first reconciled day came out at drift `0.000000`.

**That is the honest answer, and it is not a weakness in the argument.** Use the two metrics
for their two different jobs:

- `fleet_late_rows_dropped_total` shows the **mechanism** — reliably non-zero.
- `fleet_speed_batch_drift_ratio` shows its **financial materiality** — usually tiny.

The case for computing money in the batch layer was never "the speed layer is wildly wrong".
It is that the speed layer's error is **unbounded in principle and unknowable in advance**. A
watermark is a bet on how late data gets; most days you win it comfortably. But a partner can
resubmit a corrected file for *last month*, and no watermark value answers that. If an examiner
asks "so the drift is zero — why bother with a batch layer?", that is the answer, and
`make demo-resubmit` is the demonstration.

Reading the sign matters:

| Drift | Meaning |
|---|---|
| Small positive | Expected. The watermark dropped some late `trip_end` events. |
| **Negative** | The speed layer **double-counted** — an upsert key is wrong. |
| Large positive | The archive is incomplete — the archiver probably stalled. |
| Exactly zero | Suspicious: late-fault injection is probably disabled. |

Someone will ask *"why not just raise the watermark to 25 and be done?"* The honest answer is
two-part: (a) then the project would have no evidence for its own architecture, and (b) more
substantially, a watermark can only ever be a *bet* on how late data gets — a partner can
resubmit a corrected expense file for **last month**, and no watermark answers that. Only
recomputation from an immutable archive does.

---

## 6. `batch/` + the Airflow DAG — the exact answer

### `find_pending_date` — the only scheduling decision that matters

Airflow's scheduler lives in real time and knows nothing about our compressed clock, so the
2-minute schedule is only a **poll**. The real decision is made against the simulated clock:
the oldest date that is

1. **closed plus `LATE_GRACE_SIM_MIN` (30)** — deliberately *larger* than the maximum injected
   lateness of 20, so anything that is going to arrive already has; and
2. **has an expense file version newer than the last one processed successfully** — which is
   what makes `make demo-resubmit` work: dropping a `v2` makes a reconciled day pending again.

A run that short-circuits at this first task is the **normal** state, not a failure — roughly
eleven of the twelve polls per simulated day do exactly that.

`record_run` is deliberately the **last** task: a date counts as processed only once every task
has finished, so a run that dies halfway leaves the date pending and the next poll retries it.

### `compute_vehicle_day` — three things it does *not* do

- It does **not** trust the speed layer. It does not even read its output. It re-applies the
  same `common/validation.py` rules and re-deduplicates from scratch, because the archiver
  wrote whatever Kafka delivered, malformed payloads included.
- It does **not** apply a watermark. It reads the day's complete partition after the grace
  period, so lateness is simply irrelevant to it.
- It does **not** depend on previous state, so recomputing a day always gives identical
  telemetry figures.

**`gps_km`** is haversine between consecutive pings per vehicle, with one essential filter:
hops implying more than `MAX_PLAUSIBLE_SPEED_KMH` (150) are dropped. Without it, one glitched
coordinate inflates `gps_km` past a full day of driving and the vehicle is flagged for a
distance mismatch the partner never caused.

**Time in status** charges each event the interval *until the next event from the same
vehicle*, with gaps longer than `OFFLINE_SIM_MIN` excluded — otherwise a vehicle that goes off
shift would be credited with an entire night of "on trip" time.

### `reconcile` — a FULL OUTER join, deliberately

An inner join would silently drop the two cases the business most needs to see:

- telemetry but no expense row → **`missing_costs`** (we are blind to this vehicle's costs)
- an expense row but no telemetry → **`missing_telemetry`** (we are being billed for a vehicle
  that never reported)

`becoming_unprofitable` is the flag the business question actually asks for, and it is two
rules because they catch different failures:
- net profit fell on **three consecutive days** — a healthy vehicle sliding, and
- loss-making on **2 of the last 3 days** — a vehicle that is already erratic.

Fewer than three days of history is never flagged: we refuse to call a trend from two points.

### `load_batch_views` — idempotence in one transaction

`DELETE` the date then `INSERT` it, both inside one transaction. After a corrected `v2` file
this leaves **the same number of rows with updated values** — never a mixture of old and new —
and a reader never sees the date half-deleted.

---

## 7. `api/` — where the two layers meet

`GET /vehicles/{id}` is the Lambda merge and the endpoint to show in the demo. It returns three
blocks and **deliberately does not blend them**:

```json
{ "live":    { "source": "speed", "as_of": "...", "status": "idle" },
  "today":   { "source": "speed", "trips": 4, "revenue": 512.25 },
  "history": [ { "source": "batch", "sim_date": "...", "net_profit": 570.0 } ] }
```

Why not merge them into one number? Because they answer different questions with different
guarantees. A fleet manager must be able to tell at a glance which figures are safe to quote
to finance. Blending them is how a dashboard estimate ends up in a financial report.

Two implementation notes worth knowing:

- **`/vehicles/unprofitable` is declared *before* `/vehicles/{vehicle_id}`.** FastAPI matches
  routes in declaration order; the other way round, the dynamic route swallows it and looks for
  a vehicle called "unprofitable". There is a test pinning the ordering.
- **HTTP metrics are labelled with the route *template***, not the raw path. Labelling with the
  raw path would create one Prometheus time series per vehicle id — the classic way to blow up
  a metrics store with unbounded cardinality.

**"Now" is always simulated now.** The views take their reference point from
`max(last_event_time)` in `rt_vehicle_state` — the latest simulated event time the pipeline has
actually seen — rather than the database's `now()`. That is both self-consistent and honest: if
ingestion stalls, "now" stops advancing and the active-vehicle count correctly falls to zero
instead of quietly lying.

---

## 8. Observability — the two questions it answers

**"Is the pipeline healthy?"** → `fleet_stream_last_progress_timestamp`,
`fleet_stream_offsets_behind_latest`, `fleet_dlq_events_total`, `fleet_batch_last_success_timestamp`.

**"Is the *business* healthy?"** → `fleet_open_idle_alerts`, `fleet_speed_batch_drift_ratio`,
and the profitability flags. `IdleVehiclesHigh` is the clearest example: the pipeline is
perfectly fine and the fleet is not earning.

Two details that generate questions:

- **Kafka lag comes from Spark's own query progress**, not a lag exporter. Structured Streaming
  does not commit consumer-group offsets — it keeps them in its checkpoint — so
  `kafka-consumer-groups.sh` and every off-the-shelf exporter report *nothing* for these
  consumers.
- **Batch metrics are exported by the API from PostgreSQL**, not pushed. Airflow tasks are
  short-lived processes; Prometheus pulls, so by scrape time the task is gone. A Pushgateway
  would work but is another service holding job state forever with no notion of staleness. The
  batch layer already writes its outcomes to the database because the report needs them there.

### The bug worth telling the examiner about

Restarting the Spark worker killed the archiver's executor. The driver logged
`Disconnected from Spark cluster!` — and then sat there. The query was still "active", so
`awaitTermination()` never returned. **The container stayed up and its healthcheck stayed
green**, because the healthcheck hit the metrics endpoint served by the (alive) driver. The
archiver silently stopped writing the master dataset for 26 minutes, and the batch layer then
reconciled a day from an incomplete archive.

The lesson generalises past this project: *a healthcheck that only proves a process is alive
proves nothing about whether it is doing its job.* The fix is `streaming/watchdog.py`, which
checks that every query is both active **and progressing**, and exits so Docker restarts it
from the checkpoint.

---

## 9. Questions you should expect

**"Isn't Lambda just Kappa with extra steps?"**
No — the difference is *recomputability*. A partner can resubmit a corrected expense file for
any past day. Kappa answers that by replaying Kafka, which needs retention long enough to cover
any day we might be asked to recompute, and re-reads everything from the start of the window.
Our Kafka retention is 7 days and covers only recovery; the lake is partitioned by day, so the
batch layer reads exactly the one day it needs.

**"How do you avoid writing the logic twice?"**
`common/` is imported by both layers. Two places duplicate deliberately, both for performance —
the validation rules and haversine, which must not run a Python UDF per row. Both pairs are
pinned together by tests that feed identical inputs through both implementations.

**"What happens if the speed layer restarts mid-day?"**
It resumes from its checkpoint. Any events it misses while down are still archived (separate
application, separate checkpoint), so the batch layer's figures are unaffected — the day's
dashboards are briefly wrong, the day's *money* is not. That asymmetry is the architecture
working as intended.

**"Why is the drift not zero?"**
Because it *should not* be. See §5.

**"Which numbers can I trust?"**
Anything labelled `"source": "batch"`. Anything labelled `"source": "speed"` is seconds old and
approximate, and the API says so on every response.
