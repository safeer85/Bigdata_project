# Runbook

One entry per alert rule in `observability/prometheus/alerts.yml`. Each says what fired, what
it means, how to reproduce it deliberately, how to diagnose it, and how to fix it.

**Every alert is triggerable**, either by a `make demo-*` target or by a documented manual
step. Alertmanager routes everything to the API's `/alerts/webhook`, so delivery is verifiable
with one query:

```sql
SELECT alertname, severity, status, summary, received_at
FROM alert_notifications ORDER BY received_at DESC LIMIT 10;
```

A note on timings throughout: **one real minute is one simulated hour**. The `for:` durations
in the alert rules are short (1–2 real minutes) on purpose — a production-style 5-minute
window would be five simulated hours and far too slow to demonstrate.

---

## TelemetryNotProduced

**Severity:** critical **Stage:** ingestion

```promql
sum(increase(fleet_events_produced_total[1m])) == 0 or absent(fleet_events_produced_total)
```

**Means.** No telemetry events have been produced for a real minute (a simulated hour). The
fleet has gone dark. Everything downstream — the live dashboard, idle alerts, the day's
archive — is starving.

The `absent()` half covers a different failure from the `increase() == 0` half: the first is
"the simulator is up but silent", the second is "the simulator is gone and its series has
disappeared entirely".

**Reproduce.**
```bash
make demo-outage           # pauses the producer
python scripts/demo.py outage --resume    # restores it
```

**Diagnose.**
1. `curl localhost:8010/health` — if `paused: true`, someone ran the demo. Resume it.
2. `make logs s=telemetry-sim` — look for `produce_error` or `produce_failed` events.
3. `docker compose ps kafka` — if the broker is unhealthy, the producer is backing off
   correctly (it logs and retries rather than crashing, by design).

**Fix.** Resume the producer, or restart it: `docker compose restart telemetry-sim`. If Kafka
is the problem, fix Kafka first — the producer will reconnect on its own.

---

## StreamStalled

**Severity:** critical **Stage:** processing

```promql
(time() - max by (query) (fleet_stream_last_progress_timestamp) > 120)
or (max by (query) (fleet_stream_processed_rows_per_second) == 0
    and on() (sum(rate(fleet_events_produced_total[2m])) > 0))
```

**Means.** A Spark streaming query has not completed a micro-batch in over 2 minutes, or it is
processing nothing while the producer is still emitting. Two separate failure shapes, because
a stalled stream can either stop reporting or keep reporting zero.

**This is the alert that caught a real bug.** A Spark driver that loses its connection to the
master keeps its queries *active* while never running another batch — `awaitTermination()`
never returns, the container stays up, and the Docker healthcheck stays green because the
metrics endpoint is served by the (alive) driver. The archiver once stopped writing the master
dataset for 26 minutes exactly this way.

**Reproduce.**
```bash
docker compose restart spark-worker    # kills both applications' executors
```
The watchdog should fail each driver within ~2–4 minutes and Docker should restart it.

**Diagnose.**
1. `make logs s=speed` / `make logs s=archiver` — look for `watchdog_kill`, which names the
   stalled query and how long it had been silent.
2. Spark master UI (<http://localhost:8080>) — do both applications actually hold cores? The
   worker has 3 cores and 2 GB; the archiver takes 1 core / 640 MB and the speed layer 2 cores
   / 900 MB. If an old application is still holding resources, the new one waits forever.
3. `curl localhost:8003/metrics | grep last_progress` for the per-query timestamps.

**Fix.** Usually self-healing — the watchdog exits and Docker restarts from the checkpoint.
If it restart-loops, the cause is almost always resources: check the Spark master UI, and
raise Docker Desktop's memory if it is under 8 GB.

---

## StreamLagHigh

**Severity:** warning **Stage:** processing

```promql
max by (query) (fleet_stream_offsets_behind_latest) > 5000
```

**Means.** The speed layer is falling behind the producer. At 50 vehicles emitting every 2
real seconds, a sustained backlog above 5000 offsets means micro-batches cannot keep up.

**Note.** This number comes from Spark's own query progress, not from a Kafka lag exporter.
Structured Streaming does **not** commit consumer-group offsets — it tracks them in its own
checkpoint — so `kafka-consumer-groups.sh` and every off-the-shelf lag exporter report nothing
at all for these consumers.

**Reproduce.** Stop the speed layer for a few minutes and restart it:
```bash
docker compose stop speed && sleep 180 && docker compose start speed
```
It will show a large lag while it catches up (bounded by `maxOffsetsPerTrigger=20000`).

**Diagnose.** `fleet_stream_batch_duration_seconds` — if batch duration exceeds the trigger
interval, the query is genuinely too slow. Check the executor's memory on the Spark UI.

**Fix.** Transient catch-up after a restart resolves itself. If sustained: give the worker
more cores, raise the trigger interval, or lower `EMIT_INTERVAL_REAL_S` on the simulator.

---

## DLQRateHigh

**Severity:** warning **Stage:** ingestion

```promql
sum(rate(fleet_dlq_events_total[5m]))
  / clamp_min(sum(rate(fleet_events_produced_total[5m])), 0.001) > 0.02
```

**Means.** More than 2% of events are being dead-lettered. **The expected baseline is about
1%** — 0.5% malformed plus 0.5% schema-invalid, both injected deliberately. Above 2% means
something *new* is broken.

**Reproduce.** Raise the injected rate and restart the simulator:
```bash
# in .env
FAULT_INVALID_RATE=0.08
docker compose up -d telemetry-sim
```

**Diagnose.**
```bash
curl -s localhost:8003/metrics | grep fleet_dlq_events_total
```
The `reason` label says which rule is rejecting: `coords_out_of_bounds`, `negative_speed`,
`bad_status`, `unparseable_json`, `missing_field`, … Then read the payloads:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fleet.telemetry.dlq --from-beginning --max-messages 20
```
Each DLQ message carries the **original payload** alongside the reason, so a corrected
producer can replay them.

**Fix.** Fix the producer. The DLQ is a queue, not a log — the events are retained and
replayable.

---

## ExpenseFileLate

**Severity:** warning **Stage:** orchestration

```promql
fleet_expense_file_late > 0
```

**Means.** No expense file arrived within `EXPENSE_SLA_SIM_MIN` (120 simulated minutes) of the
simulated day closing. Profitability for that day **cannot be computed** until the partner
delivers. The telemetry half is unaffected.

**The SLA applies to the FIRST delivery only.** A corrected `v2` file is by definition sent
after the original, so scoring it against the same SLA would mark every resubmission late and
this alert would fire on every `make demo-resubmit`. The SLA governs delivery, not corrections
(`tests/test_batch_scheduling.py::test_the_sla_applies_to_the_first_delivery_not_to_corrections`).

**Reproduce.**
```bash
make demo-late-file        # holds the next file past its SLA
```

**Diagnose.**
```sql
SELECT sim_date, expense_file_version, status, expense_file_late, notes
FROM batch_runs ORDER BY started_at DESC LIMIT 5;
```
```bash
curl localhost:8011/health          # next_file_held_until tells you if a demo caused it
docker compose exec expense-sim ls /landing/expenses/
```

**Fix.** Wait for the file, or have the partner resubmit. Once any version arrives, the DAG
picks the date up on its next 2-minute poll — no manual intervention. To force it:
```bash
python scripts/demo.py resubmit --sim-date 2024-01-02
```

---

## BatchRunFailed

**Severity:** critical **Stage:** orchestration

```promql
fleet_batch_run_failed > 0
```

**Means.** The most recent `fleet_daily_reconciliation` run ended in failure. That day has no
profitability figures.

**Reproduce.** Corrupt an expense file beyond the 20% tolerance:
```bash
docker compose exec -u root expense-sim sh -c \
  "sed -i 's/,[0-9.]*,[0-9.]*,[0-9.]*,/,,,,/' /landing/expenses/expenses_2024-01-02_v1.csv"
python scripts/demo.py resubmit --sim-date 2024-01-02   # forces a rerun
```

**Diagnose.**
1. Airflow UI → `fleet_daily_reconciliation` → the red task → Logs.
2. `SELECT run_id, sim_date, status, notes FROM batch_runs ORDER BY started_at DESC LIMIT 5;`
   — `notes` carries the failure reason.
3. The most common genuine failure is `validate_expenses` refusing a file with more than 20%
   invalid rows. That refusal is deliberate: a file that broken must not become a financial
   figure. Check what went wrong:
   ```sql
   SELECT reason, count(*) FROM dq_issues WHERE run_id = '<run_id>' GROUP BY reason;
   ```

**Fix.** Fix the file and resubmit as a new version, or trigger the DAG manually with
`{"sim_date": "2024-01-02", "force": true}`. The load is delete-then-insert per date, so a
rerun is always safe.

---

## BatchNotRunRecently

**Severity:** warning **Stage:** orchestration

```promql
(time() - fleet_batch_last_success_timestamp > 2700) or absent(fleet_batch_last_success_timestamp)
```

**Means.** No successful reconciliation in 45 real minutes. A simulated day closes every 24
real minutes, so a gap this long means the DAG is stuck, paused, or never finding a pending
date.

**Important:** a DAG run that stops at `find_pending_date` is **normal**, not a failure. It
short-circuits whenever nothing is pending, which is most of the twelve polls per simulated
day.

**Diagnose.**
```bash
docker compose exec airflow bash -lc 'cd /opt/fleet && python -c "
from batch import runs
print(\"files:\", runs.available_files())
print(\"processed:\", runs.processed_versions())
print(\"pending:\", runs.find_pending_date())"'
```
- Files but nothing pending → every available version has already been processed. Normal.
- No files at all → the expense dropper is the problem, not the DAG. See **ExpenseFileLate**.
- A pending date but no run → check the scheduler is alive:
  `curl localhost:8088/health`.

**Fix.** Unpause the DAG in the Airflow UI, or `docker compose restart airflow`.

---

## SpeedBatchDriftHigh

**Severity:** warning **Stage:** processing

```promql
abs(fleet_speed_batch_drift_ratio) > 0.05
```

**Means.** Speed-layer and batch-layer revenue for the same day differ by more than 5%.

**Read this before treating it as a fault.** A **small positive** drift (batch above speed) is
*expected and healthy*: the speed layer applies a 10-simulated-minute watermark and about 2%
of events are injected 1–20 minutes late, so the speed layer legitimately misses some
`trip_end` events. This metric existing and being non-zero is the evidence for why the batch
layer is the authoritative one.

What is *not* healthy:

| Observation | Likely cause |
|---|---|
| **Negative** drift (speed above batch) | The speed layer is double-counting — an upsert key is wrong. |
| Drift above ~5% | The archive is incomplete (see **StreamStalled** — the archiver may have stalled silently), or the injected lateness was raised above the grace period. |
| Drift exactly 0 | Suspicious. Check the late-fault injection is still enabled. |

**Diagnose.**
```sql
SELECT sim_date, speed_revenue, batch_revenue, drift_abs, drift_ratio, speed_trips, batch_trips
FROM speed_batch_drift ORDER BY sim_date DESC LIMIT 5;
```
Then compare against `fleet_late_rows_dropped_total` — the drift should be roughly
proportional to the rows the watermark dropped.

**Fix.** If negative: check the `ON CONFLICT` keys in `streaming/sinks.py`. If large and
positive: verify the archiver has been running continuously for the whole of that simulated
day (`make logs s=archiver | grep micro_batch`).

---

## ApiDown

**Severity:** critical **Stage:** serving

```promql
up{job="api"} == 0
```

**Means.** Prometheus cannot scrape `api:8000`. The dashboards are blind, **and** every batch
metric goes stale — the API is what exports them from PostgreSQL. An inhibit rule suppresses
the downstream orchestration alerts while this one is firing, so the on-call sees one root
cause rather than five symptoms.

**Reproduce.**
```bash
docker compose stop api    # then: docker compose start api
```

**Diagnose.** `make logs s=api`; `curl localhost:8000/health`. If `status: degraded`, the API
is up but PostgreSQL is not — check `docker compose ps postgres`.

**Fix.** `docker compose restart api`. If it will not start, it is almost certainly waiting on
the database: `db.wait_for_db()` blocks at startup by design, so that the API never serves
500s while looking healthy.

---

## IdleVehiclesHigh

**Severity:** warning **Stage:** serving

```promql
fleet_open_idle_alerts > 8
```

**Means.** A **business** alert, not an infrastructure one: the pipeline is healthy and the
fleet is not earning. More than 8 vehicles have been idle past `IDLE_ALERT_SIM_MIN` (45
simulated minutes) simultaneously. Either demand has collapsed or too many vehicles are parked
in cold zones.

**Reproduce.**
```bash
for v in V001 V002 V003 V004 V005 V006 V007 V008 V009; do
  python scripts/demo.py idle --vehicle $v
done
```
Wait ~45 real seconds.

**Diagnose.**
```sql
SELECT zone_id, count(*) FROM idle_alerts WHERE status = 'open' GROUP BY zone_id ORDER BY 2 DESC;
```
```bash
curl -s localhost:8000/zones/live | python -m json.tool
```
Clustered in the cold zones (`Z01`, `Z04`, `Z13`, `Z16`) → a positioning problem. Spread
evenly across all zones → genuine low demand, most likely the overnight trough, which is
expected between simulated 01:00 and 05:00.

**Fix.** Operationally: reposition vehicles toward `Z06`, `Z07`, `Z10`. In the demo: the
forced-idle expires on its own, or `docker compose restart telemetry-sim` clears it
immediately.

---

## General triage

**Is the pipeline healthy at all?** One call answers it:
```bash
curl -s localhost:8000/pipeline/status | python -m json.tool
```
- `lag_sim_minutes` above ~15 → the speed layer is behind.
- `last_batch_run.status != "success"` → see **BatchRunFailed**.
- `drift.drift_ratio` negative or large → see **SpeedBatchDriftHigh**.
- `reconciled_dates` empty after 30+ real minutes → see **BatchNotRunRecently**.

**Reading the logs.** Everything is JSON lines with a `sim_time` field, so filter by event:
```bash
make logs s=speed     | grep '"event": "micro_batch"'
make logs s=airflow   | grep '"event": "reconciliation_complete"'
make logs s=archiver  | grep '"event": "watchdog_kill"'
```

**Start completely fresh.** `make reset && make up` — wipes volumes, the lake, all checkpoints
and the simulated clock, and restarts at simulated day 1.
