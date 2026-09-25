# Screenshot checklist for the written report

Capture these in order. Each entry says **what to show**, **where it is**, and **what the
caption should claim** — the caption matters more than the picture, because the marks are for
the argument, not the screenshot.

**Before starting:** the stack must have been up for **at least 75 real minutes** so that three
simulated days have been reconciled and the `becoming_unprofitable` trend flag has data. Check:

```bash
curl -s localhost:8000/pipeline/status | python -m json.tool | grep -A5 reconciled_dates
```

---

## A. Architecture and ingestion (criterion: Ingestion, 15 marks)

**A1 — The stack is really running.**
`docker compose ps` in a wide terminal, showing all 13 services `running (healthy)`.
*Caption:* "Thirteen services, every image tag pinned, a healthcheck on each. `make up` blocks
until all are healthy."

**A2 — Topics and partitioning.**
Kafka UI (<http://localhost:8090>, `make up-tools`) → `fleet.telemetry`, 6 partitions.
Or the terminal output of `scripts/create_topics.sh`.
*Caption:* "6 partitions, keyed by `vehicle_id`. This is a **correctness** requirement, not a
throughput one: Kafka guarantees order only within a partition, and the per-vehicle idle state
machine needs each vehicle's events in order."

**A3 — A real event on the wire.**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fleet.telemetry \
  --max-messages 3 --property print.key=true --timeout-ms 15000
```
*Caption:* "Key is `vehicle_id`; `timestamp` is simulated event time; `fare` is 0 on pings and
final on `trip_end`."

**A4 — Fault injection at the configured rates.**
`curl -s localhost:8001/metrics | grep fleet_faults_injected_total`
*Caption:* "Faults injected deliberately at ~0.5 / 0.5 / 1 / 2 %. The 2% late events are the
evidence in section F."

---

## B. Storage (criterion: Storage and serving, 10 marks)

**B1 — The Parquet lake, partitioned by simulated date.**
```bash
docker compose exec spark-master ls -R /lake/raw/telemetry/ | head -30
```
*Caption:* "Master dataset partitioned by `sim_date` derived from the **event** timestamp, so a
late event lands in the day it belongs to. `sim_date=unknown` holds unparseable payloads —
the archive is lossless."

**B2 — The serving schema.**
`docker compose exec postgres psql -U fleet -d fleet -c "\dt"`
*Caption:* "`rt_*` written by the speed layer, `batch_*` by the batch layer, side by side. Each
`rt_*` table has a primary key on its natural key so the streaming sinks can upsert."

---

## C. The live answer (criterion: Processing, 15 marks)

**C1 — Fleet operations dashboard, whole page.**
Grafana → Fleet operations. Capture during a simulated peak hour (08–10 or 17–20) so the map
is busy.
*Caption:* "Live utilization from the speed layer: geomap by status, per-zone idle ratio and
earnings, hour-of-day heatmap."

**C2 — The heatmap alone, zoomed.**
*Caption:* "Earnings by zone × hour of day. The two demand peaks and the three hot zones are
clearly separated — single-hue sequential ramp because this encodes magnitude."

**C3 — `GET /fleet/live`.**
From <http://localhost:8000/docs>, expanded response.
*Caption:* "The same figures via the API, explicitly labelled `\"source\": \"speed\"`."

---

## D. Threshold alerting (a required output)

**D1 — Trigger it.** Terminal running `make demo-idle`, showing the expected wait.

**D2 — The alert, ~45 real seconds later.** Either the **Open idle alerts** panel or:
```bash
curl -s "localhost:8000/alerts/idle?status=open" | python -m json.tool
```
*Caption:* "45 simulated minutes idle → alert. This needs per-vehicle state carried across
micro-batches and an event-time timeout, not a window — `applyInPandasWithState`."

**D3 — It closes again.**
```sql
SELECT vehicle_id, opened_at, closed_at, idle_sim_minutes, status
FROM idle_alerts ORDER BY opened_at DESC LIMIT 5;
```
*Caption:* "The alert closes when the vehicle moves, and the row records how long it **was**
idle."

---

## E. The financial answer (a required output)

**E1 — The HTML report, full page.**
<http://localhost:8000/reports/{sim_date}> — use the fleet-summary + "requires attention"
section.
*Caption:* "Daily per-vehicle reconciliation: telemetry revenue against the partner's fuel and
maintenance costs."

**E2 — The "requires attention" table, zoomed.**
*Caption:* "Each flagged vehicle with its reason. The five 'lemon' vehicles are bad in three
different ways — poor fuel efficiency, frequent servicing, low demand — so the flags do not all
trace to one root cause. Measured over four simulated days, lemons were flagged on 25% of
vehicle-days against 12.8% for the rest of the fleet."

*Be accurate about this in the report:* the lemons are roughly **twice as likely** to be
flagged, not certain to be. A high-maintenance vehicle only looks bad on a day it actually
breaks, and a low-demand vehicle earns less but also spends less, so it can stay profitable.
That is realistic — a single day is not enough evidence to condemn a vehicle, which is exactly
why `becoming_unprofitable` looks at a three-day trend.

**E3 — The data-quality section of the same report.**
*Caption:* "The report states its own uncertainty: quarantined rows by reason, and the
speed-vs-batch drift."

**E4 — `GET /vehicles/unprofitable`.**
*Caption:* "Same answer via the API, labelled `\"source\": \"batch\"`, with `reasons` spelled
out rather than raw booleans."

---

## F. **Why Lambda** (criterion: Architecture decision, 20 marks — the most important section)

**F1 — The Lambda merge.**
`GET /vehicles/V001`, expanded so all three blocks are visible.
*Caption:* "`live` and `today` are `\"source\": \"speed\"`; `history` is `\"source\": \"batch\"`.
Each block carries its own `as_of`. They are reported **side by side, not blended** — the
caller must be able to tell which figures are safe to quote to finance."

**F2 — Watermark drops.**
Pipeline health → **Rows dropped by the watermark**, non-zero.
*Caption:* "The speed layer's 10-simulated-minute watermark drops events that arrive later.
About 2% of events are injected 1–20 minutes late, so this is **expected**, not a fault."

**F3 — The drift figure.**
Pipeline health → **Speed vs batch revenue drift**, plus:
```sql
SELECT sim_date, speed_revenue, batch_revenue, drift_abs, drift_ratio FROM speed_batch_drift;
```
*Caption:* "The two layers disagree by a small, measured amount for exactly the reason above.
This number **is** the architecture argument: the speed layer is knowingly approximate; the
batch layer recomputes the complete day and is authoritative."

**F4 — Recomputation.**
`make demo-resubmit`, then before/after:
```sql
SELECT count(*), round(sum(net_profit)::numeric,2) FROM batch_vehicle_daily WHERE sim_date='...';
SELECT run_id, expense_file_version, status FROM batch_runs WHERE sim_date='...' ORDER BY started_at;
```
*Caption:* "A corrected v2 file makes an already-reconciled day pending again. **Same row
count, different values** — the load is delete-then-insert in one transaction. This is the
capability Kappa would have made expensive: replaying a past day from Kafka needs retention
measured in months."

---

## G. Orchestration

**G1 — The DAG graph.** Airflow → `fleet_daily_reconciliation` → Graph, a successful run.
*Caption:* "Nine tasks. `find_pending_date` short-circuits when no simulated date is pending —
which is the normal outcome of most polls, not a failure."

**G2 — Per-task durations.** Pipeline health → **Batch task durations**.
*Caption:* "Exported by the API from `batch_runs`. Airflow tasks are short-lived processes
Prometheus could never scrape directly, so no Pushgateway is needed."

---

## H. Observability (criterion: Observability, 10 marks)

**H1 — Pipeline health dashboard, whole page.**

**H2 — Alert rules loaded.** <http://localhost:9090/alerts> showing all ten.

**H3 — An alert actually firing.** `make demo-outage`, then Prometheus → Alerts with
`TelemetryNotProduced` red, and Alertmanager (<http://localhost:9093>) showing it grouped.

**H4 — Proof of *delivery*, not just firing.**
```sql
SELECT alertname, severity, status, received_at
FROM alert_notifications ORDER BY received_at DESC LIMIT 10;
```
*Caption:* "Alertmanager posts to the API's webhook, which stores every notification — so
delivery is demonstrable, not merely asserted."

**H5 — Structured logs.**
```bash
make logs s=speed | grep '"event": "micro_batch"' | tail -3
```
*Caption:* "JSON lines with `ts`, `level`, `service`, `stage`, `event`, `msg` and **`sim_time`**.
On a compressed clock, wall-clock time alone tells you nothing about which simulated hour a
line belongs to."

---

## I. Quality (criterion: Code quality, 5 marks)

**I1 — The test suite.** `make test`, final line showing **186 passed**.
*Caption:* "Unit, PySpark and API tests, including the duplicate-deduplication proof and the
two parity tests that pin the Python and Spark implementations of the shared rules together."

**I2 — The smoke test.** `make smoke --full`, final summary.

**I3 — `common/` as the anti-duplication mitigation.** The directory listing plus one of the
parity tests.
*Caption:* "Lambda's known weakness is two codebases that drift. Everything both layers need
lives in `common/`; the two deliberate duplications (validation and haversine, both for
performance) are pinned together by tests."

---

## Suggested figure order in the report

| § | Figure | Criterion |
|---|---|---|
| Architecture | A1, F1 | Architecture decision (20) |
| **Why Lambda** | **F2, F3, F4** | **Architecture decision (20)** |
| Tech stack | A2, B1, G1 | Tech stack (10) |
| Ingestion | A3, A4 | Ingestion (15) |
| Processing | C1, C2, D2 | Processing (15) |
| Storage/serving | B2, C3, E4 | Storage and serving (10) |
| Observability | H1, H3, H4, H5 | Observability (10) |
| Quality | I1, I3 | Code quality (5) |

If space is tight, **F2 + F3 + F4 are the ones that cannot be cut** — they are the only
figures that show the architecture decision being *justified by measurement* rather than
asserted.
