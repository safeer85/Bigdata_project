# Demo recording script

A shot-by-shot script for a **7-minute screen recording**.

The written demo in `README.md` assumes a live audience who will wait. A video cannot: the two
most impressive moments both have real waits baked into them.

| Action | Wait before anything visible happens |
|---|---|
| `make demo-idle` | **45 real seconds** (= 45 simulated minutes of idling) |
| `make demo-resubmit` | **up to 2 real minutes** (the DAG polls on a 2-minute schedule) |
| A simulated day closing | 24 real minutes |

The script below solves this by **triggering both slow things in the first 60 seconds**, then
talking about architecture while they cook, and cutting back to them at the reveal. Nothing is
faked and nothing is edited out of sequence — you simply start the clock early.

---

## Before you hit record

**1. Do a clean run.** Do not record over a stack you have been experimenting on; deliberate
outage testing leaves artefacts in the data that you will have to explain.

```bash
make reset && make up
```

**2. Wait ~75 real minutes and leave it completely alone.** You need three reconciled simulated
days before `becoming_unprofitable` can appear at all — it refuses to infer a trend from two
points. Verify:

```bash
curl -s localhost:8000/pipeline/status | python -m json.tool
```

You want `reconciled_dates` to list **at least 3 dates**. If it lists fewer, keep waiting.

**3. Open these tabs in this order**, so you never fumble for a URL on camera:

| Tab | URL |
|---|---|
| 1 | <http://localhost:8000/docs> (API) |
| 2 | <http://localhost:3000> → Fleet operations |
| 3 | <http://localhost:3000> → Pipeline health |
| 4 | <http://localhost:8088> (Airflow, already logged in) |
| 5 | <http://localhost:9090/alerts> (Prometheus) |

Plus **two terminals**, both already `cd`'d into the project: one for commands, one for `psql`.

> Ports: if you overrode any in `.env`, run `make urls` and use what it prints.

**4. Pre-log-in to Grafana and Airflow.** A login screen on camera wastes 20 seconds and looks
unprepared.

**5. Know your numbers.** Have `docs/report.md` §6 open off-camera. The drift figure is the
single most important thing you will say.

---

## The recording

### 0:00 – 0:25 — Start both slow clocks immediately

Terminal 1, type these two commands straight away and **do not wait for them**:

```bash
make demo-idle
make demo-resubmit
```

> **Say:** "I'm kicking off two things up front because they take real time to play out — a
> vehicle going idle, and a corrected supplier file. I'll come back to both. While they run,
> here's what the system is and why it's built this way."

This is the whole trick. Both reveals will be ready exactly when you need them.

---

### 0:25 – 1:30 — The question and the clock

Tab 1 (API docs) → run `GET /health`.

> **Say:** "The brief asks one question, but it's really two. What's the fleet doing *right
> now* — that needs an answer in seconds and can tolerate being slightly wrong. And which
> vehicles are unprofitable once yesterday's fuel and maintenance land — that's money, so it
> has to be exact, complete, and recomputable when a supplier sends a correction.
>
> Point at `compression: 60` in the response. One real second is one simulated minute. A full
> day passes in 24 real minutes, which is the only reason you can watch a *daily* financial
> reconciliation happen in a seven-minute video."

---

### 1:30 – 2:15 — The live answer (speed layer)

Tab 2 (Fleet operations). Let the geomap move for a beat before talking.

> **Say:** "This is the speed layer. Vehicles coloured by status, live. The zone table shows
> the hot zones earning several times what the cold ones do, and the heatmap shows the morning
> and evening demand peaks."

Tab 1 → `GET /fleet/live`.

> **Say:** "Same numbers through the API — and note it labels itself `source: speed`. That
> matters in a minute."

---

### 2:15 – 3:00 — First reveal: the idle alert

Back to Tab 2, **Open idle alerts** panel. Your `demo-idle` vehicle is now there.

> **Say:** "That's the vehicle I flagged at the start. It's been idle 45 simulated minutes, so
> the pipeline raised an alert.
>
> This one is harder than it looks. You can't answer 'has this been idle for 45 minutes?' with
> a window or a `GROUP BY` — you need memory of when it *stopped* being busy, carried across
> micro-batches, and it has to fire when *no* new event arrives, because a silent vehicle is
> exactly the case you care about. That's Spark's `applyInPandasWithState`, with an
> **event-time** timeout so the rule means the same thing whatever the clock compression is."

---

### 3:00 – 3:45 — The financial answer (batch layer)

Tab 1 → `GET /vehicles/unprofitable`. Then open a generated report:
`http://localhost:8000/reports/<one of your reconciled dates>`

> **Say:** "This is the batch layer — a different question with different guarantees. Per
> vehicle per day: revenue from telemetry, against the supplier's fuel and maintenance.
> Unprofitable, low margin, declining, distance mismatch.
>
> Scroll to the data-quality section. Note the report states its own uncertainty: how many
> supplier rows were quarantined and why, and how far the two layers disagreed. A financial
> report that doesn't tell you how much it trusts itself isn't finished."

---

### 3:45 – 5:15 — **Why there are two layers** ← the marks are here

Give this 90 seconds. Do not rush it.

Tab 1 → `GET /vehicles/{id}` for any vehicle with history. Expand the response fully.

> **Say:** "Three blocks. `live` and `today` say `source: speed`. `history` says `source:
> batch`. Each carries its own `as_of`.
>
> They are deliberately **not** blended into one number. They answer different questions with
> different guarantees, and whoever reads this has to be able to tell at a glance which figures
> they can put in front of finance. Blending them is exactly how a dashboard estimate ends up
> in a financial statement."

Tab 3 (Pipeline health) → **Rows dropped by the watermark**.

> **Say:** "The speed layer has a 10-simulated-minute watermark. The simulator injects events
> up to 20 minutes late — deliberately. So the speed layer *knowingly throws some away*. This
> panel counts them."

Now the drift panel, and Terminal 2:

```sql
SELECT sim_date, speed_revenue, batch_revenue, drift_ratio, speed_trips, batch_trips
FROM speed_batch_drift ORDER BY sim_date;
```

> **Say — this is the most important sentence in the video:** "On this day the two layers
> disagree by a third of a percent. Speed counted 316 trips, batch counted 317 — one late
> trip-end that the watermark dropped and the batch layer recovered.
>
> And on the other days the drift is zero. That's not a bug, and it's worth saying why: only
> about 3% of events are revenue-bearing, so of the handful the watermark drops, you'd expect
> roughly half of one trip per day to matter financially. We measured that rather than tuning
> the simulator to manufacture a scarier number.
>
> The point isn't that the speed layer is badly wrong. It's that its error is **unbounded in
> principle and unknowable in advance** — a watermark is a bet on how late data gets. Which
> brings me to the thing no watermark can ever fix."

---

### 5:15 – 6:15 — Second reveal: recomputation

This is your `demo-resubmit` from 0:00, now finished.

Tab 4 (Airflow) — show the DAG run that picked the date up. Then Terminal 2:

```sql
SELECT run_id, expense_file_version, status, vehicles_out
FROM batch_runs WHERE sim_date = '<the resubmitted date>' ORDER BY started_at;

SELECT count(*), round(sum(net_profit)::numeric, 2)
FROM batch_vehicle_daily WHERE sim_date = '<the resubmitted date>';
```

> **Say:** "At the start I dropped a corrected version-2 supplier file for a day that had
> already been reconciled. The DAG noticed a file version newer than the one it last
> processed, and recomputed that day on its own.
>
> Two rows in `batch_runs` for the same date — v1 and v2. And the reconciled table has **the
> same fifty rows with different values**. Delete-then-insert inside one transaction, so
> rerunning is safe.
>
> This is what Kappa would have made expensive. Replaying a past day from Kafka means keeping
> retention long enough to cover any day you might ever be asked to restate. Our retention is
> seven days. The lake is partitioned by day, so the batch layer reads exactly the one day it
> needs — about nine seconds."

---

### 6:15 – 7:00 — Operations, and close

Tab 5 (Prometheus alerts) or Terminal 2:

```sql
SELECT alertname, severity, status, received_at
FROM alert_notifications ORDER BY received_at DESC LIMIT 6;
```

> **Say:** "Ten alert rules. Alertmanager posts them to the API, which stores every
> notification — so we can show alerts were actually *delivered*, not just that they fired."

Optionally, the one bug worth 20 seconds:

> **Say:** "One thing running it taught us. Restarting a Spark worker left the archiver
> connected but frozen — the query stayed 'active' so `awaitTermination` never returned, the
> container stayed up, and **the healthcheck stayed green** while it silently stopped writing
> the master dataset for 26 minutes. A healthcheck that proves a process is alive proves
> nothing about whether it's doing its job. There's now a watchdog that monitors progress
> instead."

Close on the Fleet operations dashboard:

> **Say:** "Lambda here isn't a fashion choice. It's because a supplier can correct last
> month's costs, and the only honest answer to that is recomputing from an immutable archive.
> We measured what that costs us, and we label every number with which layer it came from."

---

## If something goes wrong on camera

| Symptom | What to do |
|---|---|
| No idle alert after 45s | `python scripts/demo.py idle --vehicle V007` and keep narrating; it needs 45 more seconds. Check `curl "localhost:8000/alerts/idle?status=open"`. |
| `demo-resubmit` says no file yet | No simulated day has closed. You did not wait long enough before recording — stop and wait. |
| DAG hasn't picked up the resubmit | It polls every 2 real minutes. Keep talking; do **not** trigger it manually on camera. |
| Drift is zero on every day | Fine, and it is in the script. Say the arithmetic (§6.2) and pivot to `fleet_late_rows_dropped_total`, which is always non-zero. |
| A panel shows "No data" | Check the dashboard's time range is `now-15m`, not an old absolute window. |
| Grafana asks you to log in | admin / admin. Should have been pre-logged-in. |

---

## What the examiner is listening for

Rehearse until these four land without notes:

1. **Why not Kappa** — supplier corrections arrive on a different channel, days later, and no
   amount of Kafka retention makes replaying them cheap.
2. **Why the drift is non-zero** — and why you did not inflate it.
3. **Why the two layers are labelled and not blended** — so nobody quotes a dashboard estimate
   to finance.
4. **What the healthcheck bug taught you** — liveness is not progress.

If the recording overruns, cut sections 6:15–7:00 first and 1:30–2:15 second.
**Never cut 3:45–5:15.** That is the 20-mark section.
