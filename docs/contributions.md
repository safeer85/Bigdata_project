# Statement of individual contributions

Three members. The project was split by **pipeline stage**, so that each member owned one
end-to-end vertical slice — its source code, its schema objects, its tests and its section of
the report — rather than everyone touching everything. The split is listed below with the
technical decisions each member is responsible for defending in the viva.

| | Member | Reg. no. | Area owned | Primary directories |
|---|---|---|---|---|
| **M1** | Ahamed N.F.F. | EG/2021/4390 | Ingestion and the data contract | `simulators/`, Kafka setup, `common/` schema + clock modules |
| **M2** | Safeer S.M. | EG/2021/4771 | Stream processing and serving | `streaming/`, `api/`, real-time tables |
| **M3** | Nafla M.N.P. | EG/2021/4687 | Batch, orchestration and observability | `batch/`, `airflow/`, `observability/` |

---

## M1 — Ahamed N.F.F. (EG/2021/4390) — Ingestion and the data contract

**Owned:** `simulators/telemetry/` (fleet state machine, Kafka producer, fault injection),
`simulators/expenses/` (daily CSV dropper and generator), `simulators/control.py`,
`scripts/create_topics.sh`, `scripts/simclock_init.py`, and in `common/`: `simclock.py`,
`schemas.py`, `validation.py`, `geo.py`, `earnings.py`.

**Tests written:** `test_simulator.py`, `test_vehicle_state.py` (simulator side),
`test_validation.py`, `test_geo.py`, `test_simclock.py`.

**Decisions to defend:**

- **`fleet.telemetry` has 6 partitions keyed by `vehicle_id`.** This is a *correctness*
  decision, not a throughput one. Kafka guarantees ordering only within a partition, and the
  downstream per-vehicle idle state machine needs each vehicle's events in order; any other
  key would let a vehicle's `trip_end` overtake its own pings.
- **The simulated clock is 60×** — one real second is one simulated minute, so a simulated day
  completes in 24 real minutes. The clock is derived from a persisted start instant rather
  than a counter, so every service agrees on `sim_time` without coordinating.
- **Faults are injected deliberately**, at roughly 0.5% malformed, 0.5% duplicate, 1% missing
  field and 2% late events. The 2% late rate is what makes the speed-vs-batch drift in report
  §6 measurable rather than theoretical.
- **The expense file SLA applies to the first delivery, not to corrections.** A corrected `v2`
  file arriving days later is normal partner behaviour, not a late-file incident — this was
  fixed after an early version raised false alerts on every resubmission.
- **Validation rejects rather than repairs.** Bad rows go to the dead-letter topic with a
  reason, because silently coercing a malformed field produces confidently wrong money figures
  downstream.

---

## M2 — Safeer S.M. (EG/2021/4771) — Stream processing and serving

**Owned:** `streaming/` (`archiver.py`, `speed.py`, `state.py`, `sinks.py`, `listener.py`,
`watchdog.py`), `api/` (`app.py`, `queries.py`, `models.py`, `batch_metrics.py`,
`main.py`), the real-time tables and views in `db/init/`, and `common/db.py` (upsert helpers).

**Tests written:** `test_spark_speed.py`, `test_api.py`, `test_vehicle_state.py` (state-machine
side).

**Decisions to defend:**

- **The archiver is deliberately boring.** It writes the raw event stream to the lake with no
  transformation beyond partitioning. The master dataset must stay immutable and
  reinterpretable, so any cleaning done here would bake today's assumptions into tomorrow's
  recomputation.
- **Idle alerts use `applyInPandasWithState`, not a window.** "Idle for N minutes" is a
  property of a vehicle's history carried across micro-batches, not of a fixed time bucket; a
  windowed aggregate would miss a vehicle idle across a window boundary. The state timeout is
  what expires a vehicle that stops reporting entirely.
- **The watermark knowingly drops late rows**, counted in `fleet_late_rows_dropped_total`. The
  speed layer is allowed to be approximate; the point is that it is approximate *by a measured
  amount* rather than an unknown one.
- **Every sink is idempotent**, keyed so that a replayed micro-batch overwrites rather than
  double-counts. Structured Streaming's at-least-once delivery makes this mandatory, not
  optional.
- **`GET /vehicles/{id}` labels its sources and never blends them.** `live` and `today` are
  `"source": "speed"`, `history` is `"source": "batch"`, each with its own `as_of`. This is the
  Lambda merge made explicit in the serving layer — averaging the two would produce a number
  that is neither fast nor exact.
- **The watchdog exists because a healthcheck lied.** An early archiver process stayed green
  while its stream had died; the watchdog now asserts on progress, not on liveness.

---

## M3 — Nafla M.N.P. (EG/2021/4687) — Batch, orchestration and observability

**Owned:** `batch/` (`runs.py`, `expenses.py`, `vehicle_day.py`, `reconcile.py`, `report.py`),
`airflow/dags/fleet_daily_reconciliation.py`, `docker/airflow/`, all of `observability/`
(Prometheus config and 10 alert rules, Alertmanager, both Grafana dashboards), and in
`common/`: `profitability.py`, `metrics.py`, `logging.py`.

**Tests written:** `test_spark_batch.py`, `test_batch_scheduling.py`,
`test_earnings_profitability.py`.

**Decisions to defend:**

- **The DAG schedule is a poll, not a cron.** Airflow's scheduler lives in real time and knows
  nothing about the compressed clock, so the DAG runs every 2 real minutes and the *first task*
  decides which simulated day is actually pending. Expressing the schedule in simulated time
  was tried and produced an interval 60× longer than intended.
- **Recompute is delete-then-insert on `sim_date`.** Rerunning a day produces the same row
  count with updated values — this is the property that justifies Lambda over Kappa, since
  replaying a past day from Kafka would require months of retention.
- **The batch layer trusts nothing.** It re-validates the archive and recomputes distance from
  raw GPS rather than reusing the speed layer's enrichment, so a bug in the speed layer cannot
  propagate into the financial figures.
- **Batch metrics are pushed, not scraped.** Airflow tasks are short-lived processes that would
  be dead before Prometheus's next scrape, so `api/batch_metrics.py` holds their last known
  values.
- **10 alert rules with inhibition.** Alerts cover both data-flow failures (no telemetry, DLQ
  rate, stream stalled, lag) and business failures (expense file late, batch run failed,
  speed-batch drift). Inhibition stops one root cause raising six pages.

---

## Deliberately shared: the `common/` package

`common/` is the project's answer to Lambda's central weakness — two codebases drifting apart.
Zone mapping, distance, fare, validation and profitability logic exist **once** and are imported
by the simulator, the speed layer, the batch layer and the API.

Each module has a primary author, listed above, but the rule the team worked to was: **no
business rule is defined twice, and a change to a `common/` module is reviewed by whichever
members' layers import it.** The two places where a formula *is* duplicated for performance
reasons are pinned together by tests that push identical inputs through both implementations.

This is why every member can explain modules they did not write: they had to, because their own
layer depends on them.

## Worked on jointly

- `docs/SPEC.md` — written together before any code; the architecture decisions in §2 were
  agreed by all three and treated as fixed thereafter.
- `docker-compose.yml`, `Makefile` / `make.ps1`, `common/config.py` — the stack contract, since
  every member's service is defined in it.
- `scripts/smoke_test.py`, `scripts/wait_for_stack.py` — each member contributed the checks for
  their own layer.
- `docs/report/` — each member drafted the chapters covering their area; the architecture
  argument (Ch. 3), the consistency analysis (§5.2) and the conclusion were written jointly.
- Acceptance runs — the fresh-clone run, the multi-day reconciliation and the alert-firing
  verification were done together, because each needs all three layers alive at once.

## Effort

Roughly even across the three members. The slices were sized by expected difficulty rather than
line count: M1's simulator is the largest single body of code but the most self-contained, M2's
streaming layer carries the hardest concurrency and state semantics, and M3's slice spans three
systems (Spark batch, Airflow and the monitoring stack) that each had to be wired to the others.
