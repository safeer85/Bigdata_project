-- =========================================================================
-- Fleet serving schema (SPEC 9.1).
--
-- Two families of tables live side by side, which is the whole point of a
-- Lambda serving layer:
--   rt_*    written by the SPEED layer. Approximate, seconds old, may be missing
--           events that arrived after the 10-simulated-minute watermark.
--   batch_* written by the BATCH layer. Exact, complete, recomputable, but only
--           available once a simulated day has closed.
-- The API merges them and labels every block with its source.
--
-- Every rt_* table has a PRIMARY KEY on its NATURAL key so that the streaming
-- sinks can upsert. Structured Streaming's foreachBatch is at-least-once: after a
-- restart Spark replays the last micro-batch, and without these keys every figure
-- on the dashboard would double. See common/db.py.
-- =========================================================================

-- -------------------------------------------------------------------------
-- SPEED LAYER TABLES
-- -------------------------------------------------------------------------

-- Latest known state of each vehicle. One row per vehicle, forever.
-- Natural key: vehicle_id.
CREATE TABLE IF NOT EXISTS rt_vehicle_state (
    vehicle_id        TEXT        PRIMARY KEY,
    driver_id         TEXT,
    status            TEXT        NOT NULL,          -- idle | enroute | on_trip | offline
    zone_id           TEXT,
    lat               DOUBLE PRECISION,
    lon               DOUBLE PRECISION,
    speed             DOUBLE PRECISION,
    trip_id           TEXT,
    -- Simulated event time of the last event that updated this row. "Active in the
    -- last 5 simulated minutes" in v_fleet_live is measured against this, NOT
    -- against wall-clock time, because the clock is compressed.
    last_event_time   TIMESTAMPTZ NOT NULL,
    -- When the vehicle most recently entered idle; NULL when it is not idle.
    idle_since        TIMESTAMPTZ,
    idle_sim_minutes  DOUBLE PRECISION DEFAULT 0,
    alert_open        BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rt_vehicle_state_status ON rt_vehicle_state (status);
CREATE INDEX IF NOT EXISTS ix_rt_vehicle_state_zone ON rt_vehicle_state (zone_id);
CREATE INDEX IF NOT EXISTS ix_rt_vehicle_state_last_event ON rt_vehicle_state (last_event_time DESC);

-- Hourly zone aggregates from a 1-simulated-hour tumbling window.
-- Natural key: (zone_id, window_start).
CREATE TABLE IF NOT EXISTS rt_zone_hourly (
    zone_id          TEXT        NOT NULL,
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    sim_date         DATE        NOT NULL,
    sim_hour         SMALLINT    NOT NULL,
    trips_started    INTEGER     NOT NULL DEFAULT 0,
    trips_completed  INTEGER     NOT NULL DEFAULT 0,
    earnings         DOUBLE PRECISION NOT NULL DEFAULT 0,
    -- Ping counts by status are our utilization PROXY. Exact countDistinct is not
    -- supported in streaming aggregations, so "how many distinct vehicles" cannot
    -- be answered here; the ratio of on_trip pings to all pings can.
    pings_idle       INTEGER     NOT NULL DEFAULT 0,
    pings_enroute    INTEGER     NOT NULL DEFAULT 0,
    pings_on_trip    INTEGER     NOT NULL DEFAULT 0,
    pings_total      INTEGER     NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (zone_id, window_start)
);
CREATE INDEX IF NOT EXISTS ix_rt_zone_hourly_date ON rt_zone_hourly (sim_date, sim_hour);

-- Running per-vehicle totals for the CURRENT simulated day.
-- Natural key: (vehicle_id, sim_date).
-- The API merges this with batch_vehicle_daily; compute_drift compares the two.
CREATE TABLE IF NOT EXISTS rt_vehicle_daily (
    vehicle_id   TEXT        NOT NULL,
    sim_date     DATE        NOT NULL,
    trips        INTEGER     NOT NULL DEFAULT 0,
    revenue      DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_date)
);
CREATE INDEX IF NOT EXISTS ix_rt_vehicle_daily_date ON rt_vehicle_daily (sim_date);

-- Idle alerts (SPEC 7.3). One OPEN alert per vehicle at a time; closed alerts are
-- kept for history, so the key includes opened_at.
CREATE TABLE IF NOT EXISTS idle_alerts (
    vehicle_id       TEXT        NOT NULL,
    opened_at        TIMESTAMPTZ NOT NULL,           -- simulated time
    closed_at        TIMESTAMPTZ,                    -- NULL while open
    zone_id          TEXT,
    idle_sim_minutes DOUBLE PRECISION NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'open',   -- open | closed
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, opened_at)
);
CREATE INDEX IF NOT EXISTS ix_idle_alerts_status ON idle_alerts (status, opened_at DESC);

-- -------------------------------------------------------------------------
-- BATCH LAYER TABLES
-- -------------------------------------------------------------------------

-- The authoritative per-vehicle-day reconciliation.
-- load_batch_views deletes and reinserts a whole sim_date in one transaction, so
-- reruns (e.g. after a v2 expense file) are idempotent.
CREATE TABLE IF NOT EXISTS batch_vehicle_daily (
    vehicle_id             TEXT    NOT NULL,
    sim_date               DATE    NOT NULL,
    -- telemetry-derived
    trips                  INTEGER NOT NULL DEFAULT 0,
    revenue                DOUBLE PRECISION NOT NULL DEFAULT 0,
    gps_km                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    online_min             DOUBLE PRECISION NOT NULL DEFAULT 0,
    on_trip_min            DOUBLE PRECISION NOT NULL DEFAULT 0,
    idle_min               DOUBLE PRECISION NOT NULL DEFAULT 0,
    utilization            DOUBLE PRECISION NOT NULL DEFAULT 0,
    -- expense-derived
    fuel_cost              DOUBLE PRECISION NOT NULL DEFAULT 0,
    maintenance_cost       DOUBLE PRECISION NOT NULL DEFAULT 0,
    distance_covered       DOUBLE PRECISION NOT NULL DEFAULT 0,
    -- reconciled economics
    cost                   DOUBLE PRECISION NOT NULL DEFAULT 0,
    net_profit             DOUBLE PRECISION NOT NULL DEFAULT 0,
    margin                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    revenue_per_km         DOUBLE PRECISION NOT NULL DEFAULT 0,
    cost_per_km            DOUBLE PRECISION NOT NULL DEFAULT 0,
    distance_mismatch_pct  DOUBLE PRECISION NOT NULL DEFAULT 0,
    -- flags
    unprofitable           BOOLEAN NOT NULL DEFAULT FALSE,
    low_margin             BOOLEAN NOT NULL DEFAULT FALSE,
    becoming_unprofitable  BOOLEAN NOT NULL DEFAULT FALSE,
    distance_mismatch      BOOLEAN NOT NULL DEFAULT FALSE,
    in_service             BOOLEAN NOT NULL DEFAULT FALSE,
    missing_costs          BOOLEAN NOT NULL DEFAULT FALSE,
    missing_telemetry      BOOLEAN NOT NULL DEFAULT FALSE,
    -- provenance: which expense file version produced this row
    expense_file_version   INTEGER NOT NULL DEFAULT 1,
    run_id                 TEXT,
    computed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_date)
);
CREATE INDEX IF NOT EXISTS ix_batch_vehicle_daily_date ON batch_vehicle_daily (sim_date DESC);
CREATE INDEX IF NOT EXISTS ix_batch_vehicle_daily_unprofitable
    ON batch_vehicle_daily (sim_date DESC) WHERE unprofitable OR becoming_unprofitable;

-- One row per DAG run. Drives "which date is pending", the late-file flag and the
-- batch metrics the API exports.
CREATE TABLE IF NOT EXISTS batch_runs (
    run_id               TEXT        PRIMARY KEY,
    sim_date             DATE        NOT NULL,
    expense_file_version INTEGER     NOT NULL DEFAULT 0,
    status               TEXT        NOT NULL,       -- running | success | failed
    expense_file_late    BOOLEAN     NOT NULL DEFAULT FALSE,
    quarantined_rows     INTEGER     NOT NULL DEFAULT 0,
    clean_rows           INTEGER     NOT NULL DEFAULT 0,
    telemetry_rows       INTEGER     NOT NULL DEFAULT 0,
    vehicles_out         INTEGER     NOT NULL DEFAULT 0,
    task_durations       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at          TIMESTAMPTZ,
    notes                TEXT
);
CREATE INDEX IF NOT EXISTS ix_batch_runs_date ON batch_runs (sim_date DESC, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_batch_runs_status ON batch_runs (status, finished_at DESC);

-- Quarantined expense rows (SPEC 8.2 task 3), with a reason code from
-- common/validation.py so speed-layer DLQ reasons and batch reasons share a vocabulary.
CREATE TABLE IF NOT EXISTS dq_issues (
    id          BIGSERIAL   PRIMARY KEY,
    run_id      TEXT,
    sim_date    DATE        NOT NULL,
    source      TEXT        NOT NULL,      -- expenses | telemetry
    vehicle_id  TEXT,
    reason      TEXT        NOT NULL,
    payload     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_issues_date ON dq_issues (sim_date DESC, reason);

-- The consistency evidence for the report: fleet revenue as the speed layer saw it
-- vs as the batch layer computed it. Non-zero drift is EXPECTED, because ~2% of
-- events are injected late and some fall outside the speed layer's watermark.
CREATE TABLE IF NOT EXISTS speed_batch_drift (
    sim_date       DATE        PRIMARY KEY,
    speed_revenue  DOUBLE PRECISION NOT NULL,
    batch_revenue  DOUBLE PRECISION NOT NULL,
    drift_abs      DOUBLE PRECISION NOT NULL,
    drift_ratio    DOUBLE PRECISION NOT NULL,
    speed_trips    INTEGER     NOT NULL DEFAULT 0,
    batch_trips    INTEGER     NOT NULL DEFAULT 0,
    run_id         TEXT,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------------------
-- OBSERVABILITY
-- -------------------------------------------------------------------------

-- Alertmanager posts here via the API's /alerts/webhook, so alert delivery is
-- visible in the demo without opening Alertmanager's own UI.
CREATE TABLE IF NOT EXISTS alert_notifications (
    id           BIGSERIAL   PRIMARY KEY,
    fingerprint  TEXT,
    alertname    TEXT        NOT NULL,
    severity     TEXT,
    status       TEXT,                     -- firing | resolved
    summary      TEXT,
    description  TEXT,
    starts_at    TIMESTAMPTZ,
    ends_at      TIMESTAMPTZ,
    labels       JSONB,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_alert_notifications_recent
    ON alert_notifications (received_at DESC);
CREATE INDEX IF NOT EXISTS ix_alert_notifications_name
    ON alert_notifications (alertname, status);
