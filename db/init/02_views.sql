-- =========================================================================
-- Serving views (SPEC 9.1).
--
-- A note on "now". These views must answer "right now" in SIMULATED time, but
-- PostgreSQL only knows wall-clock time and the two run at different speeds. The
-- reference point used throughout is therefore
--       (SELECT max(last_event_time) FROM rt_vehicle_state)
-- i.e. the latest simulated event time the pipeline has actually seen. That is
-- both self-consistent and honest: if ingestion stalls, "now" stops advancing and
-- the active-vehicle count correctly falls to zero instead of silently lying.
-- =========================================================================

-- Active vehicles, idle ratio and on-trip count for the whole fleet.
CREATE OR REPLACE VIEW v_fleet_live AS
WITH sim_now AS (
    SELECT COALESCE(max(last_event_time), to_timestamp(0)) AS ts
    FROM rt_vehicle_state
),
active AS (
    SELECT s.*
    FROM rt_vehicle_state s, sim_now n
    -- 5 SIMULATED minutes. Event timestamps are simulated time, so this interval
    -- is written in simulated minutes directly.
    WHERE s.last_event_time >= n.ts - INTERVAL '5 minutes'
      AND s.status <> 'offline'
)
SELECT
    (SELECT ts FROM sim_now)                                        AS as_of,
    count(*)                                                        AS active_vehicles,
    count(*) FILTER (WHERE status = 'idle')                         AS idle_vehicles,
    count(*) FILTER (WHERE status = 'on_trip')                      AS on_trip_vehicles,
    count(*) FILTER (WHERE status = 'enroute')                      AS enroute_vehicles,
    -- NULLIF guards the empty-fleet case: 0/0 would raise, 0/NULL gives NULL,
    -- and COALESCE turns that into an honest 0.
    COALESCE(round((count(*) FILTER (WHERE status = 'idle'))::numeric
             / NULLIF(count(*), 0), 4), 0)                          AS idle_ratio,
    count(*) FILTER (WHERE alert_open)                              AS open_idle_alerts
FROM active;

-- Per-zone live picture: the same idea, grouped by zone.
CREATE OR REPLACE VIEW v_zone_live AS
WITH sim_now AS (
    SELECT COALESCE(max(last_event_time), to_timestamp(0)) AS ts
    FROM rt_vehicle_state
),
active AS (
    SELECT s.*
    FROM rt_vehicle_state s, sim_now n
    WHERE s.last_event_time >= n.ts - INTERVAL '5 minutes'
      AND s.status <> 'offline'
      AND s.zone_id IS NOT NULL
),
-- The current simulated hour's zone aggregate, taken from the speed layer's
-- tumbling-window table rather than recomputed here.
current_hour AS (
    SELECT z.zone_id, z.trips_started, z.trips_completed, z.earnings
    FROM rt_zone_hourly z, sim_now n
    WHERE z.window_start = date_trunc('hour', n.ts)
)
SELECT
    (SELECT ts FROM sim_now)                                AS as_of,
    a.zone_id,
    count(*)                                                AS active_vehicles,
    count(*) FILTER (WHERE a.status = 'idle')               AS idle_vehicles,
    count(*) FILTER (WHERE a.status = 'on_trip')            AS on_trip_vehicles,
    COALESCE(round((count(*) FILTER (WHERE a.status = 'idle'))::numeric
             / NULLIF(count(*), 0), 4), 0)                  AS idle_ratio,
    COALESCE(max(c.trips_started), 0)                       AS trips_started_this_hour,
    COALESCE(max(c.trips_completed), 0)                     AS trips_this_hour,
    COALESCE(max(c.earnings), 0)                            AS earnings_this_hour
FROM active a
LEFT JOIN current_hour c ON c.zone_id = a.zone_id
GROUP BY a.zone_id
ORDER BY a.zone_id;

-- Convenience view for the Grafana geomap: one row per vehicle with its position
-- and status, restricted to vehicles seen recently.
CREATE OR REPLACE VIEW v_vehicle_map AS
WITH sim_now AS (
    SELECT COALESCE(max(last_event_time), to_timestamp(0)) AS ts
    FROM rt_vehicle_state
)
SELECT
    s.vehicle_id, s.driver_id, s.status, s.zone_id,
    s.lat, s.lon, s.speed, s.trip_id,
    s.idle_sim_minutes, s.alert_open, s.last_event_time
FROM rt_vehicle_state s, sim_now n
WHERE s.last_event_time >= n.ts - INTERVAL '10 minutes';

-- Latest reconciled day, used as the default for /vehicles/unprofitable.
CREATE OR REPLACE VIEW v_latest_batch_date AS
SELECT max(sim_date) AS sim_date FROM batch_vehicle_daily;
