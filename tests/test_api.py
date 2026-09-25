"""API tests with the FastAPI TestClient (SPEC 11).

The database is stubbed out at the `api.queries` boundary rather than mocked at
the psycopg level. That keeps the tests about the API's CONTRACT -- shapes,
status codes, and above all the source labelling of the Lambda merge -- instead
of about SQL, which the PySpark and integration tests already cover.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402

from api import app as api_app  # noqa: E402
from api import queries  # noqa: E402

TS = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def client(monkeypatch):
    """A TestClient with every database call stubbed."""
    monkeypatch.setattr(queries, "fleet_live", lambda: {
        "as_of": TS, "active_vehicles": 20, "idle_vehicles": 8,
        "on_trip_vehicles": 7, "enroute_vehicles": 5, "idle_ratio": 0.4,
        "open_idle_alerts": 2,
    })
    monkeypatch.setattr(queries, "open_idle_alert_count", lambda: 2)
    monkeypatch.setattr(queries, "trips_in_hour", lambda offset=0: 22 if offset == 0 else 17)
    monkeypatch.setattr(queries, "earnings_by_zone_this_hour", lambda: [
        {"zone_id": "Z07", "earnings": 733.51, "trips": 7},
        {"zone_id": "Z10", "earnings": 379.79, "trips": 4},
    ])
    monkeypatch.setattr(queries, "zones_live", lambda: [{
        "as_of": TS, "zone_id": "Z07", "active_vehicles": 5, "idle_vehicles": 1,
        "on_trip_vehicles": 3, "idle_ratio": 0.2, "trips_this_hour": 7,
        "earnings_this_hour": 733.51,
    }])
    monkeypatch.setattr(queries, "idle_alerts", lambda status, limit=200: [{
        "vehicle_id": "V002", "opened_at": TS, "closed_at": None,
        "zone_id": "Z13", "idle_sim_minutes": 58.0, "status": "open",
    }])
    return TestClient(api_app.app)


# --- health ----------------------------------------------------------------

def test_health_reports_the_simulated_clock(client, monkeypatch):
    monkeypatch.setattr(api_app.db, "query_one", lambda *a, **k: {"ok": 1})
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"
    # The compression factor must be visible: every threshold depends on it.
    assert body["compression"] == 60


def test_health_is_degraded_when_the_database_is_down(client, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(api_app.db, "query_one", boom)
    body = client.get("/health").json()
    assert body["status"] == "degraded"
    assert "connection refused" in body["database"]


# --- speed layer -----------------------------------------------------------

def test_fleet_live_is_labelled_as_a_speed_layer_answer(client):
    body = client.get("/fleet/live").json()
    assert body["source"] == "speed"
    assert body["active_vehicles"] == 20
    assert body["idle_ratio"] == 0.4
    assert body["trips_this_hour"] == 22
    assert body["trips_last_hour"] == 17
    assert body["earnings_by_zone"][0]["zone_id"] == "Z07"


def test_zones_live_returns_one_entry_per_zone(client):
    body = client.get("/zones/live").json()
    assert body["source"] == "speed"
    assert len(body["zones"]) == 1
    assert body["zones"][0]["zone_id"] == "Z07"


def test_idle_alerts_reports_the_threshold_it_used(client):
    """A bare count is not actionable without the threshold behind it."""
    body = client.get("/alerts/idle?status=open").json()
    assert body["source"] == "speed"
    assert body["count"] == 1
    assert body["threshold_sim_minutes"] > 0
    assert body["alerts"][0]["vehicle_id"] == "V002"


def test_zone_hourly_404s_when_there_is_no_data(client, monkeypatch):
    monkeypatch.setattr(queries, "zone_hourly", lambda z, d: ([], d))
    assert client.get("/zones/Z99/hourly").status_code == 404


# --- THE LAMBDA MERGE ------------------------------------------------------

def _stub_vehicle(monkeypatch, live=True, today=True, history=1):
    monkeypatch.setattr(queries, "vehicle_exists", lambda v: True)
    monkeypatch.setattr(queries, "vehicle_live", lambda v: ({
        "vehicle_id": v, "driver_id": "D001", "status": "idle", "zone_id": "Z06",
        "lat": 12.98, "lon": 77.60, "speed": 0.0, "trip_id": None,
        "last_event_time": TS, "idle_since": TS, "idle_sim_minutes": 12.0,
        "alert_open": False,
    } if live else None))
    monkeypatch.setattr(queries, "vehicle_today", lambda v: ({
        "vehicle_id": v, "sim_date": "2024-01-01", "trips": 4, "revenue": 512.25,
    } if today else None))
    monkeypatch.setattr(queries, "vehicle_history", lambda v, d: [{
        "vehicle_id": v, "sim_date": "2023-12-31", "computed_at": TS,
        "trips": 9, "revenue": 1100.0, "gps_km": 120.0, "distance_covered": 122.0,
        "fuel_cost": 380.0, "maintenance_cost": 150.0, "cost": 530.0,
        "net_profit": 570.0, "margin": 0.518, "revenue_per_km": 9.17,
        "cost_per_km": 4.42, "utilization": 0.52, "unprofitable": False,
        "low_margin": False, "becoming_unprofitable": False,
        "distance_mismatch": False, "in_service": False, "missing_costs": False,
        "missing_telemetry": False, "expense_file_version": 1,
    }][:history])


def test_vehicle_detail_labels_every_block_with_its_source(client, monkeypatch):
    """The core Lambda-architecture requirement of SPEC 9.2.

    Live state and today's earnings are SPEED-layer; the history is BATCH-layer.
    A caller must be able to tell which figures are safe to quote to finance.
    """
    _stub_vehicle(monkeypatch)
    body = client.get("/vehicles/V001").json()

    assert body["live"]["source"] == "speed"
    assert body["today"]["source"] == "speed"
    assert body["history"][0]["source"] == "batch"

    # ...and each block says how fresh it is.
    assert body["live"]["as_of"] is not None
    assert body["history"][0]["as_of"] is not None

    assert body["today"]["revenue"] == 512.25
    assert body["history"][0]["net_profit"] == 570.0


def test_vehicle_detail_works_for_an_offline_vehicle(client, monkeypatch):
    """An off-shift vehicle has no live state but may have a batch history."""
    _stub_vehicle(monkeypatch, live=False, today=False)
    body = client.get("/vehicles/V001").json()
    assert body["live"]["status"] is None
    assert body["today"]["trips"] == 0
    assert len(body["history"]) == 1     # history is still served


def test_unknown_vehicle_is_404(client, monkeypatch):
    monkeypatch.setattr(queries, "vehicle_exists", lambda v: False)
    assert client.get("/vehicles/V999").status_code == 404


def test_unprofitable_route_is_not_swallowed_by_the_vehicle_id_route(client, monkeypatch):
    """/vehicles/unprofitable must not be read as a vehicle called "unprofitable".

    FastAPI matches routes in declaration order, so the static route has to be
    declared before the dynamic one. This test pins that ordering.
    """
    monkeypatch.setattr(queries, "latest_reconciled_date", lambda: "2024-01-01")
    monkeypatch.setattr(queries, "unprofitable", lambda d: [{
        "vehicle_id": "V013", "revenue": 300.0, "cost": 500.0, "net_profit": -200.0,
        "margin": -0.67, "utilization": 0.2, "unprofitable": True,
        "low_margin": False, "becoming_unprofitable": True,
        "distance_mismatch": False, "in_service": False,
        "missing_costs": False, "missing_telemetry": False,
    }])
    response = client.get("/vehicles/unprofitable")
    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "batch"
    assert body["sim_date"] == "2024-01-01"
    assert body["vehicles"][0]["vehicle_id"] == "V013"
    # The reasons must be spelled out, not left as raw booleans.
    assert body["vehicles"][0]["reasons"]


def test_unprofitable_404s_before_any_day_has_been_reconciled(client, monkeypatch):
    monkeypatch.setattr(queries, "latest_reconciled_date", lambda: None)
    response = client.get("/vehicles/unprofitable")
    assert response.status_code == 404
    assert "24 real minutes" in response.json()["detail"]


# --- ops -------------------------------------------------------------------

def test_pipeline_status_reports_lag_in_simulated_minutes(client, monkeypatch):
    from datetime import timedelta

    monkeypatch.setattr(queries, "sim_now", lambda: TS)
    monkeypatch.setattr(api_app, "_sim_now", lambda: TS + timedelta(minutes=7))
    monkeypatch.setattr(queries, "last_batch_run", lambda: {"run_id": "r1", "status": "success"})
    monkeypatch.setattr(queries, "last_successful_run", lambda: {"sim_date": "2024-01-01"})
    monkeypatch.setattr(queries, "latest_drift", lambda: {"drift_ratio": 0.012})
    monkeypatch.setattr(queries, "dq_issue_count", lambda r: 3)
    monkeypatch.setattr(queries, "reconciled_dates", lambda limit=30: ["2024-01-01"])

    body = client.get("/pipeline/status").json()
    assert body["lag_sim_minutes"] == pytest.approx(7.0)
    assert body["drift"]["drift_ratio"] == 0.012
    assert body["open_idle_alerts"] == 2


def test_alert_webhook_stores_each_alert(client, monkeypatch):
    stored = []
    monkeypatch.setattr(queries, "store_alert", lambda a: stored.append(a))

    payload = {
        "alerts": [
            {
                "status": "firing",
                "fingerprint": "abc123",
                "labels": {"alertname": "TelemetryNotProduced", "severity": "critical"},
                "annotations": {"summary": "No telemetry events produced"},
                "startsAt": "2024-01-01T09:00:00Z",
            }
        ]
    }
    body = client.post("/alerts/webhook", json=payload).json()
    assert body == {"received": 1, "stored": 1}
    assert stored[0]["labels"]["alertname"] == "TelemetryNotProduced"


def test_a_webhook_write_failure_still_returns_200(client, monkeypatch):
    """Never NACK an alert over a database error: Alertmanager would retry
    forever and the real alert would be buried."""
    def boom(alert):
        raise RuntimeError("db down")

    monkeypatch.setattr(queries, "store_alert", boom)
    response = client.post("/alerts/webhook", json={"alerts": [{"labels": {}}]})
    assert response.status_code == 200
    assert response.json() == {"received": 1, "stored": 0}


def test_metrics_endpoint_serves_prometheus_text(client, monkeypatch):
    monkeypatch.setattr(api_app.batch_metrics, "refresh", lambda: None)
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "fleet_" in response.text


def test_report_404s_with_a_helpful_message(client):
    response = client.get("/reports/1999-01-01")
    assert response.status_code == 404
    assert "no report for" in response.json()["detail"]


def test_root_lists_the_endpoints_grouped_by_layer(client):
    body = client.get("/").json()
    assert "/vehicles/{vehicle_id}" in body["lambda_merge"]
    assert "/fleet/live" in body["speed_layer"]
