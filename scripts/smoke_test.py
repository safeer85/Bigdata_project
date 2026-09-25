"""End-to-end smoke test against the RUNNING stack (SPEC 11).

Unlike the pytest suite, this asserts nothing about code -- it asserts that the
deployed pipeline is actually moving data. Run it after `make up`:

    make smoke                  # the checks that pass within a few minutes
    make smoke -- --full        # also waits for a simulated day to close

Exit code 0 means every check passed. Standard library only, so it runs from a
fresh clone with no pip install.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Callable, List, Optional, Tuple

# Read from .env so the script follows any host-port overrides.
def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value:
        return value
    try:
        with open(".env", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line.startswith(f"{name}="):
                    return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return default


API = f"http://localhost:{_env('API_HOST_PORT', '8000')}"
PROM = f"http://localhost:{_env('PROMETHEUS_HOST_PORT', '9090')}"
ALERTMANAGER = f"http://localhost:{_env('ALERTMANAGER_HOST_PORT', '9093')}"
TELEMETRY_METRICS = f"http://localhost:{_env('TELEMETRY_METRICS_HOST_PORT', '8001')}"
EXPENSE_METRICS = f"http://localhost:{_env('EXPENSE_METRICS_HOST_PORT', '8002')}"
SPEED_METRICS = f"http://localhost:{_env('SPEED_METRICS_HOST_PORT', '8003')}"
ARCHIVER_METRICS = f"http://localhost:{_env('ARCHIVER_METRICS_HOST_PORT', '8004')}"
GRAFANA = f"http://localhost:{_env('GRAFANA_HOST_PORT', '3000')}"

PASS, FAIL = "PASS", "FAIL"
results: List[Tuple[str, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> bool:
    results.append((PASS if ok else FAIL, name, detail))
    marker = "  ok  " if ok else " FAIL "
    print(f"[{marker}] {name}" + (f"  -- {detail}" if detail else ""))
    return ok


def get_json(url: str, timeout: int = 15):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def get_text(url: str, timeout: int = 15) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def psql(sql: str) -> str:
    """Run a query inside the postgres container and return the raw value."""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "fleet", "-d", "fleet", "-tAc", sql],
        capture_output=True, text=True,
    )
    return result.stdout.strip()


def wait_for(check: Callable[[], bool], timeout_s: int, label: str,
             poll_s: int = 5) -> bool:
    """Poll until `check` passes. Streaming is asynchronous, so nothing here can
    be asserted immediately after `make up` returns."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if check():
                return True
        except Exception:  # noqa: BLE001 - the stack may still be warming up
            pass
        remaining = int(deadline - time.time())
        print(f"       waiting for {label} ({remaining}s left)", end="\r")
        time.sleep(poll_s)
    print(" " * 70, end="\r")
    return False


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_endpoints_up() -> None:
    for name, url in [
        ("API /health", f"{API}/health"),
        ("Prometheus", f"{PROM}/-/healthy"),
        ("Alertmanager", f"{ALERTMANAGER}/-/healthy"),
        ("Grafana", f"{GRAFANA}/api/health"),
    ]:
        try:
            urllib.request.urlopen(url, timeout=10)
            record(f"{name} reachable", True)
        except Exception as exc:  # noqa: BLE001
            record(f"{name} reachable", False, str(exc))

    for name, url in [
        ("telemetry-sim", TELEMETRY_METRICS),
        ("expense-sim", EXPENSE_METRICS),
        ("speed layer", SPEED_METRICS),
        ("archiver", ARCHIVER_METRICS),
    ]:
        try:
            body = get_text(f"{url}/metrics")
            record(f"{name} metrics served", "fleet_" in body)
        except Exception as exc:  # noqa: BLE001
            record(f"{name} metrics served", False, str(exc))


def check_events_in_kafka() -> None:
    """Events are being produced, and the fault rates look right."""
    try:
        body = get_text(f"{TELEMETRY_METRICS}/metrics")
    except Exception as exc:  # noqa: BLE001
        record("telemetry events produced", False, str(exc))
        return

    produced = 0.0
    faults = {}
    for line in body.splitlines():
        if line.startswith("fleet_events_produced_total{"):
            produced += float(line.rsplit(" ", 1)[1])
        elif line.startswith("fleet_faults_injected_total{"):
            kind = line.split('kind="')[1].split('"')[0]
            faults[kind] = float(line.rsplit(" ", 1)[1])

    record("telemetry events produced", produced > 0, f"{int(produced)} events")

    if produced > 500:
        for kind, expected in [("malformed", 0.005), ("schema_invalid", 0.005),
                               ("duplicate", 0.01), ("late", 0.02)]:
            rate = faults.get(kind, 0) / produced
            # Generous band: these are random draws, not quotas.
            ok = expected * 0.3 <= rate <= expected * 3.0
            record(f"fault rate {kind} ~= {expected:.1%}", ok, f"observed {rate:.2%}")


def check_rt_tables() -> None:
    """The speed layer is writing all three rt_* tables."""
    for table in ("rt_vehicle_state", "rt_zone_hourly", "rt_vehicle_daily"):
        ok = wait_for(
            lambda t=table: int(psql(f"SELECT count(*) FROM {t}") or 0) > 0,
            timeout_s=180, label=table,
        )
        count = psql(f"SELECT count(*) FROM {table}")
        record(f"{table} populated", ok, f"{count} rows")


def check_fleet_live() -> None:
    try:
        body = get_json(f"{API}/fleet/live")
    except Exception as exc:  # noqa: BLE001
        record("/fleet/live returns data", False, str(exc))
        return
    record("/fleet/live returns data", body.get("active_vehicles", 0) > 0,
           f"{body.get('active_vehicles')} active, idle ratio {body.get('idle_ratio')}")
    record("/fleet/live is labelled source=speed", body.get("source") == "speed")


def check_dlq() -> None:
    """Invalid events reach the dead-letter topic with reason codes."""
    ok = wait_for(
        lambda: "fleet_dlq_events_total{" in get_text(f"{SPEED_METRICS}/metrics"),
        timeout_s=120, label="DLQ events",
    )
    reasons = set()
    if ok:
        for line in get_text(f"{SPEED_METRICS}/metrics").splitlines():
            if line.startswith("fleet_dlq_events_total{"):
                reasons.add(line.split('reason="')[1].split('"')[0])
    record("invalid events reach the DLQ", ok, f"reasons: {sorted(reasons)}")


def check_watermark_drops() -> None:
    """Non-zero watermark drops are EXPECTED and are the consistency evidence."""
    ok = wait_for(
        lambda: "fleet_late_rows_dropped_total{" in get_text(f"{SPEED_METRICS}/metrics"),
        timeout_s=180, label="watermark drops",
    )
    record("speed layer drops some late rows (expected by design)", ok,
           "this is the evidence the batch layer is the authoritative one")


def check_parquet_partitions() -> None:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "spark-master",
         "ls", "/lake/raw/telemetry/"],
        capture_output=True, text=True,
    )
    partitions = [p for p in result.stdout.split() if p.startswith("sim_date=")]
    record("raw Parquet partitions exist", bool(partitions),
           f"{len(partitions)} partitions: {partitions[:3]}")


def check_lambda_merge() -> None:
    """/vehicles/{id} returns both layers, each labelled."""
    try:
        vehicle = psql("SELECT vehicle_id FROM rt_vehicle_state LIMIT 1")
        if not vehicle:
            record("Lambda merge on /vehicles/{id}", False, "no vehicles yet")
            return
        body = get_json(f"{API}/vehicles/{vehicle}")
    except Exception as exc:  # noqa: BLE001
        record("Lambda merge on /vehicles/{id}", False, str(exc))
        return

    ok = (body["live"]["source"] == "speed"
          and body["today"]["source"] == "speed"
          and all(h["source"] == "batch" for h in body["history"]))
    record("Lambda merge labels every block with its source", ok,
           f"{vehicle}: live+today=speed, {len(body['history'])} batch days")


def check_expense_files() -> None:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "expense-sim", "ls", "/landing/expenses/"],
        capture_output=True, text=True,
    )
    files = [f for f in result.stdout.split() if f.endswith(".csv")]
    record("expense files dropped", bool(files), f"{len(files)}: {files[:3]}")


def check_batch_layer(timeout_s: int) -> None:
    """After a simulated day closes: batch rows, a report, and a drift figure."""
    ok = wait_for(
        lambda: int(psql("SELECT count(*) FROM batch_vehicle_daily") or 0) > 0,
        timeout_s=timeout_s, label="batch reconciliation",
        poll_s=15,
    )
    count = psql("SELECT count(*) FROM batch_vehicle_daily")
    if not record("batch_vehicle_daily populated", ok, f"{count} rows"):
        return

    # Anchor every downstream check on the latest date that has a COMPLETED,
    # SUCCESSFUL run -- not simply max(sim_date). `load_batch_views` inserts the
    # rows several tasks before `generate_report` writes the file, so using
    # max(sim_date) raced the DAG and 404'd on a report that was about to exist.
    sim_date = psql(
        "SELECT max(sim_date)::text FROM batch_runs "
        "WHERE status = 'success' AND vehicles_out > 0"
    )
    if not sim_date:
        record("a simulated day has been fully reconciled", False,
               "rows exist but no run has finished yet")
        return

    try:
        urllib.request.urlopen(f"{API}/reports/{sim_date}", timeout=15)
        record("profitability report generated", True, f"/reports/{sim_date}")
    except Exception as exc:  # noqa: BLE001
        record("profitability report generated", False, str(exc))

    flagged = psql(
        "SELECT count(*) FROM batch_vehicle_daily "
        "WHERE unprofitable OR becoming_unprofitable OR low_margin"
    )
    total = int(count or 0)
    n_flagged = int(flagged or 0)
    # A believable fleet: some vehicles flagged, but not most of them. If nearly
    # everything is flagged the cost model is miscalibrated, not the fleet broken.
    record("a plausible minority of vehicles is flagged", 0 < n_flagged < total * 0.6,
           f"{n_flagged}/{total}")

    drift = psql("SELECT round(drift_ratio::numeric, 5) FROM speed_batch_drift "
                 f"WHERE sim_date = '{sim_date}'")
    record("speed/batch drift recorded", drift != "",
           f"drift_ratio={drift} (non-zero is expected)")

    # Look at the last FINISHED run, not simply the last one. The DAG polls every
    # 2 real minutes, so a run is often in flight when the smoke test looks -- and
    # "status=running" is a healthy pipeline, not a failure. Asserting on the most
    # recent row regardless of state made this check fail intermittently.
    # Check the newest run FOR THE RECONCILED DATE, not the newest run overall.
    # Two things otherwise make this flap without anything being wrong: a run is
    # often in flight (the DAG polls every 2 real minutes), and `make demo-late-file`
    # deliberately records a FAILED marker row for a day whose file never arrived,
    # which is then superseded by a successful run once the partner delivers.
    status = psql(
        "SELECT status FROM batch_runs WHERE sim_date = '" + sim_date + "' "
        "ORDER BY started_at DESC LIMIT 1"
    )
    in_flight = psql("SELECT count(*) FROM batch_runs WHERE status = 'running'")
    detail = f"{sim_date} status={status}"
    if in_flight not in ("", "0"):
        detail += f" ({in_flight} other run(s) in flight, which is normal)"
    record("the latest reconciled day ran successfully", status == "success", detail)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test the running stack")
    parser.add_argument(
        "--full", action="store_true",
        help="also wait for a simulated day to close and be reconciled "
             "(up to ~30 real minutes)",
    )
    args = parser.parse_args()

    print("=" * 72)
    print("Fleet pipeline smoke test")
    print("=" * 72)

    print("\n--- services ---")
    check_endpoints_up()

    print("\n--- ingestion ---")
    check_events_in_kafka()
    check_expense_files()

    print("\n--- speed layer ---")
    check_rt_tables()
    check_fleet_live()
    check_dlq()
    check_watermark_drops()
    check_parquet_partitions()

    print("\n--- serving ---")
    check_lambda_merge()

    print("\n--- batch layer ---")
    # A simulated day is 24 real minutes; the DAG polls every 2.
    check_batch_layer(timeout_s=2400 if args.full else 60)

    failures = [r for r in results if r[0] == FAIL]
    print("\n" + "=" * 72)
    print(f"{len(results) - len(failures)} passed, {len(failures)} failed")
    if failures:
        print("\nFailed checks:")
        for _, name, detail in failures:
            print(f"  - {name}: {detail}")
        if not args.full:
            print("\nNote: batch-layer checks need a closed simulated day "
                  "(~24 real minutes). Re-run with --full to wait for one.")
    print("=" * 72)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
