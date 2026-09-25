"""Capture sample API responses and a sample report into docs/report-assets/.

Run against a stack that has reconciled at least one simulated day:

    python scripts/capture_report_assets.py

The point is that the report's appendix shows REAL output from a run, not
hand-written examples that may have drifted from what the code actually returns.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

OUT_DIR = os.path.join("docs", "report-assets")


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


def fetch(path: str):
    with urllib.request.urlopen(f"{API}{path}", timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def psql(sql: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "fleet", "-d", "fleet", "-tAc", sql],
        capture_output=True, text=True,
    )
    return result.stdout.strip()


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)

    sim_date = psql("SELECT max(sim_date)::text FROM batch_vehicle_daily")
    if not sim_date:
        print("No simulated day has been reconciled yet. A day takes 24 real "
              "minutes; wait for the batch layer and try again.")
        return 1

    vehicle = psql(
        "SELECT vehicle_id FROM batch_vehicle_daily "
        f"WHERE sim_date = '{sim_date}' ORDER BY net_profit LIMIT 1"
    ) or "V001"

    captures = {
        "health": "/health",
        "fleet-live": "/fleet/live",
        "zones-live": "/zones/live",
        "idle-alerts": "/alerts/idle?status=open",
        # The Lambda merge, captured for the WORST vehicle so the sample is
        # actually interesting rather than a row of zeros.
        "vehicle-lambda-merge": f"/vehicles/{vehicle}",
        "unprofitable": f"/vehicles/unprofitable?sim_date={sim_date}",
        "pipeline-status": "/pipeline/status",
    }

    bundle = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "sim_date": sim_date,
        "sample_vehicle": vehicle,
        "responses": {},
    }

    for name, path in captures.items():
        try:
            body = fetch(path)
            bundle["responses"][name] = {"request": f"GET {path}", "response": body}
            print(f"  captured  {path}")
        except urllib.error.HTTPError as exc:
            print(f"  skipped   {path} ({exc.code})")
        except Exception as exc:  # noqa: BLE001
            print(f"  FAILED    {path}: {exc}")

    out = os.path.join(OUT_DIR, "sample-api-responses.json")
    with open(out, "w", encoding="utf-8") as handle:
        json.dump(bundle, handle, indent=2, default=str)
    print(f"\nwrote {out}")

    # Copy the generated HTML report out of the container volume.
    report_src = f"/reports/profitability_{sim_date}.html"
    report_dst = os.path.join(OUT_DIR, f"sample-report-{sim_date}.html")
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "api", "cat", report_src],
        capture_output=True, text=True,
    )
    if result.returncode == 0 and result.stdout:
        with open(report_dst, "w", encoding="utf-8") as handle:
            handle.write(result.stdout)
        print(f"wrote {report_dst}")
    else:
        print(f"could not copy {report_src}")

    # A few SQL snapshots that the report quotes directly.
    queries = {
        "drift": "SELECT sim_date, speed_revenue, batch_revenue, drift_abs, "
                 "drift_ratio, speed_trips, batch_trips FROM speed_batch_drift "
                 "ORDER BY sim_date",
        "batch_runs": "SELECT run_id, sim_date, expense_file_version, status, "
                      "expense_file_late, quarantined_rows, clean_rows, vehicles_out "
                      "FROM batch_runs ORDER BY started_at DESC LIMIT 10",
        "dq_issues": "SELECT sim_date, reason, count(*) FROM dq_issues "
                     "GROUP BY sim_date, reason ORDER BY sim_date DESC, 3 DESC",
        "flags": "SELECT sim_date, count(*) FILTER (WHERE unprofitable) unprofitable, "
                 "count(*) FILTER (WHERE becoming_unprofitable) becoming, "
                 "count(*) FILTER (WHERE low_margin) low_margin, "
                 "count(*) FILTER (WHERE distance_mismatch) distance_mismatch, "
                 "count(*) FILTER (WHERE in_service) in_service "
                 "FROM batch_vehicle_daily GROUP BY sim_date ORDER BY sim_date",
    }
    lines = [f"-- captured {bundle['captured_at']}", ""]
    for name, sql in queries.items():
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "postgres",
             "psql", "-U", "fleet", "-d", "fleet", "-c", sql],
            capture_output=True, text=True,
        )
        lines += [f"-- {name}", f"-- {sql}", result.stdout, ""]

    sql_out = os.path.join(OUT_DIR, "sample-sql-snapshots.txt")
    with open(sql_out, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
    print(f"wrote {sql_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
