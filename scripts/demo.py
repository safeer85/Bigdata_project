"""Demo driver behind the `make demo-*` targets (SPEC 5.1, 5.2, 10.3).

Runs on the HOST and talks to the simulators' control endpoints over the ports
compose publishes. Standard library only, so it works from a fresh clone with no
`pip install`.

Every target prints what to watch and where, because the point of these commands
is to make one specific pipeline behaviour visible during a 7-minute demo:

    idle       force a vehicle idle      -> idle_alerts row, IdleVehiclesHigh
    outage     pause the producer        -> TelemetryNotProduced, StreamStalled
    resubmit   drop a corrected v2 file  -> batch recompute, same rows/new values
    late-file  hold the next file        -> ExpenseFileLate
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Dict, Optional

TELEMETRY = "http://localhost:8010"
EXPENSES = "http://localhost:8011"
API = "http://localhost:8000"


def call(base: str, path: str, params: Optional[Dict[str, str]] = None) -> Dict:
    """GET a control endpoint and return the parsed JSON."""
    url = f"{base}/{path}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"could not reach {url}: {exc}\n"
            "Is the stack up? Try `make ps`."
        )


def show(title: str, payload: Dict) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, indent=2))


def demo_idle(args) -> int:
    """Force a vehicle idle until it crosses IDLE_ALERT_SIM_MIN."""
    params = {}
    if args.vehicle:
        params["vehicle_id"] = args.vehicle
    result = call(TELEMETRY, "force_idle", params)
    show("forced a vehicle idle", result)
    if result.get("status") != "ok":
        return 1
    wait_s = result.get("expect_alert_after_real_s", 45)
    print(
        f"\nWatch for the alert in about {wait_s:.0f} real seconds "
        f"({result.get('expect_alert_after_sim_min')} simulated minutes):\n"
        f"  curl {API}/alerts/idle?status=open\n"
        f"  psql: SELECT * FROM idle_alerts WHERE status='open';\n"
        f"  Grafana -> Fleet operations -> Open idle alerts"
    )
    return 0


def demo_outage(args) -> int:
    """Pause the producer to fire TelemetryNotProduced, then optionally resume."""
    if args.resume:
        show("producer resumed", call(TELEMETRY, "resume"))
        return 0

    show("producer paused", call(TELEMETRY, "pause"))
    print(
        "\nExpect, within ~1-2 real minutes:\n"
        "  Prometheus  TelemetryNotProduced fires (http://localhost:9090/alerts)\n"
        "  Alertmanager shows it              (http://localhost:9093)\n"
        f"  API stored the webhook             curl {API}/pipeline/status\n"
        "  psql: SELECT alertname, status, received_at FROM alert_notifications\n"
        "          ORDER BY received_at DESC LIMIT 5;\n"
        "\nRestore the fleet with:  make demo-outage ARGS=--resume\n"
        "                     or:  python scripts/demo.py outage --resume"
    )
    return 0


def demo_resubmit(args) -> int:
    """Write a corrected v2 expense file for a past simulated date."""
    params = {"sim_date": args.sim_date} if args.sim_date else {}
    result = call(EXPENSES, "resubmit", params)
    show("corrected expense file written", result)
    if result.get("status") != "ok":
        print("\nNo expense file exists yet. A simulated day takes 24 real minutes;\n"
              "wait for the first one to close, then try again.")
        return 1
    sim_date = result["sim_date"]
    print(
        f"\nThe DAG polls every 2 real minutes and will recompute {sim_date}.\n"
        "The point to show: the ROW COUNT stays the same while VALUES change.\n"
        "  Before/after:\n"
        f"    SELECT count(*), round(sum(net_profit)::numeric,2)\n"
        f"      FROM batch_vehicle_daily WHERE sim_date = '{sim_date}';\n"
        f"    SELECT run_id, expense_file_version, status\n"
        f"      FROM batch_runs WHERE sim_date = '{sim_date}' ORDER BY started_at;\n"
        f"  Airflow: http://localhost:8088 -> fleet_daily_reconciliation"
    )
    return 0


def demo_late_file(args) -> int:
    """Hold the next expense file past its SLA."""
    result = call(EXPENSES, "delay-next")
    show("next expense file will be late", result)
    print(
        "\nWhen the next simulated day closes, the DAG finds no file within the SLA\n"
        "and sets the late flag. Expect:\n"
        "  psql: SELECT sim_date, expense_file_late FROM batch_runs\n"
        "          ORDER BY started_at DESC LIMIT 3;\n"
        "  Prometheus: fleet_expense_file_late == 1 -> ExpenseFileLate fires\n"
        "  psql: SELECT * FROM alert_notifications WHERE alertname='ExpenseFileLate';"
    )
    return 0


def demo_status(_args) -> int:
    """Print both simulators' health, for a quick 'is it alive' check."""
    show("telemetry simulator", call(TELEMETRY, "health"))
    show("expense dropper", call(EXPENSES, "health"))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Fleet pipeline demo driver")
    sub = parser.add_subparsers(dest="command", required=True)

    p_idle = sub.add_parser("idle", help="force a vehicle idle -> idle alert")
    p_idle.add_argument("--vehicle", help="vehicle id, e.g. V007 (default: any online)")
    p_idle.set_defaults(func=demo_idle)

    p_outage = sub.add_parser("outage", help="pause the producer -> no-data alert")
    p_outage.add_argument("--resume", action="store_true", help="resume instead")
    p_outage.set_defaults(func=demo_outage)

    p_resub = sub.add_parser("resubmit", help="corrected v2 expense file -> recompute")
    p_resub.add_argument("--sim-date", dest="sim_date", help="YYYY-MM-DD")
    p_resub.set_defaults(func=demo_resubmit)

    p_late = sub.add_parser("late-file", help="delay the next expense file past its SLA")
    p_late.set_defaults(func=demo_late_file)

    sub.add_parser("status", help="show both simulators' health").set_defaults(
        func=demo_status
    )

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
