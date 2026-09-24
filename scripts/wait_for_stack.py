"""Block until the stack is healthy, so `make up` returns only when it truly is.

Runs on the HOST (not in a container), so it deliberately depends on nothing but
the standard library and the `docker` CLI.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from typing import Dict, List

# Services whose healthcheck must go green before `make up` is considered done.
# One-shot services are excluded: they exit, and an exited container has no health.
CORE_SERVICES = [
    "postgres",
    "kafka",
    "telemetry-sim",
    "expense-sim",
    "spark-master",
    "spark-worker",
    "archiver",
    "speed",
    "airflow",
    "api",
    "prometheus",
    "alertmanager",
    "grafana",
]

# Spark and Airflow are slow to start: the drivers download nothing (jars are
# baked in) but the JVM plus the first micro-batch still takes a while.
TIMEOUT_S = 600
POLL_S = 5


def compose_ps() -> List[Dict]:
    """Current service state, via `docker compose ps --format json`."""
    result = subprocess.run(
        ["docker", "compose", "ps", "--format", "json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return []
    rows: List[Dict] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        # Older compose versions emit a single JSON array instead of one object
        # per line; handle both rather than pinning the reader to one of them.
        rows.extend(parsed) if isinstance(parsed, list) else rows.append(parsed)
    return rows


def main() -> int:
    deadline = time.time() + TIMEOUT_S
    pending = set(CORE_SERVICES)

    print(f"waiting for {len(pending)} services to become healthy (timeout {TIMEOUT_S}s)")
    while time.time() < deadline:
        rows = {row.get("Service"): row for row in compose_ps()}
        still_pending = set()

        for service in sorted(pending):
            row = rows.get(service)
            if row is None:
                still_pending.add(service)
                continue
            health = (row.get("Health") or "").lower()
            state = (row.get("State") or "").lower()
            if health == "healthy":
                print(f"  ok      {service}")
            elif health == "unhealthy":
                print(f"  UNHEALTHY {service} - check `make logs s={service}`")
                still_pending.add(service)
            elif state == "running" and health == "":
                # No healthcheck defined: running is the best signal available.
                print(f"  ok      {service} (running, no healthcheck)")
            else:
                still_pending.add(service)

        pending = still_pending
        if not pending:
            print("\nall core services healthy")
            return 0

        print(f"  ... still waiting on: {', '.join(sorted(pending))}")
        time.sleep(POLL_S)

    print(f"\nTIMEOUT after {TIMEOUT_S}s. Still unhealthy: {', '.join(sorted(pending))}")
    print("Inspect with:  docker compose ps   and   make logs s=<service>")
    return 1


if __name__ == "__main__":
    sys.exit(main())
