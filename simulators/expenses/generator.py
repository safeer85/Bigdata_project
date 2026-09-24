"""Builds the daily expense rows (SPEC 5.2).

The partner's file is the second source in this Lambda architecture, and it is the
reason the batch layer exists at all: it arrives once per simulated day, well after
the events it relates to, and it can be RESUBMITTED with corrections. Nothing about
that fits a streaming model.

Costs are derived from the odometer ledger the telemetry simulator wrote, so the
numbers are internally consistent: a vehicle that drove further really does burn
more fuel, and the batch layer's `gps_km` really should be close to the reported
`distance_covered` -- except in the ~3% of rows where a discrepancy is injected on
purpose to exercise the distance check.
"""
from __future__ import annotations

import csv
import json
import os
import random
from typing import Dict, List, Optional

from common import config
from simulators.telemetry.fleet import build_fleet

# Reason: a service visit is a big, lumpy cost that lands on one day. Without it
# every vehicle's maintenance cost would be small and uniform, and the
# `in_service` flag in the report would never be exercised.
SERVICE_COST_MIN = 1800.0
SERVICE_COST_MAX = 6500.0


def load_odometer(sim_date: str) -> Dict[str, float]:
    """Read the ground-truth km ledger for a simulated date.

    Returns an empty dict when the ledger is missing, which happens for the
    partial first day if the stack was started mid-day. The caller then writes a
    file with zero distances rather than no file at all, because "no file" means
    something quite different to the batch layer (a late file / SLA breach).
    """
    path = os.path.join(config.ODOMETER_DIR, f"{sim_date}.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def build_rows(
    sim_date: str,
    odometer: Dict[str, float],
    rng: random.Random,
    corrected: bool = False,
) -> List[Dict[str, object]]:
    """Produce one expense row per vehicle for a simulated date.

    `corrected=True` produces the v2 file used by `make demo-resubmit`: same
    vehicles, but the injected faults and discrepancies are removed and the costs
    are slightly restated. That is what makes the recompute observable -- the row
    COUNT stays the same while the VALUES change (SPEC 12, phase 3 acceptance).
    """
    fleet = build_fleet()
    rows: List[Dict[str, object]] = []

    for vehicle_id, state in sorted(fleet.items()):
        profile = state.profile
        true_km = float(odometer.get(vehicle_id, 0.0))

        # --- fuel ---------------------------------------------------------
        # true km x litres-per-km x price, with a little noise for pump variation.
        noise = 1.0 if corrected else rng.uniform(0.94, 1.07)
        fuel = true_km * profile.fuel_rate * config.FUEL_PRICE_PER_LITRE * noise

        # --- maintenance --------------------------------------------------
        # Usually a small per-km wear charge; occasionally a real service event.
        service_flag = rng.random() < profile.maintenance_propensity
        if service_flag:
            maintenance = rng.uniform(SERVICE_COST_MIN, SERVICE_COST_MAX)
        else:
            maintenance = true_km * rng.uniform(0.4, 1.6)

        # --- reported distance --------------------------------------------
        # Normally within +-3% of the truth (odometer rounding, GPS drift).
        distance = true_km * rng.uniform(0.97, 1.03)
        if not corrected and rng.random() < config.EXPENSE_DISTANCE_DISCREPANCY_RATE:
            # A real discrepancy the reconciliation must catch: a +-25% error is
            # well past the 15% DISTANCE_MISMATCH_THRESHOLD.
            distance = true_km * rng.choice([0.75, 1.25])

        row: Dict[str, object] = {
            "date": sim_date,
            "vehicle_id": vehicle_id,
            "fuel_cost": round(fuel, 2),
            "maintenance_cost": round(maintenance, 2),
            "distance_covered": round(distance, 3),
            "service_flag": "true" if service_flag else "false",
        }

        # --- injected row-level faults (SPEC 5.2) -------------------------
        # Skipped entirely in a corrected file: "corrected" is exactly the promise
        # that the partner fixed their bad rows.
        if not corrected and rng.random() < config.EXPENSE_BAD_ROW_RATE:
            row = _corrupt(row, rng)

        rows.append(row)

    # An unknown vehicle_id is a FILE-level fault, not a row-level one: it is an
    # extra row rather than a corrupted one, so it also changes the row count, and
    # the batch layer must quarantine it without failing the whole file.
    if not corrected and rng.random() < 0.5:
        rows.append(
            {
                "date": sim_date,
                "vehicle_id": f"V{rng.randint(900, 999)}",
                "fuel_cost": round(rng.uniform(200, 900), 2),
                "maintenance_cost": round(rng.uniform(0, 200), 2),
                "distance_covered": round(rng.uniform(20, 200), 3),
                "service_flag": "false",
            }
        )

    return rows


def _corrupt(row: Dict[str, object], rng: random.Random) -> Dict[str, object]:
    """Break one field of a row, matching the reason codes the batch layer uses."""
    kind = rng.choice(["missing", "negative", "missing_distance"])
    if kind == "missing":
        row["fuel_cost"] = ""                       # -> missing_value
    elif kind == "negative":
        row["maintenance_cost"] = -abs(float(row["maintenance_cost"]))  # -> negative_value
    else:
        row["distance_covered"] = ""                # -> missing_value
    return row


def write_csv(path: str, rows: List[Dict[str, object]]) -> str:
    """Write the CSV ATOMICALLY: write `.tmp`, then rename (SPEC 4.2).

    This matters because an Airflow sensor watches the directory. `os.replace` is
    atomic within a filesystem, so the sensor either sees no file or sees the whole
    file -- never a half-written one that would fail validation for the wrong reason.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["date", "vehicle_id", "fuel_cost", "maintenance_cost",
                        "distance_covered", "service_flag"],
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)
    return path


def file_name(sim_date: str, version: int) -> str:
    """`expenses_<sim_date>_v<n>.csv` (SPEC 4.2)."""
    return f"expenses_{sim_date}_v{version}.csv"


def latest_version(sim_date: str, directory: Optional[str] = None) -> int:
    """Highest version number already on disk for a date, or 0 if none."""
    directory = directory or config.EXPENSE_DIR
    if not os.path.isdir(directory):
        return 0
    prefix = f"expenses_{sim_date}_v"
    versions = [
        int(name[len(prefix):-len(".csv")])
        for name in os.listdir(directory)
        if name.startswith(prefix) and name.endswith(".csv")
    ]
    return max(versions) if versions else 0
