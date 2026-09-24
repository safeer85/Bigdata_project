"""Expense file validation and quarantine (SPEC 8.2 task 3).

Splits a partner's CSV into clean rows and quarantined rows. Quarantined rows go
to `dq_issues` with one of four reason codes, all defined in `common.validation`
so that the batch layer and the speed layer share a single vocabulary for "bad
data" -- the DQ section of the report and the DLQ metric then line up.

The task FAILS if more than MAX_INVALID_EXPENSE_PCT (20%) of rows are invalid.
That threshold is a judgement call worth defending: a handful of bad rows is
normal partner sloppiness and should be quarantined and reported, but a file
that is one-fifth garbage is a broken feed, and silently reconciling a day from
it would put wrong numbers in front of a finance team.
"""
from __future__ import annotations

import csv
import json
from typing import Dict, List, Optional, Set, Tuple

from common import config, db, validation
from common.logging import get_logger

log = get_logger("batch", stage="processing")


def known_vehicle_ids() -> Set[str]:
    """The roster an expense row's vehicle_id is checked against.

    Derived from the simulator's own fleet definition rather than from whatever
    happens to be in the database, so the check still works on a cold start when
    `rt_vehicle_state` is empty.
    """
    return {f"V{i:03d}" for i in range(1, config.N_VEHICLES + 1)}


def read_csv(path: str) -> List[Dict[str, str]]:
    """Read the expense CSV as raw strings.

    Everything stays a string here: casting at read time would turn a
    deliberately corrupted value into a silent null, and the reason code would be
    lost. `common.validation` does the casting and reports what failed.
    """
    with open(path, encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def validate_file(
    path: str, sim_date: str, run_id: str
) -> Tuple[List[Dict], List[Dict], Dict[str, int]]:
    """Split a file into (clean, quarantined, reason_counts).

    Duplicates are detected here rather than in `validate_expense_row`, because
    "this vehicle appears twice in the file" needs the whole file to decide. The
    FIRST occurrence is kept and later ones are quarantined: a partner resending
    a row within one file gives us no way to tell which is authoritative, and
    keeping the first is at least deterministic and explainable.
    """
    rows = read_csv(path)
    roster = known_vehicle_ids()

    clean: List[Dict] = []
    quarantined: List[Dict] = []
    reasons: Dict[str, int] = {}
    seen: Set[str] = set()

    for row in rows:
        vehicle_id = (row.get("vehicle_id") or "").strip()

        if vehicle_id and vehicle_id in seen:
            reason = validation.EXP_DUPLICATE
        else:
            _, reason = validation.validate_expense_row(row, known_vehicles=roster)

        if vehicle_id:
            seen.add(vehicle_id)

        if reason is None:
            clean.append(
                {
                    "vehicle_id": vehicle_id,
                    "sim_date": sim_date,
                    "fuel_cost": float(row["fuel_cost"]),
                    "maintenance_cost": float(row["maintenance_cost"]),
                    "distance_covered": float(row["distance_covered"]),
                    "service_flag": str(row["service_flag"]).strip().lower()
                    in {"true", "1", "yes"},
                }
            )
        else:
            reasons[reason] = reasons.get(reason, 0) + 1
            quarantined.append(
                {
                    "run_id": run_id,
                    "sim_date": sim_date,
                    "source": "expenses",
                    "vehicle_id": vehicle_id or None,
                    "reason": reason,
                    "payload": json.dumps(row),
                }
            )

    log.info(
        "expense file validated",
        extra={
            "event": "expenses_validated",
            "run_id": run_id,
            "sim_date": sim_date,
            "path": path,
            "total_rows": len(rows),
            "clean_rows": len(clean),
            "quarantined_rows": len(quarantined),
            "by_reason": reasons,
        },
    )
    return clean, quarantined, reasons


def store_quarantined(quarantined: List[Dict]) -> int:
    """Write quarantined rows to `dq_issues`.

    Plain inserts, not upserts: `dq_issues` has a surrogate id and is an
    append-only audit log. A rerun for the same date writes a fresh set of rows
    tagged with the NEW run_id, so the report can show exactly what that run saw
    without losing what the previous run saw.
    """
    if not quarantined:
        return 0
    with db.cursor() as cur:
        cur.executemany(
            "INSERT INTO dq_issues (run_id, sim_date, source, vehicle_id, reason, payload) "
            "VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
            [
                (r["run_id"], r["sim_date"], r["source"], r["vehicle_id"],
                 r["reason"], r["payload"])
                for r in quarantined
            ],
        )
    return len(quarantined)


def check_invalid_ratio(clean: List[Dict], quarantined: List[Dict]) -> float:
    """Raise if the file is too broken to reconcile from. Returns the ratio."""
    total = len(clean) + len(quarantined)
    if total == 0:
        raise ValueError("expense file contains no rows at all")

    ratio = len(quarantined) / total
    if ratio > config.MAX_INVALID_EXPENSE_PCT:
        raise ValueError(
            f"{ratio:.1%} of expense rows are invalid, above the "
            f"{config.MAX_INVALID_EXPENSE_PCT:.0%} threshold. Refusing to "
            f"reconcile a financial day from a file this broken."
        )
    return ratio
