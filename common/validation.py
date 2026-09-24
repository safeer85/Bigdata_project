"""Validation rules, applied by BOTH layers (SPEC §7.3 and §8.2 task 4).

The speed layer uses these to route bad events to the DLQ; the batch layer
re-applies exactly the same rules to the raw Parquet archive. Re-applying rather
than trusting the speed layer matters: the archiver writes whatever Kafka gave it,
including malformed payloads, so the batch layer has to clean the data itself.
Because it is the *same* function, a bad event is rejected identically in both
layers -- which is the property the report claims for our Lambda mitigation.

Each rejection carries a short `reason` code. Those codes are what land in the DLQ
topic and in the `dq_issues` table, so they are part of our contract too.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from common import config, geo, schemas

# --- Reason codes ----------------------------------------------------------
MISSING_FIELD = "missing_field"
BAD_TIMESTAMP = "bad_timestamp"
BAD_EVENT_TYPE = "bad_event_type"
BAD_STATUS = "bad_status"
OUT_OF_BOUNDS = "coords_out_of_bounds"
NEGATIVE_SPEED = "negative_speed"
NEGATIVE_FARE = "negative_fare"
BAD_NUMBER = "bad_number"
UNPARSEABLE = "unparseable_json"

# Expense-specific codes (SPEC §8.2 task 3).
EXP_MISSING_VALUE = "missing_value"
EXP_NEGATIVE_VALUE = "negative_value"
EXP_UNKNOWN_VEHICLE = "unknown_vehicle"
EXP_DUPLICATE = "duplicate"


def parse_event_timestamp(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 simulated event time, or None if it is unusable."""
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def validate_event(event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Return (is_valid, reason). The first failing rule wins.

    Kept as one pure function over a plain dict so it can run unchanged inside a
    pandas UDF on Spark executors and inside the simulator's own tests.
    """
    if not isinstance(event, dict):
        return False, UNPARSEABLE

    for name, _, _, nullable in schemas.TELEMETRY_FIELDS:
        if nullable:
            continue
        if event.get(name) is None:
            return False, MISSING_FIELD

    if event["event_type"] not in schemas.EVENT_TYPES:
        return False, BAD_EVENT_TYPE
    if event["status"] not in schemas.STATUSES:
        return False, BAD_STATUS

    try:
        lat = float(event["lat"])
        lon = float(event["lon"])
        speed = float(event["speed"])
        fare = float(event["fare"])
    except (TypeError, ValueError):
        return False, BAD_NUMBER

    # Rule from SPEC §4.1: coordinates must fall inside the configured city box.
    # The simulator injects out-of-box coordinates on purpose (~0.5%).
    if not geo.in_city(lat, lon):
        return False, OUT_OF_BOUNDS
    if speed < 0:
        return False, NEGATIVE_SPEED
    if fare < 0:
        return False, NEGATIVE_FARE

    if parse_event_timestamp(event["timestamp"]) is None:
        return False, BAD_TIMESTAMP

    return True, None


def validate_expense_row(
    row: Dict[str, Any], known_vehicles: Optional[set] = None
) -> Tuple[bool, Optional[str]]:
    """Validate one expense CSV row. Returns (is_valid, reason).

    Duplicate detection is NOT done here because it needs the whole file; the
    caller marks duplicates with EXP_DUPLICATE after grouping on vehicle_id.
    """
    for column in schemas.EXPENSE_COLUMNS:
        value = row.get(column)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return False, EXP_MISSING_VALUE

    try:
        fuel = float(row["fuel_cost"])
        maintenance = float(row["maintenance_cost"])
        distance = float(row["distance_covered"])
    except (TypeError, ValueError):
        return False, EXP_MISSING_VALUE

    if fuel < 0 or maintenance < 0 or distance < 0:
        return False, EXP_NEGATIVE_VALUE

    if known_vehicles is not None and str(row["vehicle_id"]) not in known_vehicles:
        return False, EXP_UNKNOWN_VEHICLE

    return True, None


def spark_validation_expr():
    """The same rules as `validate_event`, as a Spark Column of reason-or-null.

    Why a second implementation? Running `validate_event` row-by-row through a
    Python UDF on every streaming micro-batch is the single most expensive thing
    we could do. This expression is pure Spark SQL, so it stays in the JVM.

    The two are kept honest by `tests/test_validation_parity.py`, which feeds the
    same fixture rows through both and asserts identical reason codes. That test
    is the reason we are allowed to have two implementations at all.
    """
    from pyspark.sql import functions as F

    required = [n for n, _, _, nullable in schemas.TELEMETRY_FIELDS if not nullable]
    any_missing = F.lit(False)
    for name in required:
        any_missing = any_missing | F.col(name).isNull()

    return (
        F.when(any_missing, F.lit(MISSING_FIELD))
        .when(~F.col("event_type").isin(list(schemas.EVENT_TYPES)), F.lit(BAD_EVENT_TYPE))
        .when(~F.col("status").isin(list(schemas.STATUSES)), F.lit(BAD_STATUS))
        .when(
            (F.col("lat") < F.lit(config.CITY_LAT_MIN))
            | (F.col("lat") > F.lit(config.CITY_LAT_MAX))
            | (F.col("lon") < F.lit(config.CITY_LON_MIN))
            | (F.col("lon") > F.lit(config.CITY_LON_MAX)),
            F.lit(OUT_OF_BOUNDS),
        )
        .when(F.col("speed") < 0, F.lit(NEGATIVE_SPEED))
        .when(F.col("fare") < 0, F.lit(NEGATIVE_FARE))
        .when(F.col("event_time").isNull(), F.lit(BAD_TIMESTAMP))
        .otherwise(F.lit(None).cast("string"))
    )


def reason_codes() -> List[str]:
    """Every code we can emit. Used to pre-create Prometheus label series."""
    return [
        MISSING_FIELD,
        BAD_TIMESTAMP,
        BAD_EVENT_TYPE,
        BAD_STATUS,
        OUT_OF_BOUNDS,
        NEGATIVE_SPEED,
        NEGATIVE_FARE,
        BAD_NUMBER,
        UNPARSEABLE,
    ]
