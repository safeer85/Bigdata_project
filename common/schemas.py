"""The data contracts (SPEC §4), defined exactly once.

Each contract is given twice in *representation* but once in *definition*: a plain
JSON Schema (used by the simulators and by the API) and a Spark `StructType`
(used by both Spark layers). The Spark types are derived from the same field list,
so the two can never drift.

`pyspark` is imported lazily: the simulators and the API must be able to import
this module without a Spark installation.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

SCHEMA_VERSION = 1

EVENT_TYPES = ("ping", "trip_start", "trip_end")
STATUSES = ("idle", "enroute", "on_trip")

# (field name, json type, spark type name, nullable)
# This single list drives both representations below.
TELEMETRY_FIELDS: List[Tuple[str, str, str, bool]] = [
    # Added field: a stable id so duplicates can be dropped without comparing
    # whole payloads. The simulator deliberately re-sends ~1% of ids.
    ("event_id", "string", "string", False),
    # Added field: explicit lifecycle markers. Without them, counting trips would
    # need a chained stateful operator (detect status transitions, then aggregate),
    # which Structured Streaming does not allow after an aggregation.
    ("event_type", "string", "string", False),
    ("trip_id", "string", "string", True),
    ("driver_id", "string", "string", False),
    ("vehicle_id", "string", "string", False),
    ("lat", "number", "double", False),
    ("lon", "number", "double", False),
    ("speed", "number", "double", False),
    ("status", "string", "string", False),
    # Cumulative fare for the current trip; the final value lands on trip_end and
    # is 0 elsewhere, so summing fare over trip_end rows gives exact revenue.
    ("fare", "number", "double", False),
    ("timestamp", "string", "string", False),
    # Added field: lets a future v2 producer coexist with v1 consumers.
    ("schema_version", "integer", "int", False),
]

TELEMETRY_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "fleet.telemetry event",
    "type": "object",
    "additionalProperties": False,
    "required": [name for name, _, _, nullable in TELEMETRY_FIELDS if not nullable],
    "properties": {
        "event_id": {"type": "string", "format": "uuid"},
        "event_type": {"type": "string", "enum": list(EVENT_TYPES)},
        "trip_id": {"type": ["string", "null"]},
        "driver_id": {"type": "string"},
        "vehicle_id": {"type": "string", "pattern": "^V[0-9]{3}$"},
        "lat": {"type": "number"},
        "lon": {"type": "number"},
        "speed": {"type": "number", "minimum": 0},
        "status": {"type": "string", "enum": list(STATUSES)},
        "fare": {"type": "number", "minimum": 0},
        "timestamp": {"type": "string"},
        "schema_version": {"type": "integer", "minimum": 1},
    },
}

EXPENSE_COLUMNS = [
    "date",
    "vehicle_id",
    "fuel_cost",
    "maintenance_cost",
    "distance_covered",
    "service_flag",
]

EXPENSE_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "daily expense row",
    "type": "object",
    "required": EXPENSE_COLUMNS,
    "properties": {
        "date": {"type": "string"},
        "vehicle_id": {"type": "string"},
        "fuel_cost": {"type": "number", "minimum": 0},
        "maintenance_cost": {"type": "number", "minimum": 0},
        "distance_covered": {"type": "number", "minimum": 0},
        "service_flag": {"type": "boolean"},
    },
}


def telemetry_struct():
    """Spark `StructType` for a telemetry event.

    `timestamp` stays a string here on purpose: from_json must not silently null a
    whole row over one unparseable timestamp. It is cast to a real timestamp after
    parsing, where a failure becomes an explicit DLQ reason instead.
    """
    from pyspark.sql import types as T  # lazy: simulators have no pyspark

    mapping = {
        "string": T.StringType(),
        "double": T.DoubleType(),
        "int": T.IntegerType(),
    }
    return T.StructType(
        [
            T.StructField(name, mapping[spark_type], True)
            for name, _, spark_type, _ in TELEMETRY_FIELDS
        ]
    )


def expense_struct():
    """Spark `StructType` for an expense CSV row.

    Every column is read as a STRING and cast later. Reading `fuel_cost` as a
    double would turn a deliberately corrupted value into a silent null; reading
    it as text lets `common.validation` report the real reason code instead.
    """
    from pyspark.sql import types as T

    return T.StructType([T.StructField(c, T.StringType(), True) for c in EXPENSE_COLUMNS])
