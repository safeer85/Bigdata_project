"""Typed access to the environment variables documented in `.env.example`.

Nothing in this project reads `os.environ` directly; every knob goes through here
so that a single `.env` file drives the simulators, both Spark layers, Airflow and
the API. That is what makes "changing the compression is only a config change"
(SPEC §3) actually true.
"""
from __future__ import annotations

import os
from typing import Optional


def env_str(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise KeyError(f"Required environment variable {name} is not set")
    return value


def env_int(name: str, default: Optional[int] = None) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        if default is None:
            raise KeyError(f"Required environment variable {name} is not set")
        return default
    return int(raw)


def env_float(name: str, default: Optional[float] = None) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        if default is None:
            raise KeyError(f"Required environment variable {name} is not set")
        return default
    return float(raw)


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# --- Clock -----------------------------------------------------------------
COMPRESSION = env_int("COMPRESSION", 60)
SIM_EPOCH = env_str("SIM_EPOCH", "2024-01-01T06:00:00Z")
SHARED_DIR = env_str("SHARED_DIR", "/shared")

# --- Storage ---------------------------------------------------------------
LAKE_ROOT = env_str("LAKE_ROOT", "/lake")
LANDING_DIR = env_str("LANDING_DIR", "/landing")
REPORTS_DIR = env_str("REPORTS_DIR", "/reports")
EXPENSE_DIR = os.path.join(LANDING_DIR, "expenses")
ODOMETER_DIR = os.path.join(SHARED_DIR, "odometer")

# Paths are always built from LAKE_ROOT so that swapping the Docker volume for an
# `s3a://bucket` in production is a one-variable change (SPEC §2).
RAW_TELEMETRY_PATH = f"{LAKE_ROOT}/raw/telemetry"
CURATED_VEHICLE_DAILY_PATH = f"{LAKE_ROOT}/curated/vehicle_daily"
CHECKPOINT_ROOT = f"{LAKE_ROOT}/checkpoints"

# --- Database --------------------------------------------------------------
POSTGRES_HOST = env_str("POSTGRES_HOST", "postgres")
POSTGRES_PORT = env_int("POSTGRES_PORT", 5432)
POSTGRES_DB = env_str("POSTGRES_DB", "fleet")
POSTGRES_USER = env_str("POSTGRES_USER", "fleet")
POSTGRES_PASSWORD = env_str("POSTGRES_PASSWORD", "fleet_dev_pw")

# --- Kafka -----------------------------------------------------------------
KAFKA_BOOTSTRAP = env_str("KAFKA_BOOTSTRAP", "kafka:9092")
TELEMETRY_TOPIC = env_str("TELEMETRY_TOPIC", "fleet.telemetry")
DLQ_TOPIC = env_str("DLQ_TOPIC", "fleet.telemetry.dlq")

# --- City / zones ----------------------------------------------------------
CITY_LAT_MIN = env_float("CITY_LAT_MIN", 12.90)
CITY_LAT_MAX = env_float("CITY_LAT_MAX", 13.06)
CITY_LON_MIN = env_float("CITY_LON_MIN", 77.52)
CITY_LON_MAX = env_float("CITY_LON_MAX", 77.68)
ZONE_ROWS = env_int("ZONE_ROWS", 4)
ZONE_COLS = env_int("ZONE_COLS", 4)

# --- Fleet -----------------------------------------------------------------
N_VEHICLES = env_int("N_VEHICLES", 50)
N_LEMONS = env_int("N_LEMONS", 5)
EMIT_INTERVAL_REAL_S = env_float("EMIT_INTERVAL_REAL_S", 2.0)
SIM_SEED = env_int("SIM_SEED", 20240101)

# --- Service ports ---------------------------------------------------------
# Kept here rather than in compose alone so that the code, the Prometheus scrape
# config and the demo scripts all agree on one number.
TELEMETRY_METRICS_PORT = env_int("TELEMETRY_METRICS_PORT", 8001)
TELEMETRY_CONTROL_PORT = env_int("TELEMETRY_CONTROL_PORT", 8010)
EXPENSE_METRICS_PORT = env_int("EXPENSE_METRICS_PORT", 8002)
EXPENSE_CONTROL_PORT = env_int("EXPENSE_CONTROL_PORT", 8011)
SPEED_METRICS_PORT = env_int("SPEED_METRICS_PORT", 8003)
ARCHIVER_METRICS_PORT = env_int("ARCHIVER_METRICS_PORT", 8004)
API_PORT = env_int("API_PORT", 8000)

# --- Fault injection -------------------------------------------------------
FAULT_MALFORMED_RATE = env_float("FAULT_MALFORMED_RATE", 0.005)
FAULT_INVALID_RATE = env_float("FAULT_INVALID_RATE", 0.005)
FAULT_DUPLICATE_RATE = env_float("FAULT_DUPLICATE_RATE", 0.01)
FAULT_LATE_RATE = env_float("FAULT_LATE_RATE", 0.02)
FAULT_LATE_MIN_SIM_MIN = env_int("FAULT_LATE_MIN_SIM_MIN", 1)
FAULT_LATE_MAX_SIM_MIN = env_int("FAULT_LATE_MAX_SIM_MIN", 20)

# --- Fares -----------------------------------------------------------------
CURRENCY_LABEL = env_str("CURRENCY_LABEL", "CU")
FARE_BASE = env_float("FARE_BASE", 30.0)
FARE_PER_KM = env_float("FARE_PER_KM", 12.0)
FARE_PER_MIN = env_float("FARE_PER_MIN", 1.5)

# --- Expenses --------------------------------------------------------------
EXPENSE_DELAY_SIM_MIN = env_int("EXPENSE_DELAY_SIM_MIN", 60)
EXPENSE_LATE_FILE_RATE = env_float("EXPENSE_LATE_FILE_RATE", 0.05)
EXPENSE_BAD_ROW_RATE = env_float("EXPENSE_BAD_ROW_RATE", 0.04)
EXPENSE_DISTANCE_DISCREPANCY_RATE = env_float("EXPENSE_DISTANCE_DISCREPANCY_RATE", 0.03)
FUEL_PRICE_PER_LITRE = env_float("FUEL_PRICE_PER_LITRE", 105.0)

# --- Speed layer -----------------------------------------------------------
WATERMARK_SIM_MIN = env_int("WATERMARK_SIM_MIN", 10)
IDLE_ALERT_SIM_MIN = env_int("IDLE_ALERT_SIM_MIN", 45)
OFFLINE_SIM_MIN = env_int("OFFLINE_SIM_MIN", 20)
ARCHIVER_TRIGGER_REAL_S = env_int("ARCHIVER_TRIGGER_REAL_S", 30)
SPEED_TRIGGER_REAL_S = env_int("SPEED_TRIGGER_REAL_S", 5)

# --- Batch layer -----------------------------------------------------------
LATE_GRACE_SIM_MIN = env_int("LATE_GRACE_SIM_MIN", 30)
EXPENSE_SLA_SIM_MIN = env_int("EXPENSE_SLA_SIM_MIN", 120)
MARGIN_THRESHOLD = env_float("MARGIN_THRESHOLD", 0.10)
DISTANCE_MISMATCH_THRESHOLD = env_float("DISTANCE_MISMATCH_THRESHOLD", 0.15)
MAX_INVALID_EXPENSE_PCT = env_float("MAX_INVALID_EXPENSE_PCT", 0.20)
MAX_PLAUSIBLE_SPEED_KMH = env_float("MAX_PLAUSIBLE_SPEED_KMH", 150.0)

# --- Logging ---------------------------------------------------------------
LOG_LEVEL = env_str("LOG_LEVEL", "INFO")
LOG_SAMPLE_EVERY = env_int("LOG_SAMPLE_EVERY", 500)


def jdbc_url() -> str:
    """JDBC URL used by the Spark layers when they write to PostgreSQL."""
    return f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def dsn() -> str:
    """libpq DSN used by psycopg-based components (simulators, API, Airflow)."""
    return (
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    )
