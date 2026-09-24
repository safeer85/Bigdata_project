"""Shared pytest fixtures.

Two things matter here:

1. The simulated clock is INJECTED, never read from /shared. Unit tests must not
   depend on a running stack, and a test that read the real clock file would give
   different answers depending on how long the demo had been up.
2. The SparkSession is session-scoped and local[2]. Creating a session costs a few
   seconds, and creating one per test would make the suite unusable.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

# Set before importing common.config, which reads the environment at import time.
os.environ.setdefault("SHARED_DIR", "/tmp/fleet-test-shared")
os.environ.setdefault("LAKE_ROOT", "/tmp/fleet-test-lake")

from common import simclock  # noqa: E402

# A fixed reference point so every time-based assertion is deterministic.
TEST_EPOCH = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
TEST_REAL_START = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def fixed_clock():
    """Pin the simulated clock for every test in the suite."""
    clock = simclock.SimClock(
        real_start_utc=TEST_REAL_START,
        sim_epoch=TEST_EPOCH,
        compression=60,
    )
    simclock.set_clock(clock)
    yield clock
    simclock.set_clock(None)


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession for the PySpark tests.

    `local[2]` rather than `local[*]`: two partitions is enough to prove that
    logic works across partitions, and using every core makes the suite fight the
    running stack for CPU.
    """
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("fleet-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # Keeps the driver small; these fixtures are a few dozen rows.
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def base_event():
    """One valid telemetry event, as the simulator would emit it."""
    return {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "event_type": "ping",
        "trip_id": None,
        "driver_id": "D001",
        "vehicle_id": "V001",
        "lat": 12.98,
        "lon": 77.60,
        "speed": 22.5,
        "status": "enroute",
        "fare": 0.0,
        "timestamp": "2024-01-01T08:30:00Z",
        "schema_version": 1,
    }
