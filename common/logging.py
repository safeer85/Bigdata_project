"""Structured JSON logging (SPEC 10.1). No bare print() anywhere in this project.

Every line is one JSON object on stdout, which Docker captures and (stretch goal)
Promtail ships to Loki. The required fields are always present:

    ts, level, service, stage, event, msg, sim_time

`sim_time` is the killer field: with a compressed clock, a wall-clock timestamp
tells you nothing about which simulated hour a log line belongs to. Every line
carries both.

Per-event logging is sampled (`should_log_event`) because at 50 vehicles emitting
every 2 real seconds an INFO line per event would bury everything else. Counts per
batch are logged instead.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from common import config

STAGES = ("ingestion", "processing", "storage", "serving", "orchestration")

# Attributes the stdlib puts on every LogRecord; anything else a caller passed via
# `extra=` is ours and belongs in the JSON body.
_RESERVED = {
    "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName",
    "levelname", "levelno", "lineno", "module", "msecs", "message", "msg", "name",
    "pathname", "process", "processName", "relativeCreated", "stack_info",
    "thread", "threadName", "taskName",
}


def _sim_time_or_none() -> Optional[str]:
    """Current simulated time, or None if the clock file is not readable yet.

    Logging must never be the thing that crashes a container, so a missing clock
    degrades to a null field rather than an exception.
    """
    try:
        from common import simclock

        return simclock.now_sim().isoformat().replace("+00:00", "Z")
    except Exception:  # noqa: BLE001 - logging must not raise
        return None


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single-line JSON object."""

    def __init__(self, service: str, stage: str) -> None:
        super().__init__()
        self.service = service
        self.stage = stage

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self.service,
            "stage": getattr(record, "stage", self.stage),
            "event": getattr(record, "event", record.funcName),
            "msg": record.getMessage(),
            "sim_time": getattr(record, "sim_time", None) or _sim_time_or_none(),
        }

        # Context ids and any other extras, e.g. vehicle_id, batch_id, sim_date.
        for key, value in record.__dict__.items():
            if key in _RESERVED or key in payload or key.startswith("_"):
                continue
            payload[key] = value

        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def get_logger(service: str, stage: str = "processing") -> logging.Logger:
    """Return the configured JSON logger for one service.

    Idempotent: calling it twice in the same process does not double every line,
    which matters because Spark re-imports modules on executors.
    """
    if stage not in STAGES:
        raise ValueError(f"stage must be one of {STAGES}, got {stage!r}")

    logger = logging.getLogger(service)
    if not getattr(logger, "_fleet_configured", False):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service, stage))
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, config.LOG_LEVEL.upper(), logging.INFO))
        logger.propagate = False  # do not also emit through the root handler
        logger._fleet_configured = True  # type: ignore[attr-defined]
    return logger


class _Sampler:
    """Thread-safe 1-in-N counter behind `should_log_event`."""

    def __init__(self, every: int) -> None:
        self.every = max(1, every)
        self._count = 0
        self._lock = threading.Lock()

    def tick(self) -> bool:
        with self._lock:
            self._count += 1
            return self._count % self.every == 0


_SAMPLER = _Sampler(config.LOG_SAMPLE_EVERY)


def should_log_event() -> bool:
    """True for roughly 1 in LOG_SAMPLE_EVERY calls (SPEC 10.1)."""
    return _SAMPLER.tick()
