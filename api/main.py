"""Uvicorn entry point for the serving layer.

Run as `python -m api.main` so the container command matches how the module is
imported everywhere else; `uvicorn api.app:app` would work too but would bypass
the startup checks below.
"""
from __future__ import annotations

import sys

import uvicorn

from common import config, db, simclock
from common.logging import get_logger

log = get_logger("api", stage="serving")


def main() -> int:
    # Fail fast if the database is unreachable. An API that starts and then
    # 500s on every request looks healthy to Docker but is useless, and the
    # ApiDown alert would never fire because the process is up.
    db.wait_for_db()

    try:
        clock = simclock.load_clock()
        simclock.set_clock(clock)
        sim_now = clock.now_sim().isoformat()
    except Exception as exc:  # noqa: BLE001
        # The API can still serve batch endpoints without the clock, so this is a
        # warning rather than a fatal error.
        log.warning(
            "simulated clock unavailable at startup",
            extra={"event": "simclock_unavailable", "error": str(exc)},
        )
        sim_now = None

    log.info(
        "serving layer starting",
        extra={
            "event": "api_start",
            "port": config.API_PORT,
            "sim_now": sim_now,
            "currency": config.CURRENCY_LABEL,
        },
    )

    uvicorn.run(
        "api.app:app",
        host="0.0.0.0",
        port=config.API_PORT,
        # Uvicorn's own access log is plain text and would break the "JSON lines
        # only" rule; the middleware in api/app.py records every request as a
        # structured metric instead.
        access_log=False,
        log_config=None,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
