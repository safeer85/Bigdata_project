"""One-shot clock authority (SPEC 3).

Runs as the `simclock-init` compose service before anything else. It writes
/shared/simclock.json exactly once; every other container then reads that file
instead of deciding for itself when "now" is.

Why a file and not an environment variable: the clock needs `real_start_utc`, the
wall-clock instant the simulated timeline began. An env var would be fixed at
image build time, and each container computing it at startup would give every
service a different epoch, so their windows would never line up.
"""
from __future__ import annotations

from common import config, simclock
from common.logging import get_logger

log = get_logger("simclock-init", stage="orchestration")


def main() -> None:
    existed = __import__("os").path.exists(simclock.CLOCK_FILE)
    clock = simclock.write_clock_file()
    log.info(
        "simulated clock ready",
        extra={
            "event": "simclock_ready",
            "reused_existing": existed,
            "real_start_utc": clock.real_start_utc.isoformat(),
            "sim_epoch": clock.sim_epoch.isoformat(),
            "compression": clock.compression,
            "sim_now": clock.now_sim().isoformat(),
            "sim_day_real_minutes": round(24 * 60 * 60 / clock.compression / 60, 1),
        },
    )
    print_summary(clock)


def print_summary(clock: simclock.SimClock) -> None:
    """A second, human-readable line in the container log.

    Deliberately emitted through the logger (not print) so it still satisfies the
    structured-logging rule while giving a demo audience something readable.
    """
    log.info(
        "1 real second = %s simulated seconds; 1 simulated day = %s real minutes",
        clock.compression,
        round(24 * 60 / clock.compression, 1),
        extra={"event": "simclock_summary", "lake_root": config.LAKE_ROOT},
    )


if __name__ == "__main__":
    main()
