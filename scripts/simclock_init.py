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

import os
import stat

from common import config, simclock
from common.logging import get_logger

log = get_logger("simclock-init", stage="orchestration")

# Volumes shared between containers that run as DIFFERENT users:
#   fleet/app    uid 10001  (simulators, API)
#   fleet/spark  uid 185    (spark master, worker, both streaming apps)
#   fleet/airflow uid 50000 (Airflow and the batch jobs)
#
# Docker initialises a new named volume from whichever image mounts it first,
# inheriting that image's ownership -- so which container happens to start first
# decides whether the others can write. That is a race, and it failed exactly
# that way on a clean `make up`. This one-shot init container runs as root and
# opens the permissions up front, which is what init containers are for.
SHARED_VOLUMES = ["/shared", "/lake", "/landing", "/reports"]


def prepare_volumes() -> None:
    """Create the shared directories and make them writable by every service."""
    for path in SHARED_VOLUMES:
        if not os.path.isdir(path):
            continue
        try:
            os.makedirs(path, exist_ok=True)
            # 0o777 on a single-host demo volume. The alternative -- a shared
            # group id baked into three different base images -- would be more
            # correct in production and far more fragile here. Noted in
            # docs/decisions.md.
            os.chmod(path, 0o777)
        except PermissionError:
            log.warning(
                "could not adjust permissions (not running as root?)",
                extra={"event": "volume_prepare_skipped", "path": path},
            )
            continue

    # Subdirectories the simulators and the batch layer write into.
    for path in (config.ODOMETER_DIR, config.EXPENSE_DIR):
        try:
            os.makedirs(path, exist_ok=True)
            os.chmod(path, 0o777)
        except (PermissionError, OSError):
            pass

    log.info(
        "shared volumes prepared",
        extra={
            "event": "volumes_prepared",
            "volumes": {
                p: stat.filemode(os.stat(p).st_mode)
                for p in SHARED_VOLUMES
                if os.path.isdir(p)
            },
        },
    )


def main() -> None:
    prepare_volumes()
    existed = os.path.exists(simclock.CLOCK_FILE)
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
