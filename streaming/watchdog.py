"""Progress watchdog for the two streaming applications.

WHY THIS EXISTS (a bug we hit, and the reason it is worth explaining in the viva).

Restarting the Spark worker killed the archiver's executor. The driver logged

    ERROR TaskSchedulerImpl: Lost executor 0: Worker shutting down
    WARN  StandaloneSchedulerBackend: Disconnected from Spark cluster!

...and then sat there. `query.awaitTermination()` does not return in that state:
the query is still "active", it simply never runs another micro-batch. The
container stayed up, its healthcheck stayed green (the metrics endpoint is served
by the driver, which was alive), and the archiver quietly stopped writing to the
master dataset for half an hour. The batch layer then reconciled a day from an
incomplete archive and produced wrong figures -- silently, which is the worst way
for a data pipeline to fail.

Blocking on `awaitTermination` alone assumes that a broken query TERMINATES. In
Spark standalone mode a disconnected driver does not.

So instead of blocking, each app hands its queries to this watchdog, which checks
that every query is both active and making progress and calls `os._exit(1)` when
one is not. Docker's `restart: unless-stopped` then brings the application back,
it resumes from its checkpoint, and no data is lost -- the Kafka retention covers
far more than any restart gap.

`os._exit` rather than `sys.exit`: `sys.exit` raises SystemExit on the watchdog
thread, which Python swallows without stopping the JVM or the other non-daemon
threads. `os._exit` is the blunt instrument that actually ends the process, which
is exactly what we want here.
"""
from __future__ import annotations

import os
import time
from typing import List, Optional

from common import metrics


class ProgressWatchdog:
    """Fails the process when a streaming query stops making progress.

    `stall_seconds` must be comfortably larger than the trigger interval AND than
    a plausible quiet period. The speed layer triggers every 5 real seconds and
    the fleet emits continuously, so a 120-second silence is unambiguous. The
    archiver triggers every 30 seconds, so it is given a longer allowance.
    """

    def __init__(self, queries: List, log, stall_seconds: int = 120,
                 grace_seconds: int = 180) -> None:
        self.queries = queries
        self.log = log
        self.stall_seconds = stall_seconds
        # Startup grace: the first micro-batch of a stateful query can take a
        # while (RocksDB init, Kafka metadata), and killing the app during its own
        # startup would produce a restart loop rather than a recovery.
        self.grace_seconds = grace_seconds

    def _last_progress_epoch(self, query) -> Optional[float]:
        """Wall-clock time of the query's most recent micro-batch, if any."""
        progress = query.lastProgress
        if not progress:
            return None
        stamp = progress.get("timestamp")
        if not stamp:
            return None
        from datetime import datetime, timezone

        try:
            # Spark reports an ISO-8601 instant with a trailing Z.
            return (
                datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
        except ValueError:
            return None

    def _die(self, reason: str, **context) -> None:
        self.log.error(
            "watchdog is failing the application so Docker restarts it: %s",
            reason,
            extra={"event": "watchdog_kill", "reason": reason, **context},
        )
        # Give the log line a moment to flush before the process disappears.
        time.sleep(0.5)
        os._exit(1)

    def run(self, poll_seconds: int = 15) -> None:
        """Block forever, watching. Replaces `awaitTermination`."""
        started = time.time()
        self.log.info(
            "watchdog started",
            extra={
                "event": "watchdog_started",
                "queries": [q.name for q in self.queries],
                "stall_seconds": self.stall_seconds,
                "grace_seconds": self.grace_seconds,
            },
        )

        while True:
            time.sleep(poll_seconds)
            now = time.time()
            in_grace = (now - started) < self.grace_seconds

            for query in self.queries:
                # 1. A query that has genuinely terminated: exit so the exception
                #    is visible in the container logs and the app restarts.
                if not query.isActive:
                    self._die(
                        f"query {query.name!r} is no longer active",
                        query=query.name,
                        exception=str(query.exception()) if query.exception() else None,
                    )

                if in_grace:
                    continue

                # 2. A query that is "active" but frozen -- the disconnected-driver
                #    case this watchdog was written for.
                last = self._last_progress_epoch(query)
                if last is None:
                    self._die(
                        f"query {query.name!r} has never reported a micro-batch",
                        query=query.name,
                    )
                silence = now - last
                if silence > self.stall_seconds:
                    self._die(
                        f"query {query.name!r} has not progressed for "
                        f"{silence:.0f}s (limit {self.stall_seconds}s)",
                        query=query.name,
                        seconds_since_progress=round(silence, 1),
                    )

            # Publish liveness for the StreamStalled alert even on quiet ticks.
            metrics.set_sim_time()
