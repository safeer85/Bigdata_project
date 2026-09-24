"""A tiny HTTP control surface for the simulators (SPEC 5.1 "demo controls").

Each simulator exposes a handful of commands that the `make demo-*` targets call:

    telemetry-sim :8010   /health  /force_idle  /pause  /resume
    expense-sim   :8011   /health  /resubmit    /delay-next

Deliberately built on `http.server` from the standard library rather than FastAPI.
The simulators already run a Prometheus endpoint and a tight emission loop; adding
uvicorn and an event loop to serve four endpoints would cost more memory and more
explaining than it is worth. It also doubles as the Docker healthcheck target.

Commands are accepted on GET as well as POST so that a demo can be driven from a
browser address bar when the projector is already showing one.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict
from urllib.parse import parse_qs, urlparse

from common.logging import get_logger

Handler = Callable[[Dict[str, str]], Dict]


class ControlServer:
    """Registers named commands and serves them on a background thread."""

    def __init__(self, service: str, port: int, stage: str = "ingestion") -> None:
        self.service = service
        self.port = port
        self.log = get_logger(service, stage=stage)
        self._routes: Dict[str, Handler] = {}
        self._thread = None
        self._httpd = None

    def route(self, path: str) -> Callable[[Handler], Handler]:
        """Decorator registering a handler for `/path`."""

        def register(func: Handler) -> Handler:
            self._routes[path.strip("/")] = func
            return func

        return register

    def start(self) -> None:
        """Serve forever on a daemon thread, so it never blocks shutdown."""
        routes = self._routes
        log = self.log

        class RequestHandler(BaseHTTPRequestHandler):
            # The default handler logs every request to stderr in Apache format,
            # which would break the "JSON lines only" rule.
            def log_message(self, fmt, *args):  # noqa: A003
                return

            def _respond(self, status: int, payload: Dict) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _dispatch(self) -> None:
                parsed = urlparse(self.path)
                name = parsed.path.strip("/")
                handler = routes.get(name)
                if handler is None:
                    self._respond(404, {"error": f"unknown command {name!r}",
                                        "available": sorted(routes)})
                    return

                params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

                # A POST body may carry JSON parameters instead of a query string.
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    try:
                        body = json.loads(self.rfile.read(length).decode("utf-8"))
                        if isinstance(body, dict):
                            params.update({k: str(v) for k, v in body.items()})
                    except (ValueError, UnicodeDecodeError):
                        self._respond(400, {"error": "body is not valid JSON"})
                        return

                try:
                    result = handler(params)
                    self._respond(200, result)
                except KeyError as exc:
                    self._respond(400, {"error": f"missing parameter {exc}"})
                except Exception as exc:  # noqa: BLE001 - never kill the server
                    log.exception(
                        "control command failed",
                        extra={"event": "control_command_failed", "command": name},
                    )
                    self._respond(500, {"error": str(exc)})

            do_GET = _dispatch
            do_POST = _dispatch

        self._httpd = ThreadingHTTPServer(("0.0.0.0", self.port), RequestHandler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, name=f"{self.service}-control", daemon=True
        )
        self._thread.start()
        self.log.info(
            "control endpoint listening",
            extra={
                "event": "control_started",
                "port": self.port,
                "commands": sorted(self._routes),
            },
        )
