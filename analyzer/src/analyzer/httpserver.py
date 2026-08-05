"""Health and metrics HTTP server — one port, /healthz and /metrics, same
convention as the collector and writer services.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST


def make_server(host: str, port: int, registry: CollectorRegistry) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args) -> None:  # noqa: A003 - stdlib signature
            pass  # quiet; the analyzer already logs what matters via logging

        def do_GET(self) -> None:  # noqa: N802 - stdlib method name
            if self.path == "/healthz":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
                return
            if self.path == "/metrics":
                body = generate_latest(registry)
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()

    return ThreadingHTTPServer((host, port), Handler)


def serve_in_background(server: ThreadingHTTPServer) -> threading.Thread:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread
