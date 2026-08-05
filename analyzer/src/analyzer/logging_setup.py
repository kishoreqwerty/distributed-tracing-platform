"""JSON logging, matching the collector/writer services' log/slog JSON
convention so all four services' logs look the same shape in aggregation.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

_STANDARD_FIELDS = frozenset(logging.LogRecord(None, 0, "", 0, "", None, None).__dict__.keys())


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_FIELDS and key != "message":
                payload[key] = value
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
