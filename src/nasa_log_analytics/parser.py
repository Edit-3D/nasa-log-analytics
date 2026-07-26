"""Parsing utilities for NASA Apache Common Log Format records."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

LOG_PATTERN = re.compile(
    r'^(?P<host>\S+)\s+\S+\s+\S+\s+'
    r'\[(?P<timestamp>[^\]]+)\]\s+'
    r'"(?P<method>\S+)\s+(?P<path>.*?)\s+(?P<protocol>[^"]+)"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<size>\S+)$'
)


@dataclass(frozen=True)
class LogRecord:
    """A parsed NASA web-server request."""

    host: str
    timestamp: datetime
    method: str
    path: str
    protocol: str
    status: int
    size: Optional[int]

    def as_dict(self) -> dict[str, object]:
        """Return a serialisable representation of the record."""
        return {
            "host": self.host,
            "timestamp": self.timestamp.isoformat(),
            "method": self.method,
            "path": self.path,
            "protocol": self.protocol,
            "status": self.status,
            "size": self.size,
        }


def parse_log_line(line: str) -> Optional[LogRecord]:
    """Parse one Common Log Format line.

    Malformed lines return ``None`` rather than terminating the whole pipeline.
    A response size of ``-`` is represented as ``None``.
    """
    match = LOG_PATTERN.match(line.strip())
    if match is None:
        return None

    values = match.groupdict()
    try:
        timestamp = datetime.strptime(
            values["timestamp"],
            "%d/%b/%Y:%H:%M:%S %z",
        )
        size = None if values["size"] == "-" else int(values["size"])
        return LogRecord(
            host=values["host"],
            timestamp=timestamp,
            method=values["method"],
            path=values["path"],
            protocol=values["protocol"],
            status=int(values["status"]),
            size=size,
        )
    except (TypeError, ValueError):
        return None


def parse_lines(lines: Iterable[str]) -> tuple[list[LogRecord], int]:
    """Parse an iterable and return valid records plus malformed-line count."""
    records: list[LogRecord] = []
    malformed = 0

    for line in lines:
        if not line.strip():
            continue
        record = parse_log_line(line)
        if record is None:
            malformed += 1
        else:
            records.append(record)

    return records, malformed


def read_log_records(path: str | Path) -> tuple[list[LogRecord], int]:
    """Read and parse all records from a UTF-8-compatible log file."""
    log_path = Path(path)
    if not log_path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")

    with log_path.open("r", encoding="latin-1") as handle:
        return parse_lines(handle)
