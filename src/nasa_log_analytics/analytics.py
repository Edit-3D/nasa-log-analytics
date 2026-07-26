"""Standard-Python analytics for parsed web-server logs."""

from __future__ import annotations

from collections import Counter
from statistics import mean
from typing import Iterable

from .parser import LogRecord


def analyse_records(
    records: Iterable[LogRecord],
    *,
    malformed_lines: int = 0,
    top_n: int = 10,
) -> dict[str, object]:
    """Calculate the project's core web-log statistics."""
    materialised = list(records)
    if not materialised:
        raise ValueError("No valid log records were supplied.")

    get_paths = Counter(
        record.path for record in materialised if record.method == "GET"
    )
    requests_by_day = Counter(
        record.timestamp.date().isoformat() for record in materialised
    )
    status_counts = Counter(record.status for record in materialised)
    sizes = [record.size for record in materialised if record.size is not None]

    busiest_day, busiest_day_requests = requests_by_day.most_common(1)[0]

    return {
        "total_records": len(materialised),
        "malformed_lines": malformed_lines,
        "top_get_paths": sorted(
            get_paths.items(),
            key=lambda item: (-item[1], item[0]),
        )[:top_n],
        "busiest_day": {
            "date": busiest_day,
            "requests": busiest_day_requests,
        },
        "status_counts": dict(sorted(status_counts.items())),
        "successful_responses": status_counts.get(200, 0),
        "non_200_responses": sum(
            count for status, count in status_counts.items() if status != 200
        ),
        "response_size_bytes": {
            "minimum": min(sizes) if sizes else None,
            "maximum": max(sizes) if sizes else None,
            "average": mean(sizes) if sizes else None,
        },
    }
