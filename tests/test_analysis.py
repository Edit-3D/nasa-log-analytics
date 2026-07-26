from datetime import datetime, timezone

from nasa_log_analytics.analytics import analyse_records
from nasa_log_analytics.parser import LogRecord, parse_lines, parse_log_line
from spark_job import _is_nonblank, _parse_for_spark

VALID_LINE = (
    '159.142.165.138 - - [15/Aug/1995:11:03:22 -0400] '
    '"GET /shuttle/mission.html HTTP/1.0" 200 4179'
)


def test_parse_valid_line():
    record = parse_log_line(VALID_LINE)

    assert record is not None
    assert record.host == "159.142.165.138"
    assert record.method == "GET"
    assert record.path == "/shuttle/mission.html"
    assert record.status == 200
    assert record.size == 4179


def test_missing_size_becomes_none():
    line = (
        'example.org - - [12/Aug/1995:10:38:09 -0400] '
        '"GET /missing.txt HTTP/1.0" 404 -'
    )
    record = parse_log_line(line)

    assert record is not None
    assert record.size is None


def test_malformed_line_is_counted():
    records, malformed = parse_lines([VALID_LINE, "not a log line"])

    assert len(records) == 1
    assert malformed == 1


def test_blank_lines_are_skipped():
    records, malformed = parse_lines(["", "   ", "\t", VALID_LINE])

    assert len(records) == 1
    assert malformed == 0


def test_invalid_typed_fields_are_rejected():
    invalid_timestamp = VALID_LINE.replace(
        "15/Aug/1995:11:03:22",
        "32/Aug/1995:11:03:22",
    )
    invalid_status = VALID_LINE.replace(" 200 4179", " invalid 4179")
    invalid_size = VALID_LINE.replace(" 200 4179", " 200 invalid")

    assert parse_log_line(invalid_timestamp) is None
    assert parse_log_line(invalid_status) is None
    assert parse_log_line(invalid_size) is None


def test_spark_parser_uses_python_validation_contract():
    invalid_lines = [
        "",
        "   ",
        "not a log line",
        VALID_LINE.replace("15/Aug/1995", "32/Aug/1995"),
        VALID_LINE.replace(" 200 4179", " invalid 4179"),
        VALID_LINE.replace(" 200 4179", " 200 invalid"),
    ]

    assert _parse_for_spark(VALID_LINE) is not None
    assert all(_parse_for_spark(line) is None for line in invalid_lines)
    assert all(not _is_nonblank(line) for line in ["", "   ", "\t"])
    assert _is_nonblank(VALID_LINE)


def test_analytics_summary():
    records = [
        LogRecord(
            "a", datetime(1995, 8, 1, tzinfo=timezone.utc),
            "GET", "/a", "HTTP/1.0", 200, 100
        ),
        LogRecord(
            "b", datetime(1995, 8, 1, tzinfo=timezone.utc),
            "GET", "/a", "HTTP/1.0", 404, None
        ),
        LogRecord(
            "c", datetime(1995, 8, 2, tzinfo=timezone.utc),
            "POST", "/b", "HTTP/1.0", 200, 300
        ),
    ]

    summary = analyse_records(records, malformed_lines=2, top_n=5)

    assert summary["total_records"] == 3
    assert summary["malformed_lines"] == 2
    assert summary["top_get_paths"][0] == ("/a", 2)
    assert summary["busiest_day"]["requests"] == 2
    assert summary["successful_responses"] == 2
    assert summary["non_200_responses"] == 1
    assert summary["response_size_bytes"]["average"] == 200


def test_top_get_paths_have_deterministic_tie_ordering():
    timestamp = datetime(1995, 8, 1, tzinfo=timezone.utc)
    records = [
        LogRecord("a", timestamp, "GET", "/z", "HTTP/1.0", 200, 1),
        LogRecord("b", timestamp, "GET", "/a", "HTTP/1.0", 200, 1),
    ]

    summary = analyse_records(records)

    assert summary["top_get_paths"] == [("/a", 1), ("/z", 1)]
