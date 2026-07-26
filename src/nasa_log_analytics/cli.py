"""Command-line entry point for the Python analysis."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .analytics import analyse_records
from .parser import read_log_records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyse NASA web-server logs using standard Python."
    )
    parser.add_argument(
        "log_file",
        nargs="?",
        default="data/nasa_access_log_aug95_sample.txt",
        help="Path to the raw NASA log file.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of popular GET paths to include.",
    )
    parser.add_argument(
        "--output",
        default="outputs/python_summary.json",
        help="JSON file to write.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.top <= 0:
        raise SystemExit("--top must be greater than zero.")

    records, malformed = read_log_records(args.log_file)
    summary = analyse_records(
        records,
        malformed_lines=malformed,
        top_n=args.top,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    print(json.dumps(summary, indent=2))
    print(f"\nSaved summary to {output_path}")


if __name__ == "__main__":
    main()
