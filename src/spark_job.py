"""Equivalent NASA log analysis implemented with PySpark."""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from nasa_log_analytics.parser import parse_log_line

PARSED_SCHEMA = T.StructType(
    [
        T.StructField("host", T.StringType(), nullable=False),
        T.StructField("timestamp", T.TimestampType(), nullable=False),
        T.StructField("request_date", T.DateType(), nullable=False),
        T.StructField("method", T.StringType(), nullable=False),
        T.StructField("path", T.StringType(), nullable=False),
        T.StructField("protocol", T.StringType(), nullable=False),
        T.StructField("status", T.IntegerType(), nullable=False),
        T.StructField("size", T.LongType(), nullable=True),
    ]
)


def _parse_for_spark(line: str):
    """Return Spark-compatible fields using the shared Python parser."""
    record = parse_log_line(line)
    if record is None:
        return None

    return (
        record.host,
        record.timestamp,
        record.timestamp.date(),
        record.method,
        record.path,
        record.protocol,
        record.status,
        record.size,
    )


def _is_nonblank(line: str) -> bool:
    """Use the same blank-line definition as the Python parser."""
    return bool(line and line.strip())


def analyse_with_spark(
    input_path: str,
    output_path: str,
    top_n: int = 10,
) -> None:
    """Parse and analyse NASA Apache logs with PySpark."""

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = SparkSession.builder.appName("NASAWebLogAnalytics").getOrCreate()

    try:
        raw = spark.read.text(input_path)
        is_nonblank = F.udf(_is_nonblank, T.BooleanType())
        nonblank = raw.filter(is_nonblank("value"))
        parse_record = F.udf(_parse_for_spark, PARSED_SCHEMA)
        parsed = nonblank.select(parse_record("value").alias("record"))
        valid = parsed.filter(F.col("record").isNotNull()).select("record.*").cache()

        total_records = valid.count()
        nonblank_lines = nonblank.count()
        malformed_lines = nonblank_lines - total_records

        print(f"Valid records: {total_records:,}")
        print(f"Malformed records: {malformed_lines:,}")

        print("\nMost requested GET paths")
        (
            valid.filter(F.col("method") == "GET")
            .groupBy("path")
            .count()
            .orderBy(
                F.desc("count"),
                F.asc("path"),
            )
            .show(top_n, truncate=False)
        )

        print("\nRequests by day")
        (
            valid.filter(F.col("request_date").isNotNull())
            .groupBy("request_date")
            .count()
            .orderBy(
                F.desc("count"),
                F.asc("request_date"),
            )
            .show(10, truncate=False)
        )

        print("\nHTTP status codes")
        (valid.groupBy("status").count().orderBy("status").show(truncate=False))

        print("\nResponse-size statistics")
        (
            valid.select(
                F.min("size").alias("minimum"),
                F.max("size").alias("maximum"),
                F.avg("size").alias("average"),
            ).show(truncate=False)
        )

        try:
            (valid.write.mode("overwrite").parquet(output_path))

            print(f"\nParsed records written to {output_path}")

        except Exception as error:
            print("\nCould not write Parquet output.")
            print(
                "This is commonly caused by missing Hadoop/winutils "
                "configuration on Windows."
            )
            print("The analytical calculations completed successfully.")
            print(f"Reason: {type(error).__name__}")

        valid.unpersist()

    finally:
        spark.stop()


def main() -> None:
    """Parse command-line arguments and run the Spark analysis."""

    parser = argparse.ArgumentParser(description="Analyse NASA logs using PySpark.")

    parser.add_argument(
        "log_file",
        nargs="?",
        default="data/nasa_access_log_aug95_sample.txt",
        help="Path to the raw NASA access-log file.",
    )

    parser.add_argument(
        "--output",
        default="outputs/parsed_logs.parquet",
        help="Directory where the parsed Parquet data will be written.",
    )

    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of popular GET paths to display.",
    )

    args = parser.parse_args()

    if args.top <= 0:
        raise SystemExit("--top must be greater than zero.")

    analyse_with_spark(
        input_path=args.log_file,
        output_path=args.output,
        top_n=args.top,
    )


if __name__ == "__main__":
    main()
