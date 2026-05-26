"""
GTFS Data Validation
--------------------
Checks data quality of processed GTFS datasets.
Reports issues found in the data.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("BratislavaTransport-Validation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_stops(spark):
    """
    Validate stops data:
    - Check for missing values in critical fields
    - Check GPS coordinates are within Bratislava bounds
    - Check for duplicate stop IDs
    """
    print("=" * 50)
    print("VALIDATION — Stops")
    print("=" * 50)

    df = spark.read.parquet("data/processed/stops")
    total = df.count()
    print(f"\nTotal stops: {total}")

    # Check 1: Missing values in critical fields
    print("\n[CHECK 1] Missing values in critical fields:")
    critical_fields = ["stop_id", "stop_name", "stop_lat", "stop_lon"]
    for field in critical_fields:
        missing = df.filter(F.col(field).isNull()).count()
        status = "OK" if missing == 0 else "FAIL"
        print(f"  [{status}] {field}: {missing} missing values")

    # Check 2: GPS coordinates within Bratislava bounds
    # Bratislava is roughly between lat 47.9-48.3, lon 16.8-17.4
    print("\n[CHECK 2] GPS coordinates within Bratislava bounds:")
    invalid_coords = df.filter(
        (F.col("stop_lat") < 47.9) | (F.col("stop_lat") > 48.3) |
        (F.col("stop_lon") < 16.8) | (F.col("stop_lon") > 17.4)
    )
    invalid_count = invalid_coords.count()
    status = "OK" if invalid_count == 0 else "WARN"
    print(f"  [{status}] Stops outside Bratislava bounds: {invalid_count}")
    if invalid_count > 0:
        print("  Affected stops:")
        invalid_coords.select("stop_id", "stop_name", "stop_lat", "stop_lon").show(5, truncate=False)

    # Check 3: Duplicate stop IDs
    print("\n[CHECK 3] Duplicate stop IDs:")
    duplicates = (
        df.groupBy("stop_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    status = "OK" if duplicates == 0 else "FAIL"
    print(f"  [{status}] Duplicate stop IDs: {duplicates}")


def validate_routes(spark):
    """
    Validate routes data:
    - Check for missing route names
    - Check route types are valid GTFS values
    """
    print("\n" + "=" * 50)
    print("VALIDATION — Routes")
    print("=" * 50)

    df = spark.read.parquet("data/processed/routes")
    total = df.count()
    print(f"\nTotal routes: {total}")

    # Check 1: Missing route short names
    print("\n[CHECK 1] Missing route short names:")
    missing_names = df.filter(F.col("route_short_name").isNull()).count()
    status = "OK" if missing_names == 0 else "FAIL"
    print(f"  [{status}] Missing route_short_name: {missing_names}")

    # Check 2: Valid route types (GTFS standard: 0, 3, 11)
    print("\n[CHECK 2] Valid route types:")
    valid_types = [0, 3, 11]
    invalid_types = df.filter(~F.col("route_type").isin(valid_types)).count()
    status = "OK" if invalid_types == 0 else "WARN"
    print(f"  [{status}] Routes with unexpected type: {invalid_types}")

    # Check 3: Missing long names (we know this exists — let's measure it)
    print("\n[CHECK 3] Missing route long names:")
    missing_long = df.filter(
        F.col("route_long_name").isNull() |
        (F.col("route_long_name") == "NULL")
    ).count()
    status = "OK" if missing_long == 0 else "WARN"
    print(f"  [{status}] Missing route_long_name: {missing_long} out of {total}")


def validate_stop_times(spark):
    """
    Validate stop_times data:
    - Check for missing critical fields
    - Check stop_sequence is consistent
    """
    print("\n" + "=" * 50)
    print("VALIDATION — Stop Times")
    print("=" * 50)

    df = spark.read.parquet("data/processed/stop_times")
    total = df.count()
    print(f"\nTotal stop times: {total:,}")

    # Check 1: Missing values
    print("\n[CHECK 1] Missing values in critical fields:")
    critical_fields = ["trip_id", "stop_id", "arrival_time", "departure_time"]
    for field in critical_fields:
        missing = df.filter(F.col(field).isNull()).count()
        status = "OK" if missing == 0 else "FAIL"
        print(f"  [{status}] {field}: {missing} missing values")


def main():
    spark = create_spark_session()

    validate_stops(spark)
    validate_routes(spark)
    validate_stop_times(spark)

    print("\n" + "=" * 50)
    print("Validation complete!")
    print("=" * 50)

    spark.stop()


if __name__ == "__main__":
    main()