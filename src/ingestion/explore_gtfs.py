"""
GTFS Data Explorer
------------------
Quick exploration of our processed GTFS data.
Helps us understand the data before writing transformations.
"""

from pyspark.sql import SparkSession


def create_spark_session():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("BratislavaTransport-Explorer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def explore_routes(spark):
    print("=" * 50)
    print("ROUTES")
    print("=" * 50)

    df = spark.read.parquet("data/processed/routes")

    print("\nSample data:")
    df.select("route_short_name", "route_long_name", "route_type").show(5, truncate=False)

    print("Route types (0=tram, 3=bus, 11=trolleybus):")
    df.groupBy("route_type").count().orderBy("route_type").show()


def explore_stops(spark):
    print("=" * 50)
    print("STOPS")
    print("=" * 50)

    df = spark.read.parquet("data/processed/stops")

    print("\nSample stops:")
    df.select("stop_id", "stop_name", "stop_lat", "stop_lon").show(5, truncate=False)


def explore_stop_times(spark):
    print("=" * 50)
    print("STOP TIMES")
    print("=" * 50)

    df = spark.read.parquet("data/processed/stop_times")

    print("\nSample schedule:")
    df.select("trip_id", "arrival_time", "stop_id", "stop_sequence").show(5, truncate=False)


def main():
    spark = create_spark_session()

    explore_routes(spark)
    explore_stops(spark)
    explore_stop_times(spark)

    spark.stop()


if __name__ == "__main__":
    main()