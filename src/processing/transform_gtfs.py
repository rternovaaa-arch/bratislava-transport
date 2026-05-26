"""
GTFS Transformations
--------------------
Business logic for transforming raw GTFS data into
meaningful analytical datasets.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("BratislavaTransport-Processing")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stops_per_route(spark):
    """
    Calculate how many unique stops each route serves.
    Maps route_type codes to human-readable names.
    """
    print("=" * 50)
    print("Stops per route")
    print("=" * 50)

    routes = spark.read.parquet("data/processed/routes")
    trips = spark.read.parquet("data/processed/trips")
    stop_times = spark.read.parquet("data/processed/stop_times")

    # Map route_type numbers to readable names
    routes_named = routes.withColumn(
        "transport_type",
        F.when(F.col("route_type") == 0, "tram")
         .when(F.col("route_type") == 3, "bus")
         .when(F.col("route_type") == 11, "trolleybus")
         .otherwise("other")
    )

    # Join trips with stop_times
    trips_stops = trips.join(stop_times, on="trip_id")

    # Join with routes
    full = trips_stops.join(routes_named, on="route_id")

    # Count unique stops per route
    result = (
        full
        .groupBy("route_id", "route_short_name", "transport_type")
        .agg(F.countDistinct("stop_id").alias("unique_stops"))
        .orderBy(F.desc("unique_stops"))
    )

    print("\nTop 10 routes by number of unique stops:")
    result.show(10, truncate=False)

    result.write.mode("overwrite").parquet("data/analytics/stops_per_route")
    print("Saved to data/analytics/stops_per_route\n")

    return result


def trips_per_hour(spark):
    """
    Count how many trips depart each hour of the day.
    Uses minimum stop_sequence per trip to find true first departure.
    Handles GTFS times > 24:00 (night services).
    """
    print("=" * 50)
    print("Trips per hour of day")
    print("=" * 50)

    stop_times = spark.read.parquet("data/processed/stop_times")

    # Find the minimum stop_sequence for each trip (true first stop)
    first_stops = (
        stop_times
        .groupBy("trip_id")
        .agg(
            F.min("stop_sequence").alias("min_seq"),
            F.first("departure_time").alias("departure_time")
        )
    )

    # Extract hour and normalize times > 24:00
    departures_with_hour = first_stops.withColumn(
        "hour",
        F.split(F.col("departure_time"), ":")[0].cast("int") % 24
    )

    # Count trips per hour
    result = (
        departures_with_hour
        .groupBy("hour")
        .agg(F.count("trip_id").alias("total_trips"))
        .orderBy("hour")
    )

    print("\nNumber of trips departing each hour:")
    result.show(24, truncate=False)

    result.write.mode("overwrite").parquet("data/analytics/trips_per_hour")
    print("Saved to data/analytics/trips_per_hour\n")

    return result

def main():
    spark = create_spark_session()

    stops_per_route(spark)
    trips_per_hour(spark)

    spark.stop()


if __name__ == "__main__":
    main()