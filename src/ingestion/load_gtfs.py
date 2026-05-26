"""
GTFS Data Loader
----------------
Reads raw GTFS text files using PySpark and saves them
as Parquet files for efficient downstream processing.
"""

from pyspark.sql import SparkSession
import os


def create_spark_session():
    """Create and configure a local Spark session."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("BratislavaTransport-Ingestion")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_gtfs_file(spark, file_path, file_name):
    """
    Load a single GTFS text file into a Spark DataFrame.
    
    GTFS files are CSV files with a header row.
    """
    print(f"Loading {file_name}...")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )
    
    print(f"  Rows: {df.count():,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Columns names: {df.columns}")
    print()
    
    return df


def save_as_parquet(df, output_path):
    """Save a DataFrame as Parquet format."""
    df.write.mode("overwrite").parquet(output_path)
    print(f"  Saved to: {output_path}")


def main():
    # Paths
    raw_dir = "data/raw"
    processed_dir = "data/processed"
    
    # GTFS files we want to process
    gtfs_files = [
        "routes",
        "stops", 
        "trips",
        "stop_times",
        "calendar",
        "calendar_dates",
    ]
    
    # Start Spark
    spark = create_spark_session()
    print("Spark session started successfully!\n")
    
    # Process each file
    for file_name in gtfs_files:
        file_path = os.path.join(raw_dir, f"{file_name}.txt")
        
        if not os.path.exists(file_path):
            print(f"Skipping {file_name} - file not found")
            continue
        
        # Load
        df = load_gtfs_file(spark, file_path, file_name)
        
        # Save as Parquet
        output_path = os.path.join(processed_dir, file_name)
        save_as_parquet(df, output_path)
    
    print("\nAll files processed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()