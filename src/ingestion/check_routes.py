from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("check").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("data/processed/routes")

print("All trolleybuses:")
df.filter(F.col("route_type") == 11)\
  .select("route_id", "route_short_name", "route_long_name")\
  .orderBy("route_short_name")\
  .show(20, truncate=False)

spark.stop()