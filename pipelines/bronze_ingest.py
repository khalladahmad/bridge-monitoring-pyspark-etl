from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date

spark = SparkSession.builder \
    .appName("BronzeIngest") \
    .master("local[*]") \
    .getOrCreate()

# Paths
landing_paths = {
    "temperature": "streams/bridge_temperature",
    "vibration": "streams/bridge_vibration",
    "tilt": "streams/bridge_tilt"
}
bronze_path = "bronze"

for sensor, path in landing_paths.items():
    df = spark.readStream.json(path)
    # parse event_time and add processing columns
    df = df.withColumn("event_time_ts", col("event_time").cast("timestamp")) \
           .withColumn("ingest_time_ts", current_timestamp()) \
           .withColumn("partition_date", to_date(col("event_time_ts")))

    query = df.writeStream \
        .format("parquet") \
        .option("path", f"{bronze_path}/{sensor}") \
        .option("checkpointLocation", f"checkpoints/bronze_{sensor}") \
        .start()

spark.streams.awaitAnyTermination()
