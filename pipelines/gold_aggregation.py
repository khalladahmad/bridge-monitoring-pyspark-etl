from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, max

spark = SparkSession.builder.appName("GoldAggregation").master("local[*]").getOrCreate()

silver_path = "silver"
gold_path = "gold"

# read silver streams
temp_df = spark.readStream.parquet(f"{silver_path}/temperature") \
    .withWatermark("event_time_ts", "2 minutes") \
    .groupBy("bridge_id", window("event_time_ts", "1 minute")) \
    .agg(avg("value").alias("avg_temperature"))

vib_df = spark.readStream.parquet(f"{silver_path}/vibration") \
    .withWatermark("event_time_ts", "2 minutes") \
    .groupBy("bridge_id", window("event_time_ts", "1 minute")) \
    .agg(max("value").alias("max_vibration"))

tilt_df = spark.readStream.parquet(f"{silver_path}/tilt") \
    .withWatermark("event_time_ts", "2 minutes") \
    .groupBy("bridge_id", window("event_time_ts", "1 minute")) \
    .agg(max("value").alias("max_tilt"))

# join streams
joined_df = temp_df.join(vib_df, ["bridge_id", "window"]) \
                   .join(tilt_df, ["bridge_id", "window"]) \
                   .select("bridge_id", "window.start", "window.end", "avg_temperature", "max_vibration", "max_tilt")

query = joined_df.writeStream \
    .format("parquet") \
    .option("path", f"{gold_path}/bridge_metrics") \
    .option("checkpointLocation", "checkpoints/gold_bridge_metrics") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
