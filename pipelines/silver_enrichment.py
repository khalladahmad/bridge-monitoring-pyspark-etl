from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SilverEnrichment").master("local[*]").getOrCreate()

# Load static metadata
bridges = spark.read.csv("metadata/bridges.csv", header=True, inferSchema=True)

sensors = ["temperature", "vibration", "tilt"]
bronze_path = "bronze"
silver_path = "silver"

for sensor in sensors:
    df = spark.readStream.parquet(f"{bronze_path}/{sensor}")
    
    # join with metadata
    df = df.join(bridges, on="bridge_id", how="left")

    # data quality checks
    if sensor == "temperature":
        df = df.filter((col("value").between(-40, 80)))
    elif sensor == "vibration":
        df = df.filter(col("value") >= 0)
    else:  # tilt
        df = df.filter(col("value").between(0, 90))

    query = df.writeStream \
        .format("parquet") \
        .option("path", f"{silver_path}/{sensor}") \
        .option("checkpointLocation", f"checkpoints/silver_{sensor}") \
        .start()

spark.streams.awaitAnyTermination()
