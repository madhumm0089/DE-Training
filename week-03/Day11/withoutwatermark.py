from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col

# Start Spark session
spark = SparkSession.builder \
    .appName("WithoutWatermark") \
    .master("local[*]") \
    .getOrCreate()

# Simulated input stream (rate source)
stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# Windowed aggregation without watermark
windowed_counts = stream_df \
    .groupBy(window(col("timestamp"), "10 seconds")) \
    .count()

# Print streaming output to console
query = windowed_counts.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", False) \
    .option("checkpointLocation", "file:///C:/Users/madhu.m/Desktop/DE-Training/week-03/Day11/checkpoints/console") \
    .option("path", "file:///C:/Users/madhu.m/Desktop/DE-Training/week-03/Day11/delta/query_table") \
    .start()

print("Streaming WITHOUT watermark. Output below. Please wait...")

# Wait for query termination
# query.awaitTermination()  # Keep process alive to print output continuously

# query.write.format("delta").mode("overwrite").save("delta/query_table")
