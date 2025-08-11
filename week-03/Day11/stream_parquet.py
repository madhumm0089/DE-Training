from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
# Start Spark session
spark = SparkSession.builder \
    .appName("parquetStream") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("inventory_batch1.csv", header=True, inferSchema=True)
df.show()
df.write.mode("overwrite").parquet("output/parquet")
parquet_df = spark.read.parquet("output/parquet")
parquet_df.show()

query = parquet_df.writeStream \
    .format("parquet") \
    .option("path", "output/parquet") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .outputMode("append") \
    .start()