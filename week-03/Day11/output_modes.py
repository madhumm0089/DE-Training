from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col

# Start Spark session
spark = SparkSession.builder \
    .appName("outputModes") \
    .master("local[*]") \
    .getOrCreate()

df = spark.readStream.format("rate").load()
# df.writeStrea
agg = df.groupBy(window(col("timestamp"), "10 seconds")).count()

query = agg.writeStream\
        .outputMode("complete")\
        .format("console")\
        .start()

query.awaitTermination()