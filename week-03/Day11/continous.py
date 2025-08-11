from pyspark.sql import SparkSession
# from pyspark.sql.streaming import Trigger
import time

spark = SparkSession.builder\
        .appName("continousMode")\
        .master("local[*]")\
        .getOrCreate()

df = spark.readStream.format("rate").option("rowPerSecond", 10).load()

jvm = spark._jvm
continous_t = jvm.org.apache.spark.sql.streaming.Trigger.Continuous("1 second")

writer = df.writeStream.format("console").outputMode("append").option("truncate", False)


writer._jwrite = writer._jwrite.trigger(continous_t)
query = writer.start()
print("Running in continuous mode for 10 second")
time.sleep(10)
query.stop()