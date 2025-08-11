from pyspark.sql import SparkSession
# from pyspark.sql.streaming import Trigger
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip
import time

# import org.appache.spark.sql.streaming.trigger
# import org.appache.spark.sql.SparkSession


builder = SparkSession.builder \
    .appName("spark structured streaming") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# import spark.implicits._

#create a dummy streaming source

df =  spark.readStream.format("rate").option("rowPerSecond", 5).load()
df.printSchema()

microbatchquery = df.writeStream.format("console").outputMode("append").option("truncate", False)\
                .queryName("microbatchquery ")\
                .start()
print("Running in microBatch mode for 10 second")
time.sleep(10)
microbatchquery.stop()

# df.writeStream.format("console").trigger(Trigger.Continuous("1 second")).start()