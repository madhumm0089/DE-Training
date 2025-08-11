from pyspark.sql import SparkSession
import time

spark = SparkSession.builder\
        .appName("dummystream")\
        .master("local[*]")\
        .getOrCreate()

df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv").load("https://blobstoragetestvikash551.blob.core.windows.net/csvfiles/customers-100.csv?sp=r&st=2025-07-28T11:51:16Z&se=2025-07-28T20:06:16Z&spr=https&sv=2024-11-04&sr=b&sig=A78JH2wc34Bd6TMrla%2FMRqLWUnLinhRc3xtVYu1%2F7Ec%3D")

df.writeStream.format("console").outputMode("append").option("truncate", False).start()