from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("DataFrame Lab") \
        .master("local[*]") \
        .getOrCreate()

print("Spark session created")

df = spark.range(1, 1000000).withColumn('squared', col('id')*col('id'))
df.groupBy((col('id')% 10).alias('group')).count().show()