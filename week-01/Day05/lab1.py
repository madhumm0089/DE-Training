from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('test dataframe') \
    .master('local[*]') \
    .getOrCreate()

countLines = spark.read.text('C:\\Users\\madhu.m\\Desktop\\DE-Training\\DE-Training\\Day06\\sample.txt').count()
print(countLines)
spark.stop()