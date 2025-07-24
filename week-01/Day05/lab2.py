from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder \
    .appName('data reader') \
    .master('local[*]') \
    .getOrCreate()

df = spark.read.csv("data.csv", header=True, inferSchema=True)
final_data =  df.filter("paid_amount > 50")
df.select('paid_amount').show()
df.groupBy(expr('paid_amount < 100').alias('low_amount')).count().show()


# df.show()
final_data.show()