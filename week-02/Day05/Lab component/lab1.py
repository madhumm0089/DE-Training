from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder \
    .appName('Transaction') \
    .master('local[*]') \
    .getOrCreate()

df = spark.read.csv('transactions.csv', header=True, inferSchema=True)
df.printSchema()

df_repart = df.repartition("product_id")

aggregated_df = df_repart.groupBy("product_id").agg(sum("amount").alias("total_amount"))
aggregated_df.show()

input()