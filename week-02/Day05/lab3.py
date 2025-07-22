from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("revenucalulate").master("local[*]").getOrCreate()

product_df = spark.read.csv('products.csv', header=True, inferSchema=True)
orders_df = spark.read.csv('orders.csv', header=True, inferSchema=True)

join_df = orders_df.join(product_df, orders_df.product_id == product_df.id, how='inner')

revenue_df = join_df.withColumn("revenue", col('quantity') * col('price'))

total_revenue = revenue_df.agg(_sum("revenue").alias('total_revenue')).collect()[0]["total_revenue"]

print(f"Total Revenue: {total_revenue}")

join_df.show()
input()

# product_df.show()
# orders_df.show()
