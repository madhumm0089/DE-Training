from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("PartitionPruningDemo") \
    .getOrCreate()

data = [(1, "A", 100.0, 2022), (2, "B", 200.0, 2023), (3, "C", 150.0, 2023)]
cols = ["id", "product", "amount", "year"]
df = spark.createDataFrame(data, cols)

df.write.partitionBy("year").parquet("data/sales_data", mode="overwrite")
df = spark.read.parquet("data/sales_data")




print("=== Schema ===")
df.printSchema()

print("=== Filtered Query Physical Plan ===")
df.filter("year = 2023").explain(True)

df.filter("year = 2022").show()

