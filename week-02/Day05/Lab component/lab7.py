from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time

spark = SparkSession.builder \
    .appName("BroadcastJoinDemo") \
    .getOrCreate()

small_df = spark.read.option("header", True).csv("product_info.csv")

large_df = spark.read.option("header", True).csv("transactions.csv")
start_time = time.time()

broadcast_df = broadcast(small_df)
res = large_df.join(broadcast_df, "product_id", 'inner')
res.select('product_id','category','price','region').show()

end_time = time.time()

print(f"execution time:{end_time - start_time}")

start_time = time.time()

result_df = 'brodcast_join',lambda:large_df.join(broadcast_df, "product_id", 'inner')


job_name, job_func = result_df
res = job_func()
result_df=res.select('product_id','category','price','region')

# result = res.rdd.map(lambda row :(
#     row.product_id,
#     row.price
#     row.region
# ))
# result_df = result.toDF(['product_id', 'price','region'])
result_df.show()
end_time = time.time()

print(f"execution time:{end_time - start_time}")




print("Small DF count:", small_df.count())
print("Large DF count:", large_df.count())

# res.show()


# joined = large_df.join(broadcast(small_df), on="store_id")

print("=== Physical Plan with Broadcast Join ===")
# joined.explain(True)
