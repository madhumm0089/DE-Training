# Compatre RDD vs DataFrame

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
import json
import time

spark = SparkSession.builder.appName("Load JSON RDD vs DF").getOrCreate()

start_time = time.time()

rdd = spark.sparkContext.textFile('user_logs.json')
parsed_rdd = rdd.map(lambda line:json.loads(line))
print(parsed_rdd.take(2))
# total_duration = rdd.map(lambda row: rdd['duration']).sum()
# print(f"Total duration using rdd: {total_duration}")

data_df = spark.read.json('user_logs.json')
# data_df.show()


aggregated_df = data_df.groupBy('user_id').agg(sum('duration').alias('total_duration'))
aggregated_df.orderBy("user_id").show()

end_time = time.time()
print(f"Execution time:{end_time - start_time}")

input()