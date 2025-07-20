from pyspark.sql import SparkSession  # ✅ Corrected class name
from pyspark.sql.functions import col  # ✅ Corrected import
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType  # ✅ You need this for schema


spark = SparkSession.builder\
    .appName("ParquetProcessor")\
    .config("spark.sql.shuffle.partitions", "200")\
    .config("spark.sql.autoBroadcastJoinThreshold", 104857600)\
    .getOrCreate()

csv_path = 'C:\Users\madhu.m\Desktop\DE-Training\DE-Training\week01 case study\spark\parking_events_large.csv'
parquet_output_path = 'C:\Users\madhu.m\Desktop\DE-Training\DE-Training\week01 case study\spark\output.parquet'


schema = StructType([
    StructField("event_date", DateType(), True),
    StructField("dim_id", StringType(), True),
    StructField("metric", FloatType(), True)
])

csv_df = spark.read.csv(
    csv_path,
    schema=schema,
    header=True,
    mode="DROPMALFORMED",
    multiLine=False
)


filtered_df = csv_df.filter(col("event_date").isNotNull())


estimated_size_mb = 10000  
target_partition_size_mb = 200
num_partitions = max(1, estimated_size_mb // target_partition_size_mb)
repartitioned_df = filtered_df.repartition(num_partitions)


repartitioned_df.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet(parquet_output_path)


spark.stop()
