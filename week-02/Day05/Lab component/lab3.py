from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import IntegerType
import time
import pandas as pd

spark = SparkSession.builder.appName("FraudScoring").getOrCreate()

df = spark.read.csv("fraud_transactions.csv", header=True, inferSchema=True)
df.show(3)


def fraud_score(amount, threshold = 1000):
    return 1 if amount > threshold else 0

fraud_udf = udf(fraud_score, IntegerType())

start_time = time.time()
df_udf = df.withColumn("fraud_flag", fraud_udf(df.amount))
df_udf.count()  # trigger execution
print("Standard UDF execution time:", time.time() - start_time)

@pandas_udf("int")
def fraud_score_pandas_udf(amount: pd.Series, threshold: int = 1000) -> pd.Series:
    return amount.apply(lambda x: 1 if x > threshold else 0)

start_time = time.time()
df_pandas_udf = df.withColumn("fraud_flag", fraud_score_pandas_udf(df.amount))
df_pandas_udf.count()  # trigger execution
print("pandas_udf execution time:", time.time() - start_time)

