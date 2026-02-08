from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SilverTransform").getOrCreate()
df = spark.read.parquet("/data/bronze/public_debt_transactions")

df_clean = df \
    .dropna(subset=["record_date"]) \
    .withColumn("record_date", col("record_date").cast("date")) \
    .withColumn("transaction_today_amt", col("transaction_today_amt").cast("double")) \
    .dropDuplicates()

df_clean.write.mode("overwrite").parquet("/data/silver/public_debt_transactions")