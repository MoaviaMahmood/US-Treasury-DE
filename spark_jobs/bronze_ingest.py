from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

df = spark.read.json("/opt/airflow/app/raw/pdt_raw.json")

df = df.withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("US_Treasury_API"))

df.write.mode("append").parquet("/opt/airflow/app/data/bronze/public_debt_transactions")

spark.stop()
