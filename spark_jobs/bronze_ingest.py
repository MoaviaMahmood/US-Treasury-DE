from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

df = spark.read.csv(
        "/opt/spark-apps/raw_data/public_debt_transactions.csv",
        header=True,
        inferSchema=True
)

df = df.withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("US_Treasury_API"))

df.write.mode("overwrite").parquet("/data/bronze/public_debt_transactions")

spark.stop()
