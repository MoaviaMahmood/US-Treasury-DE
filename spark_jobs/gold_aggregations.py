from pyspark.sql.functions import sum, month, year
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GoldAggregations").getOrCreate()
df = spark.read.parquet("/data/silver/public_debt_transactions")

gold_df = df.groupBy(
    year("record_date").alias("year"),
    month("record_date").alias("month")
).agg(
    sum("transaction_today_amt").alias("monthly_debt_change")
)

gold_df.write.mode("overwrite").parquet("/data/gold/monthly_debt_growth")
