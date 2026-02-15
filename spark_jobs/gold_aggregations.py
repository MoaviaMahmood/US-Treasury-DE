from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, month, year
from pyspark.sql.functions import sum as _sum, col

spark = SparkSession.builder.appName("GoldLayer").getOrCreate()

# Read silver
df = spark.read.parquet("/data/silver/public_debt_transactions")

# Daily aggregation
daily_agg = df.groupBy("record_date") \
    .agg(
        _sum("transaction_today_amt").alias("daily_total")
    )

daily_agg.write.mode("overwrite").parquet("/data/gold/daily_totals")


#  Monthly aggregation
monthly_agg = df.groupBy(
    "record_calendar_year",
    "record_calendar_month"
).agg(
    _sum("transaction_today_amt").alias("monthly_total")
)

monthly_agg.write.mode("overwrite").parquet("/data/gold/monthly_totals")

# monthly_debt_growth
monthly_debt_growth = df.groupBy(
    year("record_date").alias("year"),
    month("record_date").alias("month")
).agg(
    sum("transaction_today_amt").alias("monthly_debt_change")
)

monthly_debt_growth.write.mode("overwrite").parquet("/data/gold/monthly_debt_growth")

#  By security type
security_agg = df.groupBy("security_type") \
    .agg(
        _sum("transaction_today_amt").alias("total_by_security")
    )

security_agg.write.mode("overwrite").parquet("/data/gold/security_totals")


spark.stop()
