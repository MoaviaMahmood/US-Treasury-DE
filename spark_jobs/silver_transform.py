from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("SilverTransform").getOrCreate()

# Read bronze
df = spark.read.parquet("/data/bronze/public_debt_transactions")

# Standardize column names (lower + strip)
df = df.toDF(*[c.lower().strip() for c in df.columns])

# Convert date column
df = df.withColumn(
    "record_date",
    to_date(col("record_date"))
)

# Amount columns → double
amount_cols = [
    "transaction_today_amt",
    "transaction_mtd_amt",
    "transaction_fytd_amt"
]

for c in amount_cols:
    df = df.withColumn(c, col(c).cast(DoubleType()))

# Integer columns
int_cols = [
    "src_line_nbr",
    "record_fiscal_year",
    "record_fiscal_quarter",
    "record_calendar_year",
    "record_calendar_quarter",
    "record_calendar_month",
    "record_calendar_day"
]

for c in int_cols:
    df = df.withColumn(c, col(c).cast(IntegerType()))

# Clean string columns (trim spaces)
string_cols = [
    "transaction_type",
    "security_market",
    "security_type",
    "security_type_desc",
    "table_nbr",
    "table_nm"
]

for c in string_cols:
    df = df.withColumn(c, trim(col(c)))

# Replace literal "nan" string with null
df = df.withColumn(
    "security_type_desc",
    when(col("security_type_desc") == "nan", None)
    .otherwise(col("security_type_desc"))
)


# Drop invalid dates
df = df.dropna(subset=["record_date"])

# Remove duplicates
df = df.dropDuplicates()

# Write silver layer
df.write.mode("overwrite").parquet("/data/silver/public_debt_transactions")
