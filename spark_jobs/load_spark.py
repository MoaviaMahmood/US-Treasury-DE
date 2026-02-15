from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GoldToPostgres") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://treasury_postgres:5432/US_Treasury"

connection_properties = {
    "user": "postgres",
    "password": "mavi123",
    "driver": "org.postgresql.Driver"
}

gold_tables = {
    "daily_totals": "/data/gold/daily_totals",
    "monthly_totals": "/data/gold/monthly_totals",
    "monthly_debt_growth": "/data/gold/monthly_debt_growth",
    "security_totals": "/data/gold/security_totals"
}

for table_name, path in gold_tables.items():
    print(f"Loading {table_name}...")

    df = spark.read.parquet(path)

    df.write \
        .mode("overwrite") \
        .jdbc(
            url=jdbc_url,
            table=f"gold_{table_name}",
            properties=connection_properties
        )

    print(f"Loaded {table_name} successfully ")

print("All Gold tables loaded to PostgreSQL successfully ")
