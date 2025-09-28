from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.appName("BankMedallion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

bronze_path = "./bronze/bank_transactions/"
silver_path = "./silver/bank_transactions/"
gold_path = "./gold/bank_transactions_summary/"

# Create sample data
raw_data = [
    (1001, "A001", "Deposit", 5000, "2025-01-01"),
    (1002, "A002", "Withdrawal", -2000, "2025-01-01"),
    (1003, "A001", "Transfer", 3000, "2025-01-02"),
    (1004, "A003", "Deposit", 4000, "2025-13-01"),
    (1005, "A004", "Withdrawal", None, "2025-01-03"),
]

columns = ["txn_id", "account_id", "txn_type", "amount", "txn_date"]
bronze_df = spark.createDataFrame(raw_data, columns)

# Save to Bronze layer in Delta format
bronze_df.write.format("delta").mode("overwrite").save(bronze_path)

# Check the data
bronze_df = spark.read.format("delta").load(bronze_path)
bronze_df.show()

# Load data from Bronze layer
from pyspark.sql.functions import col, to_date, when, count

bronze_df = spark.read.format("delta").load(bronze_path)


# Clean and process the data:
# - Remove missing amounts
# - Remove negative amounts
# - Convert to date
# - Remove invalid dates
# - Add transaction flow
# - Drop the original date column
# - Rename the converted date column

silver_df = (
    bronze_df.filter(col("amount").isNotNull() & (col("amount") > 0))
    # Only attempt to convert valid dates using regexp
    .withColumn(
        "txn_date_converted",
        when(
            col("txn_date").rlike(r"^\d{4}-(0[1-9]|1[0-2])-\d{2}$"),
            to_date(col("txn_date"), "yyyy-MM-dd"),
        ),
    )
    .filter(col("txn_date_converted").isNotNull())
    .withColumn("txn_flow", when(col("txn_type") == "Deposit", "Credit").otherwise("Debit"))
    .drop("txn_date")
    .withColumnRenamed("txn_date_converted", "txn_date")
)

# Save to Silver layer
silver_df.write.format("delta").mode("overwrite").save(silver_path)


# Check the data
silver_df = spark.read.format("delta").load(silver_path)
silver_df.show()

# gold

# Load data from Silver layer
silver_df = spark.read.format("delta").load(silver_path)

# Summarize by transaction type and month
gold_df = silver_df.groupBy("txn_type", to_date(col("txn_date"), "yyyy-MM").alias("month")).agg(
    _sum("amount").alias("total_amount"), count("txn_id").alias("total_transactions")
)

# Save to Gold layer with overwriting (overwriting is faster)
gold_df.write.format("delta").mode("overwrite").save(gold_path)

# Check the final report
gold_df = spark.read.format("delta").load(gold_path)
gold_df.show()
