from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

# -----------------------
# 1. Spark session
# -----------------------
spark = (
    SparkSession.builder.appName("Wikimedia Bronze Ingestion")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.files.maxPartitionBytes", "128MB")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# -----------------------
# 2. Define schema
# -----------------------
raw_schema = StructType(
    [
        StructField("domain_code", StringType(), True),  # e.g. "en", "es", "fr"
        StructField("page_title", StringType(), True),  # e.g. "Main_Page"
        StructField("count_views", LongType(), True),  # number of requests
        StructField("total_response_size", LongType(), True),  # bytes
    ]
)

# -----------------------
# 3. Read raw gz files
# -----------------------
raw_path = "data/raw/pageviews/2025-01/*.gz"  # adjust to your folder
raw_df = (
    spark.read.option("sep", " ")  # space-separated
    .schema(raw_schema)  # enforce schema
    .csv(raw_path)
)
from pyspark.sql.functions import input_file_name, regexp_extract

# Add a column with the full file path
df_with_path = raw_df.withColumn("source_file", input_file_name())

# Extract YYYYMMDD-HHMMSS part from the filename
df_with_date = df_with_path.withColumn(
    "file_datetime", regexp_extract("source_file", r"pageviews-(\d{8}-\d{6})", 1)
)

# If you want separate date and hour fields:
df_with_date = df_with_date.withColumn(
    "file_date", regexp_extract("file_datetime", r"(\d{8})", 1)
).withColumn("file_time", regexp_extract("file_datetime", r"-(\d{6})", 1))
# print(f"Bronze count = {raw_df.count():,}")
df_with_date.show(20)

# -----------------------
# 4. Write to Bronze layer
# -----------------------
bronze_path = "data/bronze/pageviews/2025-01"

(
    df_with_date.write.format("parquet")  # or "parquet"
    .mode("overwrite")  # full refresh for now
    .save(bronze_path)
)

print(f"Bronze table written to {bronze_path}")
