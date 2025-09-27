from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import input_file_name, regexp_extract, substring_index
from pyspark.sql import functions as F
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name, regexp_extract, substring_index

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


# Add source_file column
df_with_path = raw_df.withColumn("source_file", substring_index(input_file_name(), "/", -1))

# Regex to capture YYYYMMDD and HHMMSS from the filename
pattern = r"pageviews-(\d{8})-(\d{6})(?:\..*)?$"

df_with_date_time = (
    df_with_path.withColumn("file_date_str", regexp_extract("source_file", pattern, 1))
    .withColumn("file_time_str", regexp_extract("source_file", pattern, 2))
    .withColumn(
        "file_timestamp",
        F.to_timestamp(F.concat_ws("", "file_date_str", "file_time_str"), "yyyyMMddHHmmss"),
    )
    .withColumn("file_date", F.to_date("file_timestamp"))
    .withColumn("file_time", F.date_format("file_timestamp", "HH:mm:ss"))  # keep as string
    .drop("file_date_str", "file_time_str", "file_timestamp")
)

df_with_date_time.show(10)

# -----------------------
# 4. Write to Bronze layer
# -----------------------
bronze_path = "data/bronze/pageviews/2025-01"

(
    df_with_date_time.write.format("parquet")  # or "parquet"
    .mode("overwrite")  # full refresh for now
    .save(bronze_path)
)

print(f"Bronze table written to {bronze_path}")
