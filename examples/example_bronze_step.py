from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import input_file_name, regexp_extract, substring_index
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name, regexp_extract, substring_index, substring
from delta import configure_spark_with_delta_pip

# -----------------------
# 1. Spark session
# -----------------------
builder = (
    SparkSession.builder.appName("Wikimedia Bronze Ingestion")
    .config("spark.sql.files.maxPartitionBytes", "256MB")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
spark.conf.set("spark.sql.files.maxRecordsPerFile", 2_000_000)

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


# keep only proper pageviews files
df_with_path = df_with_path.filter(df_with_path.source_file.rlike(r"^pageviews-\d{8}-\d{6}"))

df_with_path.show(10)

df_parsed = df_with_path.withColumn("file_date_str", substring("source_file", 11, 8)).withColumn(
    "file_time_str", substring("source_file", 20, 6)
)


ts = F.to_timestamp(F.concat_ws("", "file_date_str", "file_time_str"), "yyyyMMddHHmmss")

df_with_date_time = (
    df_parsed.withColumn("file_timestamp", ts)
    .withColumn("file_date", F.to_date("file_timestamp"))
    .withColumn("file_time", F.date_format("file_timestamp", "HH:mm:ss"))
    .drop("file_date_str", "file_time_str", "file_timestamp", "source_file")
)

df_with_date_time.show(10)

# -----------------------
# 3. Write Bronze as Delta
# -----------------------
bronze_path = "data/bronze/pageviews/2025-01"

(
    df_with_date_time.repartition("file_date")
    .write.format("delta")
    .option("compression", "snappy")
    .mode("overwrite")
    .partitionBy("file_date")
    .save(bronze_path)
)

print(f"✅ Bronze Delta written to {bronze_path}")
