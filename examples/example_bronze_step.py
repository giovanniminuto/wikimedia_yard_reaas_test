# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, LongType
# from pyspark.sql.functions import input_file_name, substring_index, col
# from pyspark.sql import functions as F
# from pyspark.sql.functions import input_file_name, substring_index, substring
# from delta import configure_spark_with_delta_pip

# # -----------------------
# # 1. Spark session
# # -----------------------
# builder = (
#     SparkSession.builder.appName("Wikimedia Bronze Processing")
#     .config("spark.sql.files.maxPartitionBytes", "256MB")
#     .config("spark.driver.memory", "12g")
#     .config("spark.executor.memory", "12g")
#     .config("spark.executor.cores", "8")
#     .config("spark.memory.offHeap.enabled", "true")
#     .config("spark.memory.offHeap.size", "3g")
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# )
# spark = configure_spark_with_delta_pip(builder).getOrCreate()

# spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
# spark.conf.set("spark.sql.files.maxRecordsPerFile", 2_000_000)

# # -----------------------
# # 2. Define schema
# # -----------------------
# raw_schema = StructType(
#     [
#         StructField("domain_code", StringType(), True),  # e.g. "en", "es", "fr"
#         StructField("page_title", StringType(), True),  # e.g. "Main_Page"
#         StructField("count_views", LongType(), True),  # number of requests
#         StructField("total_response_size", LongType(), True),  # bytes
#     ]
# )

# # -----------------------
# # 3. Read raw gz files
# # -----------------------
# raw_path = "data/raw/pageviews/2025-01/pageviews-2025013*.gz"  # adjust to your folder
# raw_df = (
#     spark.read.option("sep", " ")  # space-separated
#     .schema(raw_schema)  # enforce schema
#     .csv(raw_path)
# )


# # Add source_file column
# df_with_path = raw_df.withColumn("source_file", substring_index(input_file_name(), "/", -1))


# # keep only proper pageviews files
# df_with_path = df_with_path.filter(df_with_path.source_file.rlike(r"^pageviews-\d{8}-\d{6}"))

# df_with_path.show(10)

# df_parsed = df_with_path.withColumn("file_date_str", substring("source_file", 11, 8)).withColumn(
#     "file_time_str", substring("source_file", 20, 6)
# )


# ts = F.to_timestamp(F.concat_ws("", "file_date_str", "file_time_str"), "yyyyMMddHHmmss")

# df_with_date_time = (
#     df_parsed.withColumn("file_timestamp", ts)
#     .withColumn("file_date", F.to_date("file_timestamp"))
#     .withColumn("file_time", F.date_format("file_timestamp", "HH:mm:ss"))
#     .drop("file_date_str", "file_time_str", "file_timestamp", "source_file")
# )

# df_with_date_time.show(10)

# # -----------------------
# # 3. Write Bronze as Delta
# # -----------------------
# bronze_path = "data/bronze/pageviews/2025-01"

# dates = [row.file_date for row in df_with_date_time.select("file_date").distinct().collect()]
# for d in dates:
#     (
#         df_with_date_time.filter(col("file_date") == d)
#         .write.format("delta")
#         .option("compression", "snappy")
#         .mode("append")
#         .partitionBy("file_date")
#         .save(bronze_path)
#     )

# print(f"✅ Bronze Delta written to {bronze_path}")


from wikimedia_yard_reaas_test.cleaning_pipeline import (
    create_spark,
    get_raw_schema,
    bronze_read_raw_files,
    write_delta,
)

# ----------------------
# example of bronze step
# ----------------------

# Spark session
spark = create_spark()
schema = get_raw_schema()

# data paths
input_dir = "data/raw/pageviews/2025-01/"  # read all files in this folder
bronze_path = "data/bronze/pageviews/2025-01"

# raw file trasformation
df = bronze_read_raw_files(spark, input_dir, schema)

df.show(10, truncate=False)

# Write Silver DF as Delta
write_delta(df, bronze_path)


# todo (late records)
