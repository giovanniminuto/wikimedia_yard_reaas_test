from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    input_file_name,
    to_date,
    split,
    array_contains,
    size,
    when,
)
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from wikimedia_yard_reaas_test.maps import get_lang_map_expr
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F, Window

# -----------------------
# 1. Spark session
# -----------------------

builder = (
    SparkSession.builder.appName("Wikimedia KPI Processing")
    .config("spark.sql.files.maxPartitionBytes", "256MB")
    .config("spark.driver.memory", "12g")
    .config("spark.executor.memory", "12g")
    .config("spark.executor.cores", "8")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "4g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
spark.conf.set("spark.sql.files.maxRecordsPerFile", 2_000_000)

# -----------------------
# 2. Read Silver
# -----------------------
filtered_only_language_path = "data/filtered_only_language/pageviews_daily/2025-01"


filtered_only_language = spark.read.format("delta").load(filtered_only_language_path)


filtered_only_language.show(10)


from wikimedia_yard_reaas_test.kpis import dau

dau_df = dau(filtered_only_language)

dau_df.show(100)
