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

# outlier

# -----------------------
# # 2. Aggregate to Daily per project + page
# # -----------------------
# daily_df = silver_df.groupBy("file_date", "database_name", "page_title").agg(
#     F.sum("count_views").alias("views_day")
# )


# # -----------------------
# # 4. Compute Median + MAD per (dt, project)
# # -----------------------

# # Median
# median_df = daily_df.groupBy("file_date", "database_name").agg(
#     F.expr("percentile_approx(views_day, 0.5)").alias("median_views")
# )

# # Join back
# with_median = daily_df.join(median_df, on=["file_date", "database_name"], how="left")

# # Compute absolute deviation from median
# with_dev = with_median.withColumn("abs_dev", F.abs(F.col("views_day") - F.col("median_views")))

# # Median absolute deviation
# mad_df = with_dev.groupBy("file_date", "database_name").agg(
#     F.expr("percentile_approx(abs_dev, 0.5)").alias("mad_views")
# )

# # Join back
# with_mad = with_dev.join(mad_df, on=["file_date", "database_name"], how="left")

# # -----------------------
# # 5. Outlier flag
# # -----------------------
# gold_df = with_mad.withColumn(
#     "is_outlier", (F.col("views_day") > F.col("median_views") + 10 * F.col("mad_views"))
# ).drop(
#     "abs_dev"
# )  # cleanup

# gold_df.show(100)

# -----------------------
# 6. Write Gold
# -----------------------
# gold_path = "data/gold/pageviews_daily/2025-01"
# (
#     gold_df.write
#     .format("parquet")
#     .mode("overwrite")
#     .partitionBy("file_date", "database_name")
#     .save(gold_path)
# )

# print(f"✅ Gold table with outlier flags written to {gold_path}")
