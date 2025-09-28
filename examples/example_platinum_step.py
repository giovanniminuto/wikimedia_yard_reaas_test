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
    SparkSession.builder.appName("Wikimedia Gold Processing")
    .config("spark.sql.files.maxPartitionBytes", "256MB")
    .config("spark.driver.memory", "10g")
    .config("spark.executor.memory", "10g")
    .config("spark.executor.cores", "7")
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

from wikimedia_yard_reaas_test.filters import language_filter

filtered_only_language_path = "data/filtered_only_italian_language/pageviews_daily/2025-01"


filtered_only_italian_language = language_filter(
    df=filtered_only_language, path=filtered_only_language_path
)


# # -----------------------
# # 6. Write Gold
# # -----------------------

# dates = [row.file_date for row in filtered_only_language.select("file_date").distinct().collect()]
# for d in dates:
#     (
#         filtered_only_language.filter(col("file_date") == d)
#         .write.format("delta")
#         .option("compression", "snappy")
#         .mode("append")
#         .partitionBy("file_date")
#         .save(filtered_only_language_path)
#     )

# print(f"✅ filtered_only_language table with outlier flags written to {filtered_only_language_path}")


# from pyspark.sql import functions as F

# def diversity_metrics(df, label=""):
#     """
#     Compute diversity metrics:
#       - Distinct pages per day
#       - Total views per day
#       - Diversity Index = distinct / total
#       - Shannon Entropy of page distribution
#     """
#     # aggregate per day and page
#     daily_pageviews = (
#         df.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
#           .groupBy("dt", "page_title")
#           .agg(F.sum("count_views").alias("views_day"))
#     )

#     # total + distinct per day
#     daily_stats = (
#         daily_pageviews.groupBy("dt")
#         .agg(
#             F.sum("views_day").alias("total_views"),
#             F.countDistinct("page_title").alias("distinct_pages")
#         )
#         .withColumn("diversity_index",
#                     F.col("distinct_pages") / F.col("total_views"))
#     )

#     # Shannon entropy per day
#     daily_entropy = (
#         daily_pageviews
#         .withColumn("dt", F.to_date("dt"))
#         .withColumn("prob",
#                     F.col("views_day") /
#                     F.sum("views_day").over(Window.partitionBy("dt")))
#         .withColumn("entropy_component",
#                     -F.col("prob") * F.log2("prob"))
#         .groupBy("dt")
#         .agg(F.sum("entropy_component").alias("shannon_entropy"))
#     )

#     result = daily_stats.join(daily_entropy, on="dt", how="left")
#     result = result.withColumn("dataset", F.lit(label))
#     return result

# # --- Before filtering ---
# div_before = diversity_metrics(df, label="before_filter")

# # --- After filtering (remove null languages) ---
# filtered_df = df.filter(F.col("language").isNotNull())
# div_after = diversity_metrics(filtered_df, label="after_filter")

# # Combine to compare
# diversity_compare = div_before.unionByName(div_after)
# diversity_compare.show(truncate=False)
