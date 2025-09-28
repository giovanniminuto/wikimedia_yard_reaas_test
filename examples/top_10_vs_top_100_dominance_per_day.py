from pyspark.sql import functions as F
from pyspark.sql.window import Window
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


filtered_only_language_path = "data/filtered_only_italian_language/pageviews_daily/2025-01"


filtered_only_language = spark.read.format("delta").load(filtered_only_language_path)


# Make sure file_date is a date
daily_page_df = (
    filtered_only_language.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
    .groupBy("dt", "language", "database_name", "page_title")
    .agg(F.sum("count_views").alias("views_page"))
)

window_spec = Window.partitionBy("dt", "language", "database_name").orderBy(F.desc("views_page"))

ranked_df = daily_page_df.withColumn("rank", F.row_number().over(window_spec))

topN_df = (
    ranked_df.withColumn(
        "top10_views", F.when(F.col("rank") <= 10, F.col("views_page")).otherwise(0)
    )
    .withColumn("top100_views", F.when(F.col("rank") <= 100, F.col("views_page")).otherwise(0))
    .groupBy("dt", "language", "database_name")
    .agg(
        F.sum("views_page").alias("total_views"),
        F.sum("top10_views").alias("top10_views"),
        F.sum("top100_views").alias("top100_views"),
    )
    .withColumn("top10_share", F.col("top10_views") / F.col("total_views"))
    .withColumn("top100_share", F.col("top100_views") / F.col("total_views"))
)


topN_pd = (
    topN_df.filter((F.col("language") == "English") & (F.col("database_name") == "wikipedia.org"))
    .orderBy("dt")
    .toPandas()
)


import matplotlib.pyplot as plt

plt.figure(figsize=(12, 6))

plt.stackplot(
    topN_pd["dt"],
    topN_pd["top10_share"],
    topN_pd["top100_share"] - topN_pd["top10_share"],
    1 - topN_pd["top100_share"],
    labels=["Top 10 pages", "Ranks 11-100", "Long tail"],
    alpha=0.8,
)

plt.title("Top-10 vs Top-100 Dominance (English Wikipedia, Jan 2025)")
plt.ylabel("Share of total views")
plt.xlabel("Date")
plt.legend(loc="upper right")
plt.show()
