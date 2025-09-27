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

# -----------------------
# 1. Spark session
# -----------------------
spark = (
    SparkSession.builder.appName("Wikimedia Gold Processing")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# -----------------------
# 2. Read Silver
# -----------------------
silver_path = "data/silver/pageviews/2025-01"
silver_df = spark.read.parquet(silver_path)

from pyspark.sql import functions as F, Window


# -----------------------
# 2. Aggregate to Daily per project + page
# -----------------------
daily_df = silver_df.groupBy("file_date", "database_name", "page_title").agg(
    F.sum("count_views").alias("views_day")
)


# -----------------------
# 4. Compute Median + MAD per (dt, project)
# -----------------------

# Median
median_df = daily_df.groupBy("file_date", "database_name").agg(
    F.expr("percentile_approx(views_day, 0.5)").alias("median_views")
)

# Join back
with_median = daily_df.join(median_df, on=["file_date", "database_name"], how="left")

# Compute absolute deviation from median
with_dev = with_median.withColumn("abs_dev", F.abs(F.col("views_day") - F.col("median_views")))

# Median absolute deviation
mad_df = with_dev.groupBy("file_date", "database_name").agg(
    F.expr("percentile_approx(abs_dev, 0.5)").alias("mad_views")
)

# Join back
with_mad = with_dev.join(mad_df, on=["file_date", "database_name"], how="left")

# -----------------------
# 5. Outlier flag
# -----------------------
gold_df = with_mad.withColumn(
    "is_outlier", (F.col("views_day") > F.col("median_views") + 10 * F.col("mad_views"))
).drop(
    "abs_dev"
)  # cleanup

gold_df.show(100)

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
