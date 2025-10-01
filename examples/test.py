# from pyspark.sql import functions as F
# from pyspark.sql.window import Window

# from wikimedia_yard_reaas_test.utils import read_delta_table, create_spark, write_delta
# import matplotlib.pyplot as plt

# # from wikimedia_yard_reaas_test.data_quirks import bucket_pages_adaptive, anti_seasonality


# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
# from pyspark.sql import DataFrame

# from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql.types import StructType, StructField, StringType, LongType
# from pyspark.sql.functions import (
#     input_file_name,
#     substring_index,
#     substring,
#     col,
#     split,
#     array_contains,
#     when,
#     size,
# )
# from pyspark.sql import functions as F
# from delta import configure_spark_with_delta_pip

# from typing import Optional, Union, Callable
# from pathlib import Path
# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
# from pyspark.sql.functions import col


# def write_delta(
#     df: DataFrame,
#     delta_path: Union[Path, str],
#     mode_str: str = "append",
#     partition: str = "file_date",
# ) -> None:
#     """
#     todo_move to utils
#     Write DataFrame to Delta Table partitioned by file_date.

#     Args:
#         df (DataFrame): Input dataframe
#         delta_path (str): Destination folder for Delta table
#     """
#     (
#         df.write.format("delta")
#         .option("compression", "snappy")
#         .mode(mode_str)
#         .partitionBy(partition)
#         .save(delta_path)
#     )
#     print(f"✅ Delta Table written to {delta_path}")


# def read_delta_table(spark: SparkSession, delta_table_path: Union[Path, str]) -> DataFrame:
#     """
#     todo: move in utils
#     Read Delta table.

#     Args:
#         spark (SparkSession): active Spark session
#         delta_table_path (str): path to Delta table

#     Returns:
#         DataFrame: delta table dataframe
#     """
#     return spark.read.format("delta").load(delta_table_path)


# def bucket_pages_adaptive(
#     df: DataFrame,
#     pct: float = 0.05,  # rare = bottom 5% daily views
#     approx_accuracy: int = 1000,  # percentile_approx accuracy (lower=faster)
#     min_floor: int = 5,  # never set the cutoff below this
#     max_cap: int = 50,  # never set the cutoff above this
#     top_k: int = 500,  # always keep the daily Top-K pages
#     whitelist_titles: list[str] | None = None,
#     whitelist_regexes: list[str] | None = None,
# ) -> tuple[DataFrame, DataFrame]:
#     """
#     Returns:
#       bucketed_with_share: file_date, language, database_name, page_bucket, views_bucket, share
#       thresholds: per-day rare threshold used (for auditing)
#     """

#     # 1) Daily per-page views
#     daily = df.groupBy(
#         "file_date", "language", "database_name", "page_title", "is_mobile", "namespace"
#     ).agg(F.sum("count_views").alias("views_page"))

#     # 2) Adaptive threshold per (file_date, language, database_name)
#     thresholds = (
#         daily.groupBy("file_date", "language", "database_name", "is_mobile", "namespace")
#         .agg(F.percentile_approx("views_page", F.lit(pct), approx_accuracy).alias("pct_thr"))
#         .withColumn(
#             "rare_thr", F.least(F.greatest(F.col("pct_thr"), F.lit(min_floor)), F.lit(max_cap))
#         )
#     )

#     # 3) Daily Top-K pages (always keep)
#     w = Window.partitionBy(
#         "file_date", "language", "database_name", "is_mobile", "namespace"
#     ).orderBy(F.desc("views_page"))
#     ranked = daily.withColumn("rank_page", F.row_number().over(w)).join(
#         thresholds, ["file_date", "language", "database_name", "is_mobile", "namespace"], "left"
#     )

#     # 4) Whitelist flags (titles + regex)
#     is_whitelist_title = F.lit(False)
#     if whitelist_titles:
#         is_whitelist_title = F.col("page_title").isin(list(set(whitelist_titles)))

#     is_whitelist_regex = F.lit(False)
#     if whitelist_regexes:
#         combined = "|".join([f"(?:{p})" for p in whitelist_regexes])  # non-capturing groups
#         is_whitelist_regex = F.col("page_title").rlike(combined)

#     flagged = (
#         ranked.withColumn("is_whitelist", is_whitelist_title | is_whitelist_regex).withColumn(
#             "is_topk", F.col("rank_page") <= F.lit(top_k)
#         )
#         # rare if <= threshold AND not whitelisted AND not Top-K
#         .withColumn(
#             "is_rare",
#             (F.col("views_page") <= F.col("rare_thr"))
#             & (~F.col("is_whitelist"))
#             & (~F.col("is_topk")),
#         )
#     )

#     # 5) Create bucket
#     # Keep page title if NOT rare (i.e., whitelisted, Top-K, or above threshold); else bucket to "Other"
#     bucketed = flagged.withColumn(
#         "page_bucket", F.when(F.col("is_rare"), F.lit("Other")).otherwise(F.col("page_title"))
#     )

#     # 6) Aggregate by bucket + compute daily share
#     bucketed_agg = bucketed.groupBy(
#         "file_date", "language", "database_name", "page_bucket", "is_mobile", "namespace"
#     ).agg(F.sum("views_page").alias("views_bucket"))
#     totals = bucketed_agg.groupBy(
#         "file_date", "language", "database_name", "is_mobile", "namespace"
#     ).agg(F.sum("views_bucket").alias("total_views"))
#     bucketed_with_share = bucketed_agg.join(
#         totals, ["file_date", "language", "database_name", "is_mobile", "namespace"], "left"
#     ).withColumn("share", F.col("views_bucket") / F.col("total_views"))

#     return bucketed_with_share, thresholds


# def anti_seasonality(df: DataFrame) -> DataFrame:
#     # Convert file_date to integer for rangeBetween
#     df_days = df.withColumn("date_int", F.datediff(F.col("file_date"), F.lit("2025-01-01")))

#     # Past 6 days + current
#     w_past = Window.partitionBy("language", "database_name").orderBy("date_int").rangeBetween(-6, 0)

#     # Current + next 6 days
#     w_future = (
#         Window.partitionBy("language", "database_name").orderBy("date_int").rangeBetween(0, 6)
#     )

#     df_norm_rolling = (
#         df_days
#         # Past stats
#         .withColumn("rolling_mean_past", F.avg("views_bucket").over(w_past))
#         # Future stats
#         .withColumn("rolling_mean_future", F.avg("views_bucket").over(w_future))
#         # Rule: if fewer than 7 past days, use future mean
#         .withColumn(
#             "rolling_mean",
#             F.when(F.col("date_int") < 4, F.col("rolling_mean_future")).otherwise(
#                 F.col("rolling_mean_past")
#             ),
#         )
#         .withColumn("views_norm", F.col("views_bucket") / F.col("rolling_mean"))
#         .drop("date_int")
#         .drop("total_views")
#         .drop("views_bucket")
#         .drop("rolling_mean_past")
#         .drop("rolling_mean_future")
#         .drop("rolling_mean")
#         .drop("rolling_mean_past")
#     )

#     return df_norm_rolling


# valid_namespaces = [
#     "Article" "User",
#     "Wikipedia",
#     "WP",
#     "Project",
#     "File",
#     "Image",
#     "MediaWiki",
#     "Template",
#     "TM",
#     "Help",
#     "Category",
#     "Portal",
#     "Draft",
#     "TimedText",
#     "Module",
#     "MOS",
#     "Event",
#     "User talk",
#     "Talk",
#     "WT",
#     "Special",
#     "Help",
#     "Transwiki",
#     "Media",
#     "Wikipedia talk",
#     "File talk",
#     "MediaWiki talk",
#     "Template talk",
#     "Help talk",
#     "Category talk",
#     "Portal talk",
#     "Draft talk",
#     "MOS talk",
#     "TimedText talk",
#     "Module talk",
#     "Event talk",
# ]
# spark = create_spark()

# delta_italian_path = "data/languages/pageviews/2025-01/Italian"


# # delta_italian = read_delta_table(spark, delta_italian_path)  # to change
# # delta_italian.sample(fraction=0.001)

# healthy_data = spark.read.format("delta").load(delta_italian_path)
# # healthy_data.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/wiki_repaired")


# # -----------------------------
# # 1. Apply bucketing + anti-seasonality
# # -----------------------------
# bucketed, thresholds = bucket_pages_adaptive(healthy_data)
# df_norm = anti_seasonality(bucketed)
# clean_df = df_norm.filter(F.col("namespace").isin(valid_namespaces))

# # -----------------------------
# # 2. Define windows
# # -----------------------------
# feature_end = F.to_date(F.lit("2025-01-24"))
# label_start = F.to_date(F.lit("2025-01-25"))
# label_end = F.to_date(F.lit("2025-01-31"))

# # Ensure file_date is cast to date
# clean_df = clean_df.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))


# # -----------------------------
# # 3. Feature aggregation (Jan 1–24)
# # -----------------------------
# features_part = clean_df.filter(F.col("dt") <= feature_end)

# # Base stats
# features = features_part.groupBy(
#     "language", "database_name", "page_bucket", "is_mobile", "namespace"
# ).agg(
#     F.countDistinct("dt").alias("active_days"),
#     F.avg("views_norm").alias("avg_norm"),
#     F.stddev("views_norm").alias("std_norm"),
#     F.max("views_norm").alias("max_norm"),
# )

# # Recency stats
# recent = (
#     features_part.withColumn("d_from_end", F.datediff(feature_end, F.col("dt")))
#     .withColumn("is_last1", (F.col("d_from_end") == 0).cast("int"))
#     .withColumn("is_last3", (F.col("d_from_end") <= 2).cast("int"))
#     .withColumn("is_last7", (F.col("d_from_end") <= 6).cast("int"))
# )

# recent_agg = recent.groupBy(
#     "language", "database_name", "page_bucket", "is_mobile", "namespace"
# ).agg(
#     F.sum(F.when(F.col("is_last1") == 1, F.col("views_norm")).otherwise(0)).alias("views_last1"),
#     F.sum(F.when(F.col("is_last3") == 1, F.col("views_norm")).otherwise(0)).alias("views_last3"),
#     F.sum(F.when(F.col("is_last7") == 1, F.col("views_norm")).otherwise(0)).alias("views_last7"),
# )

# # Join feature tables
# features_final = features.join(
#     recent_agg, ["language", "database_name", "page_bucket", "is_mobile", "namespace"], "left"
# )
