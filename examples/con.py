# # -----------------------
# # Imports
# # -----------------------
# from pyspark.sql import SparkSession
# from delta import configure_spark_with_delta_pip
# from pyspark.sql import functions as F
# import pandas as pd
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
# import datetime

# # -----------------------
# # 1. Spark session
# # -----------------------
# builder = (
#     SparkSession.builder.appName("Wikipedia Content Diversity")
#     .config("spark.sql.files.maxPartitionBytes", "256MB")
#     .config("spark.driver.memory", "12g")
#     .config("spark.executor.memory", "12g")
#     .config("spark.executor.cores", "8")
#     .config("spark.memory.offHeap.enabled", "true")
#     .config("spark.memory.offHeap.size", "4g")
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# )

# spark = configure_spark_with_delta_pip(builder).getOrCreate()


# # -----------------------
# # 2. Diversity function
# # -----------------------
# def compute_content_diversity(df, group_cols=["dt"]):
#     """
#     Compute content diversity using Shannon entropy over pageviews.

#     Args:
#         df (DataFrame): Spark DataFrame with columns:
#                         - dt (date)
#                         - page_title (string)
#                         - count_views (int)
#         group_cols (list): Columns to group by (default = ["dt"]).

#     Returns:
#         DataFrame: group_cols + ["diversity"] (Shannon entropy)
#     """

#     # Total views per group (e.g., per day)
#     total_views = df.groupBy(group_cols).agg(F.sum("count_views").alias("total_views"))

#     # Views per page per group
#     page_views = df.groupBy(group_cols + ["page_title"]).agg(
#         F.sum("count_views").alias("views_page")
#     )

#     # Join to get probabilities
#     joined = page_views.join(total_views, on=group_cols, how="inner")
#     joined = joined.withColumn("p", F.col("views_page") / F.col("total_views"))

#     # Shannon entropy term: -p * log(p)
#     joined = joined.withColumn("entropy_term", -F.col("p") * F.log(F.col("p")))

#     # Aggregate entropy per group
#     diversity_df = (
#         joined.groupBy(group_cols).agg(F.sum("entropy_term").alias("diversity")).orderBy(group_cols)
#     )

#     return diversity_df


# # -----------------------
# # 3. Process each language
# # -----------------------
# language_names = ["Italian", "English", "German", "Spanish", "French"]
# diversity_list = []

# for language in language_names:
#     # Load data
#     path = f"data/languages/pageviews/2025-01/{language}"
#     df_lang = spark.read.format("delta").load(path)
#     df_lang.sample(fraction=0.01)
#     df_lang = df_lang.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))

#     # Compute diversity
#     diversity_df = compute_content_diversity(df_lang, group_cols=["dt"])
#     diversity_pd = diversity_df.toPandas()
#     diversity_pd["language"] = language

#     diversity_list.append(diversity_pd)

# # -----------------------
# # 4. Combine results
# # -----------------------
# all_diversity = pd.concat(diversity_list)

# # -----------------------
# # 5. Plot
# # -----------------------
# plt.figure(figsize=(12, 6))

# for language in all_diversity["language"].unique():
#     subset = all_diversity[all_diversity["language"] == language]
#     plt.plot(subset["dt"], subset["diversity"], marker="o", label=language)

# # Highlight weekends (January 2025)
# start_date = datetime.date(2025, 1, 1)
# end_date = datetime.date(2025, 1, 31)
# current = start_date

# while current <= end_date:
#     if current.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
#         plt.axvspan(current, current + datetime.timedelta(days=1), facecolor="lightgray", alpha=0.3)
#     current += datetime.timedelta(days=1)

# # Format the x-axis
# plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
# plt.xticks(rotation=45)

# # Titles and labels
# plt.title("Daily Content Diversity (Shannon Entropy) by Language (Jan 2025)")
# plt.xlabel("Date")
# plt.ylabel("Diversity (Entropy)")
# plt.legend()
# plt.grid(True, linestyle="--", alpha=0.6)

# plt.tight_layout()
# plt.savefig("content_diversity_weekend.png", dpi=300, bbox_inches="tight")
# plt.show()
