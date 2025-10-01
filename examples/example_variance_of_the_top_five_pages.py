from pyspark.sql import functions as F
import matplotlib.pyplot as plt
from wikimedia_yard_reaas_test.cleaning_pipeline import (
    silver_apply_quality_checks,
    silver_transform_domain_step,
    silver_drop_total_response_size,
    silver_transform_page_title,
)
from wikimedia_yard_reaas_test.maps import get_lang_map_expr
from wikimedia_yard_reaas_test.utils import create_spark, write_delta, read_delta_table


# ----------------------
# example of silver step
# ----------------------

# Spark session
spark = create_spark()

# delta paths
Italian_path = "data/languages/pageviews/2025-01/Italian"

df_ita = read_delta_table(spark, Italian_path)

df = df_ita.sample(fraction=0.05)

# # 1. Identify top 5 pages by total views
# top_pages = (
#     df.groupBy("page_title", "language")
#       .agg(F.sum("count_views").alias("total_views"))
#       .orderBy(F.desc("total_views"))
#       .limit(5)
#       .toPandas()
# )

# print("Top 5 pages:", top_pages[["page_title", "language", "total_views"]])

# # 2. Collect daily views for each top page
# plots_data = []
# for row in top_pages.itertuples():
#     page = row.page_title
#     lang = row.language

#     daily_views = (
#         df.filter((F.col("page_title") == page) & (F.col("language") == lang))
#           .withColumn("dt", F.to_date("file_date"))
#           .groupBy("dt")
#           .agg(F.sum("count_views").alias("views"))
#           .orderBy("dt")
#           .toPandas()
#     )
#     daily_views["page_title"] = page
#     daily_views["language"] = lang
#     daily_views["views_7d_avg"] = daily_views["views"].rolling(window=7, min_periods=1).mean()
#     plots_data.append(daily_views)

# # Combine all results into one DataFrame
# import pandas as pd
# plot_df = pd.concat(plots_data, ignore_index=True)

# plot_df.to_csv('example_variance_of_the_top_five_pages.csv')  # to remove

# # 3. Plot each page in a grid (raw vs smoothed)
# fig, axes = plt.subplots(nrows=5, ncols=1, figsize=(12, 18), sharex=True)

# for (page, lang), ax in zip(top_pages[["page_title", "language"]].values, axes):
#     data = plot_df[(plot_df["page_title"] == page) & (plot_df["language"] == lang)]
#     ax.plot(data["dt"], data["views"], label="Raw daily views", marker="o", alpha=0.6)
#     ax.plot(data["dt"], data["views_7d_avg"], label="7-day rolling avg", linewidth=2, color="red")
#     ax.set_title(f"{page} ({lang})")
#     ax.set_ylabel("Views")
#     ax.grid(True)
#     ax.legend()

# plt.xlabel("Date")
# plt.tight_layout()
# plt.savefig("example_variance_of_the_top_five_pages.png", dpi=300, bbox_inches="tight")
# plt.show()


# 1. Aggregate raw data at daily level for a single page or domain
# lang_example = "Italian"  # optional filter

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# 1. Pick one example page (or small set of pages) to visualize
example_page = "Giacomo_Leopardi"

# Filter to one page
raw_page = df.filter(F.col("page_title") == example_page).select(
    "file_date", "file_time", "count_views"
)

# 2. Aggregate at hourly (or raw file_time) level
hourly = (
    raw_page.groupBy("file_date", "file_time")
    .agg(F.sum("count_views").alias("views_hour"))
    .orderBy("file_date", "file_time")
)

# 3. Aggregate at daily level
daily = (
    raw_page.groupBy("file_date").agg(F.sum("count_views").alias("views_day")).orderBy("file_date")
)

# 4. Collect to Pandas for plotting
hourly_pd = hourly.toPandas()
daily_pd = daily.toPandas()

# 5. Plot noisy raw vs. smoothed daily
plt.figure(figsize=(12, 5))

plt.plot(
    hourly_pd["file_date"] + " " + hourly_pd["file_time"],
    hourly_pd["views_hour"],
    label="Raw hourly views",
    alpha=0.6,
)
plt.plot(
    daily_pd["file_date"],
    daily_pd["views_day"],
    label="Daily aggregated views",
    linewidth=2,
    color="red",
)

plt.xticks(rotation=45)
plt.title(f"Variance in Raw vs. Aggregated Data for {example_page}")
plt.ylabel("Views")
plt.xlabel("Time")
plt.legend()
plt.tight_layout()
plt.show()
