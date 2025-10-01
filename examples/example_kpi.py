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
from wikimedia_yard_reaas_test.kpis import dau_wiki
import matplotlib.pyplot as plt
import pandas as pd

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
# dau_pd_list = []
# language_names = ["Italian", "English", "German", "Spanish", "French"]
# plt.figure(figsize=(10,5))

# for language in language_names:

#     filtered_italian = "data/languages/pageviews/2025-01/" + language
#     filtered_only_language = spark.read.format("delta").load(filtered_italian)
#     samples_df = filtered_only_language.sample(0.00001)


#     filtered_only_language.show(10)


#     dau_df = dau_wiki(samples_df)
#     dau_pd = dau_df.toPandas()


#     dau_pd_list.append(dau_pd)

#     plt.plot(dau_pd["file_date"], dau_pd["DAU"], marker="o", label=language)


# # 3. Plot
# plt.title("Daily Active Usage")
# plt.xlabel("Date")
# plt.ylabel("Distinct Pages")
# plt.legend()

# # 4a. Save the plot (must come before show)
# plt.savefig("dau_trend.png", dpi=300, bbox_inches="tight")

# # 4b. Show the plot
# plt.show()

# # dau_df.show(100)


# dau_pd_list = []
# language_names = ["Italian", "English", "German", "Spanish", "French"]

# plt.figure(figsize=(10,5))

# # Step 1: Collect all DAU in one list
# for language in language_names:
#     path = f"data/languages/pageviews/2025-01/{language}"
#     df_lang = spark.read.format("delta").load(path)
#     samples_df = df_lang.sample(0.00001)

#     dau_df = dau_wiki(samples_df)   # → has file_date, DAU
#     dau_pd = dau_df.toPandas()
#     dau_pd["language"] = language   # tag language

#     dau_pd_list.append(dau_pd)

# # Step 2: Concatenate all languages
# import pandas as pd
# all_dau = pd.concat(dau_pd_list)

# # Step 3: Compute total per day
# daily_totals = all_dau.groupby("file_date")["DAU"].sum().reset_index()
# daily_totals.rename(columns={"DAU": "DAU_total"}, inplace=True)

# # Step 4: Merge + compute percentage
# all_dau = all_dau.merge(daily_totals, on="file_date")
# all_dau["DAU_pct"] = all_dau["DAU"] / all_dau["DAU_total"] * 100

# # Step 5: Plot
# for language in language_names:
#     subset = all_dau[all_dau["language"] == language]
#     plt.plot(subset["file_date"], subset["DAU_pct"], marker="o", label=language)

# plt.title("Daily Active Usage (percentage share)")
# plt.xlabel("Date")
# plt.ylabel("Share of Distinct Pages (%)")
# plt.legend()

# plt.savefig("dau_percentage_trend.png", dpi=300, bbox_inches="tight")
# plt.show()

dau_pd_list = []
language_names = ["Italian", "English", "German", "Spanish", "French"]

# Step 1: Collect DAU for each language
for language in language_names:
    path = f"data/languages/pageviews/2025-01/{language}"
    df_lang = spark.read.format("delta").load(path)
    samples_df = df_lang.sample(0.00001)

    dau_df = dau_wiki(samples_df)  # returns file_date, DAU
    dau_pd = dau_df.toPandas()
    dau_pd["language"] = language

    dau_pd_list.append(dau_pd)

# Step 2: Concatenate
all_dau = pd.concat(dau_pd_list)

# Step 3: Compute daily totals
daily_totals = all_dau.groupby("file_date")["DAU"].sum().reset_index()
daily_totals.rename(columns={"DAU": "DAU_total"}, inplace=True)

# Step 4: Merge + compute %
all_dau = all_dau.merge(daily_totals, on="file_date")
all_dau["DAU_pct"] = all_dau["DAU"] / all_dau["DAU_total"] * 100

# Step 5: English vs Others
english = all_dau[all_dau["language"] == "English"].copy()
others = (
    all_dau[all_dau["language"] != "English"].groupby("file_date")["DAU_pct"].sum().reset_index()
)
others["language"] = "Others"

compare_df = pd.concat([english[["file_date", "language", "DAU_pct"]], others])

# -------------------------
# Plot side-by-side figure
# -------------------------
fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)

# Left: each language
for language in language_names:
    subset = all_dau[all_dau["language"] == language]
    axes[0].plot(subset["file_date"], subset["DAU_pct"], marker="o", label=language)

axes[0].set_title("Daily Active Usage (%) by Language")
axes[0].set_xlabel("Date")
axes[0].set_ylabel("Share of Distinct Pages (%)")
axes[0].legend()

# Right: English vs Others
for lang in ["English", "Others"]:
    subset = compare_df[compare_df["language"] == lang]
    axes[1].plot(subset["file_date"], subset["DAU_pct"], marker="o", label=lang)

axes[1].set_title("English vs Others")
axes[1].set_xlabel("Date")
axes[1].legend()

plt.tight_layout()
plt.savefig("dau_percentage_comparison.png", dpi=300, bbox_inches="tight")
plt.show()
