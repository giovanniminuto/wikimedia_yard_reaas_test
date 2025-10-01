from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from wikimedia_yard_reaas_test.kpis import dau_wiki
from wikimedia_yard_reaas_test.utils import create_spark

import matplotlib.pyplot as plt
import pandas as pd

# 1. Spark session
spark = create_spark()


# 2. DAU Percentage Comparison
dau_pd_list = []
language_names = ["Italian", "English", "German", "Spanish", "French"]

# Collect DAU for each language
for language in language_names:
    path = f"data/languages/pageviews/2025-01/{language}"
    df_lang = spark.read.format("delta").load(path)
    samples_df = df_lang.sample(0.01)

    dau_df = dau_wiki(samples_df)  # returns file_date, DAU
    dau_pd = dau_df.toPandas()
    dau_pd["language"] = language
    dau_pd_list.append(dau_pd)

# Concatenate all languages
all_dau = pd.concat(dau_pd_list)

# Compute daily totals
daily_totals = all_dau.groupby("file_date")["DAU"].sum().reset_index()
daily_totals.rename(columns={"DAU": "DAU_total"}, inplace=True)

# Merge + compute percentage
all_dau = all_dau.merge(daily_totals, on="file_date")
all_dau["DAU_pct"] = all_dau["DAU"] / all_dau["DAU_total"] * 100

# English vs Others
english = all_dau[all_dau["language"] == "English"].copy()
others = (
    all_dau[all_dau["language"] != "English"].groupby("file_date")["DAU_pct"].sum().reset_index()
)
others["language"] = "Others"

compare_df = pd.concat([english[["file_date", "language", "DAU_pct"]], others])

# 3. Plot
fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)

# Left: each language
for language in language_names:
    subset = all_dau[all_dau["language"] == language]
    axes[0].plot(subset["file_date"], subset["DAU_pct"], marker="o", label=language)

axes[0].set_title("Daily Active Usage (%) by Language")
axes[0].set_xlabel("Date")
axes[0].set_ylabel("Share of Distinct Pages (%)")
axes[0].legend()
axes[0].tick_params(axis="x", rotation=45)

# Right: English vs Others
for lang in ["English", "Others"]:
    subset = compare_df[compare_df["language"] == lang]
    axes[1].plot(subset["file_date"], subset["DAU_pct"], marker="o", label=lang)

axes[1].set_title("English vs Others")
axes[1].set_xlabel("Date")
axes[1].legend()
axes[1].tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("dau_percentage_comparison.png", dpi=300, bbox_inches="tight")
plt.show()
