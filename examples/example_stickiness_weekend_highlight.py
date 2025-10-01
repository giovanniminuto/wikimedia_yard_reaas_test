import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from wikimedia_yard_reaas_test.utils import create_spark

# 1. Spark session

spark = create_spark()

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
spark.conf.set("spark.sql.files.maxRecordsPerFile", 2_000_000)

# 2. Stickiness computation
language_names = ["Italian", "English", "German", "Spanish", "French"]
stickiness_list = []

for language in language_names:
    # Load data for language
    path = f"data/languages/pageviews/2025-01/{language}"
    df_lang = spark.read.format("delta").load(path)
    df_lang = df_lang.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
    df_lang = df_lang.sample(0.05)

    # Compute DAU (per day)
    dau_df = df_lang.groupBy("dt").agg(F.countDistinct("page_title").alias("DAU")).orderBy("dt")

    # Compute MAU (per month)
    mau_df = (
        df_lang.withColumn("month", F.date_format("dt", "yyyy-MM"))
        .groupBy("month")
        .agg(F.countDistinct("page_title").alias("MAU"))
    )

    # Join DAU + MAU and compute Stickiness
    dau_with_month = dau_df.withColumn("month", F.date_format("dt", "yyyy-MM"))
    stickiness_df = (
        dau_with_month.join(mau_df, on="month", how="left")
        .withColumn("Stickiness", (F.col("DAU") / F.col("MAU")) * 100)
        .orderBy("dt")
    )

    # Convert to Pandas and tag with language
    stickiness_pd = stickiness_df.toPandas()
    stickiness_pd["language"] = language
    stickiness_list.append(stickiness_pd)

# 3. Combine and plot
all_stickiness = pd.concat(stickiness_list)

plt.figure(figsize=(12, 6))

for language in all_stickiness["language"].unique():
    subset = all_stickiness[all_stickiness["language"] == language]
    plt.plot(subset["dt"], subset["Stickiness"], marker="o", label=language)

# Highlight weekends (January 2025)
start_date = datetime.date(2025, 1, 1)
end_date = datetime.date(2025, 1, 31)
current = start_date
while current <= end_date:
    if current.weekday() >= 5:  # Saturday=5, Sunday=6
        plt.axvspan(current, current + datetime.timedelta(days=1), facecolor="lightgray", alpha=0.3)
    current += datetime.timedelta(days=1)

# Format the x-axis
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
plt.xticks(rotation=45)

plt.title("DAU/MAU Stickiness (%) by Language (Jan 2025)")
plt.xlabel("Date")
plt.ylabel("Stickiness (%)")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.6)

plt.tight_layout()
plt.savefig("stickiness_weekend_highlight.png", dpi=300, bbox_inches="tight")
plt.show()
