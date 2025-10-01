import pyspark.sql.functions as F
import matplotlib.pyplot as plt

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

# path = f"data/languages/pageviews/2025-01/Italian"
# df_lang = spark.read.format("delta").load(path)
# df = df_lang.sample(0.001)
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import pandas as pd

language_names = ["Italian", "English", "German", "Spanish", "French"]
stickiness_list = []

for language in language_names:
    # -----------------------
    # 1. Load data for language
    # -----------------------
    path = f"data/languages/pageviews/2025-01/{language}"
    df_lang = spark.read.format("delta").load(path)
    df_lang = df_lang.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
    df_lang = df_lang.sample(0.5)

    # -----------------------
    # 2. Compute DAU (per day)
    # -----------------------
    dau_df = df_lang.groupBy("dt").agg(F.countDistinct("page_title").alias("DAU")).orderBy("dt")

    # -----------------------
    # 3. Compute MAU (per month)
    # -----------------------
    mau_df = (
        df_lang.withColumn("month", F.date_format("dt", "yyyy-MM"))
        .groupBy("month")
        .agg(F.countDistinct("page_title").alias("MAU"))
    )

    # -----------------------
    # 4. Join DAU + MAU and compute Stickiness
    # -----------------------
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

# -----------------------
# 5. Combine all languages
# -----------------------
all_stickiness = pd.concat(stickiness_list)

# # -----------------------
# # 6. Plot comparison
# # -----------------------
# plt.figure(figsize=(12,6))

# for language in language_names:
#     subset = all_stickiness[all_stickiness["language"] == language]
#     plt.plot(subset["dt"], subset["Stickiness"], marker="o", label=language)

# plt.title("DAU/MAU Stickiness (%) by Language")
# plt.xlabel("Date")
# plt.ylabel("Stickiness (%)")
# plt.legend()
# plt.grid(True)

# plt.savefig("stickiness_by_language.png", dpi=300, bbox_inches="tight")
# plt.show()


import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import datetime

# Assume you already have `all_stickiness` DataFrame from before
# columns: dt (datetime), Stickiness, language

plt.figure(figsize=(12, 6))

# Plot stickiness by language
for language in all_stickiness["language"].unique():
    subset = all_stickiness[all_stickiness["language"] == language]
    plt.plot(subset["dt"], subset["Stickiness"], marker="o", label=language)

# Highlight weekends (January 2025)
start_date = datetime.date(2025, 1, 1)
end_date = datetime.date(2025, 1, 31)
current = start_date

while current <= end_date:
    if current.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        plt.axvspan(current, current + datetime.timedelta(days=1), facecolor="lightgray", alpha=0.3)
    current += datetime.timedelta(days=1)

# Format the x-axis
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
plt.xticks(rotation=45)

# Titles and labels
plt.title("DAU/MAU Stickiness (%) by Language (Jan 2025)")
plt.xlabel("Date")
plt.ylabel("Stickiness (%)")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.6)

plt.tight_layout()
plt.savefig("stickiness_weekend_highlight.png", dpi=300, bbox_inches="tight")
plt.show()
