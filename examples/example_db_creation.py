from pyspark.sql import functions as F
from pyspark.sql.window import Window

from wikimedia_yard_reaas_test.utils import create_spark, read_delta_table, write_delta
import matplotlib.pyplot as plt
from wikimedia_yard_reaas_test.data_quirks import bucket_pages_adaptive, anti_seasonality
from wikimedia_yard_reaas_test.maps import valid_namespaces

delta_italian_path = "data/languages/pageviews/2025-01/Italian"

spark = create_spark()
delta_italian = read_delta_table(spark, delta_italian_path)  # to change


delta_italian.sample(fraction=0.0000001)

delta_italian.show(100)

# -----------------------------
# 1. Apply bucketing + anti-seasonality
# -----------------------------
bucketed, thresholds = bucket_pages_adaptive(delta_italian)

df_norm = anti_seasonality(bucketed)

df_norm.show(100)

clean_df = df_norm.filter(F.col("namespace").isin(valid_namespaces))

# -----------------------------
# 2. Define windows
# -----------------------------
feature_end = F.to_date(F.lit("2025-01-24"))
label_start = F.to_date(F.lit("2025-01-25"))
label_end = F.to_date(F.lit("2025-01-31"))

# Ensure file_date is cast to date
clean_df = clean_df.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))


# -----------------------------
# 3. Feature aggregation (Jan 1–24)
# -----------------------------
features_part = df_norm.filter(F.col("dt") <= feature_end)

# Base stats
features = features_part.groupBy(
    "language", "database_name", "page_bucket", "is_mobile", "namespace"
).agg(
    F.countDistinct("dt").alias("active_days"),
    F.avg("views_norm").alias("avg_norm"),
    F.stddev("views_norm").alias("std_norm"),
    F.max("views_norm").alias("max_norm"),
)

df_test = df_norm.filter(F.col("page_bucket") == "About").count()
# Recency stats
recent = (
    features_part.withColumn("d_from_end", F.datediff(feature_end, F.col("dt")))
    .withColumn("is_last1", (F.col("d_from_end") == 0).cast("int"))
    .withColumn("is_last3", (F.col("d_from_end") <= 2).cast("int"))
    .withColumn("is_last7", (F.col("d_from_end") <= 6).cast("int"))
)

recent_agg = recent.groupBy(
    "language", "database_name", "page_bucket", "is_mobile", "namespace"
).agg(
    F.sum(F.when(F.col("is_last1") == 1, F.col("views_norm")).otherwise(0)).alias("views_last1"),
    F.sum(F.when(F.col("is_last3") == 1, F.col("views_norm")).otherwise(0)).alias("views_last3"),
    F.sum(F.when(F.col("is_last7") == 1, F.col("views_norm")).otherwise(0)).alias("views_last7"),
)

# Join feature tables
features_final = features.join(
    recent_agg, ["language", "database_name", "page_bucket", "is_mobile", "namespace"], "left"
)

# -----------------------------
# 4. Label creation (Jan 25–31)
# -----------------------------
label_part = df_norm.filter((F.col("dt") >= label_start) & (F.col("dt") <= label_end))

# Get distinct page_buckets observed in the label window, keeping metadata
labels_present = label_part.select(
    "language", "database_name", "page_bucket", "is_mobile", "namespace"
).distinct()


labels_present.show(50)

# -----------------------------
# 5. Supervised table = features + label
# -----------------------------

supervised = (
    features_final.join(
        labels_present.withColumnRenamed("language", "lang_l")
        .withColumnRenamed("database_name", "db_l")
        .withColumnRenamed("is_mobile", "mob_l")
        .withColumnRenamed("namespace", "nm_l"),
        on="page_bucket",
        how="left",
    )
    # churn = 0 only if all metadata match, else 1
    .withColumn(
        "churn",
        F.when(
            (F.col("lang_l") == F.col("language"))
            & (F.col("db_l") == F.col("database_name"))
            & (F.col("mob_l") == F.col("is_mobile"))
            & (F.col("nm_l") == F.col("namespace")),
            F.lit(0),
        ).otherwise(F.lit(1)),
    ).drop("lang_l", "db_l", "mob_l", "nm_l")
)

# Optional: filter out pages with too little history
supervised = supervised.filter(F.col("active_days") >= 3)

supervised.show(100, truncate=False)


# write_delta(supervised, delta_path= "data/supervised_English", mode_str="overwrite", partition="active_days")
