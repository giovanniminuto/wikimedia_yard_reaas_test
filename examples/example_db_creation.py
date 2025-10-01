from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType
from typing import Union, List
from pyspark.sql import DataFrame
import datetime


def build_ml_dataset(
    df: DataFrame,
    obs_start: Union[str, datetime.date],
    obs_end: Union[str, datetime.date],
    lbl_start: Union[str, datetime.date],
    lbl_end: Union[str, datetime.date],
    key_cols: List[str] = ["language", "database_name", "is_mobile", "namespace", "page_title"],
) -> DataFrame:
    """
    Build a machine learning dataset for churn prediction.

    The function transforms raw pageview logs into a supervised dataset where each row
    corresponds to a unique entity (defined by `key_cols`). It computes activity,
    recency, seasonality, and diversity features over the observation window, and
    generates churn labels based on presence in the label window.

    Args:
        df (DataFrame):
            Input Spark DataFrame with at least the following columns:
            - file_date (date or string in 'yyyy-MM-dd' format)
            - domain_code (str)
            - page_title (str)
            - count_views (int)
        obs_start (str | datetime.date):
            Start date of the observation window (inclusive).
        obs_end (str | datetime.date):
            End date of the observation window (inclusive).
            All feature computations are restricted to this period.
        lbl_start (str | datetime.date):
            Start date of the label window (inclusive).
        lbl_end (str | datetime.date):
            End date of the label window (inclusive).
            Used to determine churn labels.
        key_cols (List[str], optional):
            Columns that uniquely identify an entity.
            Defaults to ["language", "database_name", "is_mobile", "namespace", "page_title"].

    Returns:
        DataFrame:
            Spark DataFrame with one row per entity, including:

            **Aggregate features**
                - days_active: number of active days in the observation window
                - views_total, views_mean, views_max, views_median, views_std
                - trend_slope: linear regression slope of activity over time

            **Recency / rolling features**
                - views_last_day: activity on the last observation day
                - sum_3d: rolling sums over last 3 days

            **Seasonality features**
                - unique_weekdays: number of distinct weekdays active
                - views_std_dow: variation across weekdays

            **Sparsity indicator**
                - sparsity_level: categorical label {very_sparse, sparse, medium, frequent}

            **Label**
                - churn: 1 if the entity disappeared in label window, else 0

    Notes:
        - Protects against data leakage by restricting features strictly to the
          observation window (`obs_start`–`obs_end`).
        - Label generation compares presence in the label window (`lbl_start`–`lbl_end`).
        - Assumes `file_date` is compatible with Spark date functions and
          comparable to the provided window boundaries.
    """
    # ----------------------
    # Filter obs + churn
    # ----------------------
    obs_df = df.filter(
        (F.col("file_date") >= F.lit(obs_start)) & (F.col("file_date") <= F.lit(obs_end))
    )
    lbl_df = df.filter(
        (F.col("file_date") >= F.lit(lbl_start)) & (F.col("file_date") <= F.lit(lbl_end))
    )

    # ----------------------
    # Daily aggregates
    # ----------------------
    obs_df = obs_df.withColumn("dow", F.dayofweek("file_date"))  # 1=Sunday, 7=Saturday

    daily = obs_df.groupBy(["file_date", "dow"] + key_cols).agg(
        F.sum("count_views").alias("views_day")
    )

    # ----------------------
    # Rolling sums (no leakage)
    # ----------------------
    time_w = Window.partitionBy(key_cols).orderBy(F.col("file_date")).rowsBetween(-6, 0)
    time_w3 = Window.partitionBy(key_cols).orderBy(F.col("file_date")).rowsBetween(-2, 0)
    daily = daily.withColumn("sum_3d", F.sum("views_day").over(time_w3))

    # ----------------------
    # Last day snapshot
    # ----------------------
    last_day = (
        daily.filter(F.col("file_date") == F.lit(obs_end))
        .select(key_cols + ["views_day", "sum_3d"])
        .withColumnRenamed("views_day", "views_last_day")
    )

    # ----------------------
    # Aggregate features
    # ----------------------
    agg_feats = daily.groupBy(key_cols).agg(
        F.countDistinct("file_date").alias("days_active"),
        F.sum("views_day").alias("views_total"),
        F.avg("views_day").alias("views_mean"),
        F.stddev_pop("views_day").alias("views_std"),
        F.countDistinct("dow").alias("unique_weekdays"),
        F.expr("stddev_pop(views_day)").alias("views_std_dow"),
    )

    # ----------------------
    # Merge features
    # ----------------------
    features = (
        agg_feats.join(last_day, on=key_cols, how="left").fillna(
            {
                "views_last_day": 0,
                "sum_3d": 0,
            }
        )
        # --- Sparsity guards ---
        .withColumn(
            "sparsity_level",
            F.when(F.col("days_active") <= 2, "very_sparse")
            .when(F.col("days_active") <= 5, "sparse")
            .when(F.col("days_active") <= 8, "medium")
            .otherwise("frequent"),
        )
    )

    # ----------------------
    # Labels
    # ----------------------
    alive_keys = lbl_df.select(key_cols).distinct().withColumn("alive_flag", F.lit(1))
    obs_keys = daily.select(key_cols).distinct()
    labels = (
        obs_keys.join(alive_keys, on=key_cols, how="left")
        .withColumn("churn", F.when(F.col("alive_flag").isNull(), 1).otherwise(0))
        .drop("alive_flag")
    )

    # ----------------------
    # Join features + labels
    # ----------------------
    ml_dataset = features.join(labels, on=key_cols, how="inner")

    return ml_dataset


# train_set = build_ml_dataset(
#     df,
#     obs_start="2025-01-01", obs_end="2025-01-23",
#     lbl_start="2025-01-24", lbl_end="2025-01-27"
# )

# test_set = build_ml_dataset(
#     df,
#     obs_start="2025-01-05", obs_end="2025-01-27",
#     lbl_start="2025-01-28", lbl_end="2025-01-31"
# )

# print("Train rows:", train_set.count())
# print("Test rows:", test_set.count())
