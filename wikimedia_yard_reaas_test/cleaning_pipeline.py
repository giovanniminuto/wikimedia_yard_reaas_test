from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import (
    input_file_name,
    substring_index,
    substring,
    col,
    split,
    array_contains,
    when,
    size,
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col

from typing import Optional, Union, Callable, List
from pathlib import Path
import datetime

from wikimedia_yard_reaas_test.utils import write_delta
from wikimedia_yard_reaas_test.maps import valid_namespaces

# bronze


def bronze_get_raw_schema() -> StructType:
    """
    Return schema for raw Wikimedia pageviews.
    """
    return StructType(
        [
            StructField("domain_code", StringType(), True),  # e.g. "en", "es", "fr"
            StructField("page_title", StringType(), True),  # e.g. "Main_Page"
            StructField("count_views", LongType(), True),  # number of requests
            StructField("total_response_size", LongType(), True),  # bytes
        ]
    )


def bronze_read_and_modify_raw_files(
    spark: SparkSession, input_dir: str, schema: StructType
) -> DataFrame:
    """
    Read all raw pageviews files in a directory and attach file metadata.

    Args:
        spark (SparkSession): Active Spark session
        input_dir (str): Directory containing .gz pageview files
        schema (StructType): Schema to enforce

    Returns:
        DataFrame with additional columns:
            - source_file
            - file_date
            - file_time
    """
    raw_df = (
        spark.read.option("sep", " ")  # space-separated
        .schema(schema)
        .csv(input_dir)  # read all files in folder
    )

    df_with_path = raw_df.withColumn("source_file", substring_index(input_file_name(), "/", -1))

    # Only keep proper pageviews-* files
    df_with_path = df_with_path.filter(df_with_path.source_file.rlike(r"^pageviews-\d{8}-\d{6}"))

    # Extract file_date, file_time
    df_parsed = df_with_path.withColumn(
        "file_date_str", substring("source_file", 11, 8)
    ).withColumn("file_time_str", substring("source_file", 20, 6))

    ts = F.to_timestamp(F.concat_ws("", "file_date_str", "file_time_str"), "yyyyMMddHHmmss")

    df_with_date_time = (
        df_parsed.withColumn("file_timestamp", ts)
        .withColumn("file_date", F.to_date("file_timestamp"))
        .withColumn("file_time", F.date_format("file_timestamp", "HH:mm:ss"))
        .drop("file_date_str", "file_time_str", "file_timestamp", "source_file")
    )

    return df_with_date_time


# silver
def silver_apply_quality_checks(df: DataFrame) -> DataFrame:
    """
    Apply sanity checks:
    - count_views within valid range
    - domain_code not null
    - page_title not '-'
    - page_title not null

    """
    return (
        df.filter((F.col("count_views") >= 0) & (F.col("count_views") <= 1_000_000_000))
        .filter(col("domain_code").isNotNull())
        .filter(col("page_title") != "-")
        .filter(col("page_title").isNotNull())
    )


def silver_transform_domain_step(df: DataFrame, lang_map_expr: Callable) -> DataFrame:
    """
    Derive language, database_name, and is_mobile fields from domain_code,
    and filter out special domain codes (commons, meta, incubator, etc.).

    Args:
        df (DataFrame): Input DataFrame with 'domain_code'.
        lang_map_expr (Callable): Function returning a Spark map expression
                                  for mapping language codes.

    Returns:
        DataFrame: Transformed DataFrame without special domain codes.
    """
    parts = split(col("domain_code"), "\\.")

    # List of special domains we want to filter out
    special_domain_codes = [
        "commons",
        "meta",
        "incubator",
        "species",
        "strategy",
        "outreach",
        "usability",
        "quality",
    ]

    # Filter out special domains completely
    filtered_df = df.filter(~parts.getItem(0).isin(special_domain_codes))

    lang_map = lang_map_expr()

    # Main transformation
    transformed_df = (
        filtered_df.withColumn("language", lang_map[parts.getItem(0)])
        .withColumn(
            "database_name",
            when(array_contains(parts, "voy"), "wikivoyage")
            .when(array_contains(parts, "b"), "wikibooks")
            .when(array_contains(parts, "q"), "wikiquote")
            .when(array_contains(parts, "n"), "wikinews")
            .when(array_contains(parts, "s"), "wikisource")
            .when(array_contains(parts, "v"), "wikiversity")
            .when(array_contains(parts, "d"), "wiktionary")
            .when(array_contains(parts, "w"), "mediawikiwiki")
            .when(array_contains(parts, "wd"), "wikidatawiki")
            .when(array_contains(parts, "f"), "foundationwiki")
            .when(col("language").isNotNull(), "wikipedia.org"),
        )
        .withColumn(
            "is_mobile",
            when(array_contains(parts, "m"), True).otherwise(False),
        )
    )

    no_language_filtered_df = transformed_df.filter(col("language").isNotNull())

    return no_language_filtered_df


def silver_drop_total_response_size(df: DataFrame) -> DataFrame:
    """
    Drop total_response_size column (not useful downstream).
    """
    return df.drop("total_response_size")


def silver_transform_page_title(df: DataFrame) -> DataFrame:
    """
    Split page_title into namespace and cleaned title.
    - If prefix exists (e.g. 'Special:AbuseLog'), split on first ':'.
    - Else, assign namespace='Article'.
    """

    df_name_spaces = df.withColumn(
        "namespace",
        when(
            size(split(col("page_title"), ":", 2)) > 1, split(col("page_title"), ":", 2).getItem(0)
        ).otherwise("Article"),
    ).withColumn(
        "page_title",
        when(
            size(split(col("page_title"), ":", 2)) > 1, split(col("page_title"), ":", 2).getItem(1)
        ).otherwise(col("page_title")),
    )

    clean_df = df_name_spaces.filter(F.col("namespace").isin(valid_namespaces))

    return clean_df


# Gold


def language_filter(
    df: DataFrame,
    languages: Union[str, list[str]] = "English",
    path: Optional[Union[Path, str]] = None,
) -> list[DataFrame]:
    """
    Filter the DataFrame by one or more languages and optionally write to Delta.

    Args:
        df (DataFrame): Input DataFrame with at least 'language' and 'file_date'.
        languages (str or list): Language(s) to filter by (e.g., "English", ["English","German"]).
        path (Union[Path, str], optional): If provided, filtered data is written to Delta,
                                           partitioned by file_date.

    Returns:
        list[DataFrame]: a list with all the Filtered DataFrames
    """

    if isinstance(languages, str):
        languages = [languages]

    list_filtered_df = []

    for language in languages:
        filtered_df = df.filter(col("language") == language)

        if path:
            write_delta(df=filtered_df, delta_path=str(path) + "/" + language)

        list_filtered_df.append(filtered_df)

    return list_filtered_df


def optimize_and_vacuum(spark: SparkSession, silver_path: Union[Path, str]) -> None:
    """
    Run Delta optimizations on Silver table:
      - OPTIMIZE with ZORDER
      - VACUUM old files
    """
    # Optimize with ZORDER for top-page queries
    spark.sql(
        f"""
        OPTIMIZE delta.`{silver_path}`
        ZORDER BY (page_title, count_views)
    """
    )

    # Vacuum old files (retain last 7 days)
    spark.sql(
        f"""
        VACUUM delta.`{silver_path}` RETAIN 168 HOURS
    """
    )


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
    # Filter obs + churn
    obs_df = df.filter(
        (F.col("file_date") >= F.lit(obs_start)) & (F.col("file_date") <= F.lit(obs_end))
    )
    lbl_df = df.filter(
        (F.col("file_date") >= F.lit(lbl_start)) & (F.col("file_date") <= F.lit(lbl_end))
    )

    # Daily aggregates
    obs_df = obs_df.withColumn("dow", F.dayofweek("file_date"))  # 1=Sunday, 7=Saturday

    daily = obs_df.groupBy(["file_date", "dow"] + key_cols).agg(
        F.sum("count_views").alias("views_day")
    )

    # Rolling sums (no leakage)
    time_w = Window.partitionBy(key_cols).orderBy(F.col("file_date")).rowsBetween(-6, 0)
    time_w3 = Window.partitionBy(key_cols).orderBy(F.col("file_date")).rowsBetween(-2, 0)
    daily = daily.withColumn("sum_3d", F.sum("views_day").over(time_w3))

    # Last day snapshot
    last_day = (
        daily.filter(F.col("file_date") == F.lit(obs_end))
        .select(key_cols + ["views_day", "sum_3d"])
        .withColumnRenamed("views_day", "views_last_day")
    )

    # Aggregate features
    agg_feats = daily.groupBy(key_cols).agg(
        F.countDistinct("file_date").alias("days_active"),
        F.sum("views_day").alias("views_total"),
        F.avg("views_day").alias("views_mean"),
        F.stddev_pop("views_day").alias("views_std"),
        F.countDistinct("dow").alias("unique_weekdays"),
        F.expr("stddev_pop(views_day)").alias("views_std_dow"),
    )

    # Merge features
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

    # Labels
    alive_keys = lbl_df.select(key_cols).distinct().withColumn("alive_flag", F.lit(1))
    obs_keys = daily.select(key_cols).distinct()
    labels = (
        obs_keys.join(alive_keys, on=key_cols, how="left")
        .withColumn("churn", F.when(F.col("alive_flag").isNull(), 1).otherwise(0))
        .drop("alive_flag")
    )

    # Join features + labels
    ml_dataset = features.join(labels, on=key_cols, how="inner")

    return ml_dataset
