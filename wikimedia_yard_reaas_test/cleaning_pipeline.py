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

from typing import Optional, Union, Callable
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col


from wikimedia_yard_reaas_test.utils import write_delta


# -----------------------
# Schema
# -----------------------
def get_raw_schema() -> StructType:
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


# -----------------------
# Read raw data
# -----------------------
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

    todo improve description

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
    return df.withColumn(
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


# -----------------------
# Daily Page-level Views
# -----------------------
def compute_daily_pageviews(df: DataFrame, path: Optional[Union[Path, str]] = None) -> DataFrame:
    """
    Compute daily page-level views.

    Args:
        df (DataFrame): Silver DataFrame with at least
            - file_date (date or string 'yyyyMMdd')
            - language (string)
            - database_name (string)
            - page_title (string)
            - count_views (int)

    Returns:
        DataFrame: Aggregated page-level views with columns:
            - dt (date)
            - language
            - database_name
            - page_title
            - views_page (int)
    """
    daily_page = (
        df.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
        .groupBy("dt", "language", "database_name", "page_title")
        .agg(F.sum("count_views").alias("views_page"))
    )

    if path:
        write_delta(df=daily_page, delta_path=path)

    return daily_page


# -----------------------
# Daily Project-level Summary
# -----------------------
def compute_daily_summary(
    daily_page: DataFrame,
    path: Optional[Union[Path, str]] = None,
) -> DataFrame:
    """
    Compute daily summaries per project and optionally write to Delta.

    Metrics:
      - total_views (sum of views across pages)
      - distinct_pages (unique pages viewed)
      - diversity_index (distinct_pages / total_views)

    Args:
        daily_page (DataFrame): Output from compute_daily_pageviews,
            must contain columns ['dt','language','database_name','page_title','views_page'].
        path (Union[Path,str], optional): If provided, writes results to Delta Lake at this path,
            partitioned by 'dt'.

    Returns:
        DataFrame: Daily summary per project with columns:
            - dt (date)
            - language
            - database_name
            - total_views (int)
            - distinct_pages (int)
            - diversity_index (float)
    """
    required_cols = {"dt", "language", "database_name", "page_title", "views_page"}
    if not required_cols.issubset(set(daily_page.columns)):
        raise ValueError(
            f"Missing required columns. Expected {required_cols}, got {set(daily_page.columns)}"
        )

    daily_summary_df = (
        daily_page.groupBy("dt", "language", "database_name")
        .agg(
            F.sum("views_page").alias("total_views"),
            F.countDistinct("page_title").alias("distinct_pages"),
        )
        .withColumn("diversity_index", F.col("distinct_pages") / F.col("total_views"))
    )

    if path:
        write_delta(df=daily_summary_df, delta_path=path)

    return daily_summary_df


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

    # todo add disk warning
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
