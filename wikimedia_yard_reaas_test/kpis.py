from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col


def dau_wiki(df: DataFrame) -> DataFrame:
    """
    Compute Daily Active Usage (DAU).

    DAU is defined as the number of distinct pages viewed per day
    for each (language, database_name) pair.

    Args:
        df (DataFrame): Input DataFrame with at least the columns:
            - file_date (string, format "yyyyMMdd")
            - language (string)
            - database_name (string)
            - page_title (string)

    Returns:
        DataFrame: Aggregated DataFrame with schema:
            - file_date (date)
            - language (string)
            - database_name (string)
            - DAU (int): distinct pages viewed on that day
    """
    dau_df = (
        df.groupBy("file_date", "language")
        .agg(F.countDistinct("page_title").alias("DAU"))
        .orderBy("file_date")
    )
    return dau_df


def wau(df: DataFrame) -> DataFrame:
    """
    Compute Weekly Active Usage (WAU).

    WAU is defined as the number of distinct pages viewed per ISO week
    (year-week) for each (language, database_name) pair.

    Args:
        df (DataFrame): Input DataFrame with a `dt` column of type date,
        plus:
            - language (string)
            - database_name (string)
            - page_title (string)

    Returns:
        DataFrame: Aggregated DataFrame with schema:
            - week (string, format "YYYY-ww")
            - language (string)
            - database_name (string)
            - WAU (int): distinct pages viewed in that week
    """
    wau_df = (
        df.withColumn("week", F.date_format("dt", "YYYY-ww"))
        .groupBy("week", "language", "database_name")
        .agg(F.countDistinct("page_title").alias("WAU"))
    )
    return wau_df


def mau(df: DataFrame) -> DataFrame:
    """
    Compute Monthly Active Usage (MAU).

    MAU is defined as the number of distinct pages viewed per month
    for each (language, database_name) pair.

    Args:
        df (DataFrame): Input DataFrame with a `dt` column of type date,
        plus:
            - language (string)
            - database_name (string)
            - page_title (string)

    Returns:
        DataFrame: Aggregated DataFrame with schema:
            - month (string, format "yyyy-MM")
            - language (string)
            - database_name (string)
            - MAU (int): distinct pages viewed in that month
    """
    mau_df = (
        df.withColumn("month", F.date_format("dt", "yyyy-MM"))
        .groupBy("month", "language", "database_name")
        .agg(F.countDistinct("page_title").alias("MAU"))
    )
    return mau_df


def stickness_df(dau_df: DataFrame, mau_df: DataFrame) -> DataFrame:
    """
    Compute the DAU/MAU stickiness ratio.

    Stickiness is a common engagement metric that measures
    how often monthly active content is also active daily.

        stickiness = DAU / MAU

    Args:
        dau_df (DataFrame): DataFrame with daily distinct counts per
            (file_date, language, database_name).
        mau_df (DataFrame): DataFrame with monthly distinct counts per
            (month, language, database_name).

    Returns:
        DataFrame: Joined DataFrame with schema:
            - language (string)
            - database_name (string)
            - file_date (date)
            - month (string)
            - DAU (int)
            - MAU (int)
            - stickiness (float)
    """
    dau_df = dau_df.withColumn("month", F.date_format("file_date", "yyyy-MM"))

    stickiness_df = dau_df.join(
        mau_df, on=["language", "database_name", "month"], how="inner"
    ).withColumn("stickiness", F.col("DAU") / F.col("MAU"))
    return stickiness_df


def diversity(df: DataFrame) -> DataFrame:
    """
    Compute the daily content diversity index for each language/project.

    The diversity index is defined as:
        distinct_pages / total_views
    where:
        - distinct_pages = number of distinct pages viewed on that day
        - total_views    = total number of page views on that day

    Args:
        df (DataFrame): Input dataframe with columns
            - file_date (string, format 'yyyyMMdd')
            - language (string)
            - database_name (string)
            - page_title (string)
            - count_views (int)

    Returns:
        DataFrame: Aggregated dataframe with columns:
            - dt (date)
            - language
            - database_name
            - distinct_pages (int)
            - total_views (int)
            - diversity_index (float)
    """
    diversity_df = (
        df.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
        .groupBy("dt", "language", "database_name")
        .agg(
            F.countDistinct("page_title").alias("distinct_pages"),
            F.sum("count_views").alias("total_views"),
        )
        .withColumn("diversity_index", F.col("distinct_pages") / F.col("total_views"))
    )
    return diversity_df


def shannon_entropy(df: DataFrame) -> DataFrame:
    """
    Compute Shannon entropy of page views per day/project.

    Shannon entropy measures how evenly distributed attention is across pages:
        H = - Σ p * log(p)
    where p = probability of a page being viewed.

    Args:
        df (DataFrame): Input dataframe with columns
            - file_date (string, format 'yyyyMMdd')
            - language (string)
            - database_name (string)
            - page_title (string)
            - count_views (int)

    Returns:
        DataFrame: Aggregated dataframe with columns:
            - dt (date)
            - language
            - database_name
            - shannon_entropy (float)
    """
    page_dist_df = (
        df.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
        .groupBy("dt", "language", "database_name", "page_title")
        .agg(F.sum("count_views").alias("views_page"))
    )

    total_views_df = page_dist_df.groupBy("dt", "language", "database_name").agg(
        F.sum("views_page").alias("total_views")
    )

    prob_df = page_dist_df.join(total_views_df, on=["dt", "language", "database_name"])
    prob_df = prob_df.withColumn("p", F.col("views_page") / F.col("total_views"))

    shannon_entropy_df = (
        prob_df.withColumn("entropy_term", -F.col("p") * F.log(F.col("p")))
        .groupBy("dt", "language", "database_name")
        .agg(F.sum("entropy_term").alias("shannon_entropy"))
    )
    return shannon_entropy_df


def top_10_100_pages(df: DataFrame) -> DataFrame:
    """
    Compute the dominance of top-10 and top-100 pages by views.

    For each day/project:
        - Ranks pages by their daily views.
        - Computes share of total views coming from top-10 and top-100 pages.

    Args:
        df (DataFrame): Input dataframe with columns
            - file_date (string, format 'yyyyMMdd')
            - language (string)
            - database_name (string)
            - page_title (string)
            - count_views (int)

    Returns:
        DataFrame: Aggregated dataframe with columns:
            - dt (date)
            - language
            - database_name
            - total_views (int)
            - top10_views (int)
            - top100_views (int)
            - top10_share (float)
            - top100_share (float)
    """
    page_dist_df = (
        df.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
        .groupBy("dt", "language", "database_name", "page_title")
        .agg(F.sum("count_views").alias("views_page"))
    )

    window_spec = Window.partitionBy("dt", "language", "database_name").orderBy(
        F.desc("views_page")
    )

    ranked_df = page_dist_df.withColumn("rank", F.row_number().over(window_spec))

    topN_df = (
        ranked_df.withColumn(
            "top10_views", F.when(col("rank") <= 10, col("views_page")).otherwise(0)
        )
        .withColumn("top100_views", F.when(col("rank") <= 100, col("views_page")).otherwise(0))
        .groupBy("dt", "language", "database_name")
        .agg(
            F.sum("views_page").alias("total_views"),
            F.sum("top10_views").alias("top10_views"),
            F.sum("top100_views").alias("top100_views"),
        )
        .withColumn("top10_share", F.col("top10_views") / F.col("total_views"))
        .withColumn("top100_share", F.col("top100_views") / F.col("total_views"))
    )
    return topN_df


def language_engagement(df: DataFrame) -> DataFrame:
    """
    Compute daily engagement per language/project.

    Engagement here is measured as the total number of page views
    on a given day for a language/project.

    Args:
        df (DataFrame): Input dataframe with columns
            - file_date (string, format 'yyyyMMdd')
            - language (string)
            - database_name (string)
            - count_views (int)

    Returns:
        DataFrame: Aggregated dataframe with columns:
            - dt (date)
            - language
            - database_name
            - views_per_day (int)
    """
    lang_engagement_df = (
        df.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
        .groupBy("dt", "language", "database_name")
        .agg(F.sum("count_views").alias("views_per_day"))
    )
    return lang_engagement_df


# outliers part
# outlier

# -----------------------
# # 2. Aggregate to Daily per project + page
# # -----------------------
# daily_df = silver_df.groupBy("file_date", "database_name", "page_title").agg(
#     F.sum("count_views").alias("views_day")
# )


# # -----------------------
# # 4. Compute Median + MAD per (dt, project)
# # -----------------------

# # Median
# median_df = daily_df.groupBy("file_date", "database_name").agg(
#     F.expr("percentile_approx(views_day, 0.5)").alias("median_views")
# )

# # Join back
# with_median = daily_df.join(median_df, on=["file_date", "database_name"], how="left")

# # Compute absolute deviation from median
# with_dev = with_median.withColumn("abs_dev", F.abs(F.col("views_day") - F.col("median_views")))

# # Median absolute deviation
# mad_df = with_dev.groupBy("file_date", "database_name").agg(
#     F.expr("percentile_approx(abs_dev, 0.5)").alias("mad_views")
# )

# # Join back
# with_mad = with_dev.join(mad_df, on=["file_date", "database_name"], how="left")

# # -----------------------
# # 5. Outlier flag
# # -----------------------
# gold_df = with_mad.withColumn(
#     "is_outlier", (F.col("views_day") > F.col("median_views") + 10 * F.col("mad_views"))
# ).drop(
#     "abs_dev"
# )  # cleanup

# gold_df.show(100)
