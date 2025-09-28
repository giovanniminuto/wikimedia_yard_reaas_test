from pyspark.sql import functions as F


from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def dau(df: DataFrame) -> DataFrame:
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
        df.withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
        .groupBy("file_date", "language", "database_name")
        .agg(F.countDistinct("page_title").alias("DAU"))
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
    stickiness_df = dau_df.join(mau_df, on=["language", "database_name"], how="inner").withColumn(
        "stickiness", F.col("DAU") / F.col("MAU")
    )
    return stickiness_df
