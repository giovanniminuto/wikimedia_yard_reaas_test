from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame


def bucket_pages_adaptive(
    df: DataFrame,
    pct: float = 0.05,  # rare = bottom 5% daily views
    approx_accuracy: int = 1000,  # percentile_approx accuracy (lower=faster)
    min_floor: int = 5,  # never set the cutoff below this
    max_cap: int = 50,  # never set the cutoff above this
    top_k: int = 500,  # always keep the daily Top-K pages
    whitelist_titles: list[str] | None = None,
    whitelist_regexes: list[str] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """
    Returns:
      bucketed_with_share: file_date, language, database_name, page_bucket, views_bucket, share
      thresholds: per-day rare threshold used (for auditing)
    """

    # 1) Daily per-page views
    daily = df.groupBy("file_date", "language", "database_name", "page_title", "is_mobile").agg(
        F.sum("count_views").alias("views_page")
    )

    # 2) Adaptive threshold per (file_date, language, database_name)
    thresholds = (
        daily.groupBy("file_date", "language", "database_name", "is_mobile")
        .agg(F.percentile_approx("views_page", F.lit(pct), approx_accuracy).alias("pct_thr"))
        .withColumn(
            "rare_thr", F.least(F.greatest(F.col("pct_thr"), F.lit(min_floor)), F.lit(max_cap))
        )
    )

    # 3) Daily Top-K pages (always keep)
    w = Window.partitionBy("file_date", "language", "database_name", "is_mobile").orderBy(
        F.desc("views_page")
    )
    ranked = daily.withColumn("rank_page", F.row_number().over(w)).join(
        thresholds, ["file_date", "language", "database_name", "is_mobile"], "left"
    )

    # 4) Whitelist flags (titles + regex)
    is_whitelist_title = F.lit(False)
    if whitelist_titles:
        is_whitelist_title = F.col("page_title").isin(list(set(whitelist_titles)))

    is_whitelist_regex = F.lit(False)
    if whitelist_regexes:
        combined = "|".join([f"(?:{p})" for p in whitelist_regexes])  # non-capturing groups
        is_whitelist_regex = F.col("page_title").rlike(combined)

    flagged = (
        ranked.withColumn("is_whitelist", is_whitelist_title | is_whitelist_regex).withColumn(
            "is_topk", F.col("rank_page") <= F.lit(top_k)
        )
        # rare if <= threshold AND not whitelisted AND not Top-K
        .withColumn(
            "is_rare",
            (F.col("views_page") <= F.col("rare_thr"))
            & (~F.col("is_whitelist"))
            & (~F.col("is_topk")),
        )
    )

    # 5) Create bucket
    # Keep page title if NOT rare (i.e., whitelisted, Top-K, or above threshold); else bucket to "Other"
    bucketed = flagged.withColumn(
        "page_bucket", F.when(F.col("is_rare"), F.lit("Other")).otherwise(F.col("page_title"))
    )

    # 6) Aggregate by bucket + compute daily share
    bucketed_agg = bucketed.groupBy(
        "file_date", "language", "database_name", "page_bucket", "is_mobile"
    ).agg(F.sum("views_page").alias("views_bucket"))
    totals = bucketed_agg.groupBy("file_date", "language", "database_name", "is_mobile").agg(
        F.sum("views_bucket").alias("total_views")
    )
    bucketed_with_share = bucketed_agg.join(
        totals, ["file_date", "language", "database_name", "is_mobile"], "left"
    ).withColumn("share", F.col("views_bucket") / F.col("total_views"))

    return bucketed_with_share, thresholds


def anti_seasonality(df: DataFrame) -> DataFrame:
    # Convert file_date to integer for rangeBetween
    df_days = df.withColumn("date_int", F.datediff(F.col("file_date"), F.lit("2025-01-01")))

    # Past 6 days + current
    w_past = Window.partitionBy("language", "database_name").orderBy("date_int").rangeBetween(-6, 0)

    # Current + next 6 days
    w_future = (
        Window.partitionBy("language", "database_name").orderBy("date_int").rangeBetween(0, 6)
    )

    df_norm_rolling = (
        df_days
        # Past stats
        .withColumn("rolling_mean_past", F.avg("views_bucket").over(w_past))
        # Future stats
        .withColumn("rolling_mean_future", F.avg("views_bucket").over(w_future))
        # Rule: if fewer than 7 past days, use future mean
        .withColumn(
            "rolling_mean",
            F.when(F.col("date_int") < 4, F.col("rolling_mean_future")).otherwise(
                F.col("rolling_mean_past")
            ),
        )
        .withColumn("views_norm", F.col("views_bucket") / F.col("rolling_mean"))
        .drop("date_int")
        .drop("total_views")
        .drop("views_bucket")
        .drop("rolling_mean_past")
        .drop("rolling_mean_future")
        .drop("rolling_mean")
        .drop("rolling_mean_past")
    )

    return df_norm_rolling
