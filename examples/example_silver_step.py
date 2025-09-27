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
from wikimedia_yard_reaas_test.maps import lang_map_expr

# -----------------------
# 1. Spark session
# -----------------------
spark = (
    SparkSession.builder.appName("Wikimedia Silver Processing")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# -----------------------
# 2. Read Bronze
# -----------------------
bronze_path = "data/bronze/pageviews/2025-01"
bronze_df = spark.read.parquet(bronze_path)

# -----------------------
# 3. Domain step
# -----------------------

# Split domain into parts
parts = split(col("domain_code"), "\\.")

special_domain_codes = [
    "commons",
    "meta",
    "incubator",
    "species",
    "strategy" "outreach",
    "usability",
    "quality",
]


silver_df_domain_step = (
    bronze_df
    # Language = first part
    .withColumn("lang_code", parts.getItem(0))
    # Project detection (commons is special)
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
        .when(col("lang_code") == "commons", "commonswiki")
        .when(col("lang_code") == "meta", "metawiki")
        .when(col("lang_code") == "incubator", "incubatorwiki")
        .when(col("lang_code") == "species", "specieswiki")
        .when(col("lang_code") == "strategy", "strategywiki")
        .when(col("lang_code") == "outreach", "outreachwiki")
        .when(col("lang_code") == "usability", "usabilitywiki")
        .when(col("lang_code") == "quality", "qualitywiki")
        .otherwise("unknown"),
    )
    # mobile detection
    .withColumn(
        "is_mobile",
        when(
            col("lang_code").isin(special_domain_codes), col("domain_code").endswith(".m.m")
        ).otherwise(  # special case
            array_contains(parts, "m")
        ),
    )
)


silver_df_domain_step = silver_df_domain_step.withColumn(
    "language", lang_map_expr[col("lang_code")]
)
silver_df_domain_step.show(200)

# -----------------------
# 4. Page_title Step
# -----------------------

# # -----------------------
# # 5. Join with translations lookup (optional)
# # -----------------------
# # Lookup must be built beforehand (via Wikipedia API batch job)
# # Columns: lang_code,page_title,page_title_en
# lookup_path = "data/lookups/wiki_translations.csv"

# try:
#     lookup_df = spark.read.option("header", True).csv(lookup_path)
#     silver_df = (
#         silver_df.alias("s")
#         .join(
#             lookup_df.alias("l"),
#             (col("s.lang_code") == col("l.lang_code")) &
#             (col("s.page_title") == col("l.page_title")),
#             "left"
#         )
#         .drop("l.lang_code", "l.page_title")
#         .withColumnRenamed("page_title_en", "page_title_english")
#     )
# except Exception as e:
#     print(f"⚠️ Warning: No translation lookup found at {lookup_path}. "
#           "Proceeding without English titles.")

# # -----------------------
# # 6. Write Silver
# # -----------------------
# silver_path = "data/silver/pageviews/2025-01"

# (silver_df.write
#     .format("parquet")
#     .mode("overwrite")
#     .partitionBy("file_date")
#     .save(silver_path))

# print(f"✅ Silver table written to {silver_path}")
