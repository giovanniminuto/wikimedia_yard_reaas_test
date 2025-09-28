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

# -----------------------
# 1. Spark session
# -----------------------
builder = (
    SparkSession.builder.appName("Wikimedia Silver Processing")
    .config("spark.sql.files.maxPartitionBytes", "256MB")
    .config("spark.driver.memory", "10g")
    .config("spark.executor.memory", "10g")
    .config("spark.executor.cores", "7")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "4g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
spark.conf.set("spark.sql.files.maxRecordsPerFile", 2_000_000)
# -----------------------
# 2. Read Bronze
# -----------------------
bronze_path = "data/bronze/pageviews/2025-01"
bronze_df = spark.read.format("delta").load(bronze_path)


bronze_df.show(100)

bronze_df_checked = (
    bronze_df.filter((F.col("count_views") >= 0) & (F.col("count_views") <= 1_000_000_000))
    .filter(col("domain_code").isNotNull())
    .filter(col("page_title") != "-")
)

# -----------------------
# 3. Domain_code step
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


lang_map_expr = get_lang_map_expr()

silver_df_domain_step = (
    bronze_df_checked
    # Map language names directly from parts[0]
    .withColumn("language", lang_map_expr[parts.getItem(0)])
    # Database mapping
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
        .when(parts.getItem(0) == "commons", "commonswiki")
        .when(parts.getItem(0) == "meta", "metawiki")
        .when(parts.getItem(0) == "incubator", "incubatorwiki")
        .when(parts.getItem(0) == "species", "specieswiki")
        .when(parts.getItem(0) == "strategy", "strategywiki")
        .when(parts.getItem(0) == "outreach", "outreachwiki")
        .when(parts.getItem(0) == "usability", "usabilitywiki")
        .when(parts.getItem(0) == "quality", "qualitywiki")
        .when(col("language").isNotNull(), "wikipedia.org"),  # fallback if language recognized
    )
    # Mobile detection
    .withColumn(
        "is_mobile",
        when(
            parts.getItem(0).isin(special_domain_codes), col("domain_code").endswith(".m.m")
        ).otherwise(array_contains(parts, "m")),
    )
)


# -----------------------
# 4. total_response_size step
# -----------------------

silver_df_total_response_size_step = silver_df_domain_step.drop("total_response_size")


# -----------------------
# 4. page title step
# -----------------------

silver_df_page_title = (
    silver_df_total_response_size_step
    # Split on the first ":"
    .withColumn(
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
)
silver_df_page_title.show(100)

# -----------------------
# 5. Write Silver
# -----------------------
silver_path = "data/silver/pageviews/2025-01"


dates = [row.file_date for row in silver_df_page_title.select("file_date").distinct().collect()]
for d in dates:
    (
        silver_df_page_title.filter(col("file_date") == d)
        .write.format("delta")
        .option("compression", "snappy")
        .mode("append")
        .partitionBy("file_date")
        .save(silver_path)
    )

print(f"✅ Silver table written to {silver_path}")
