import os
import requests
from bs4 import BeautifulSoup
from delta import configure_spark_with_delta_pip

from typing import Optional, Union, Callable
from pathlib import Path
from pyspark.sql.window import Window

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def download_pageviews_files(base_url: str, output_dir: str) -> None:
    """
    Download Wikipedia pageviews files from a given base URL.

    This function scrapes the index page at `base_url` to find all
    files matching the pattern "pageviews-2025*.gz". It downloads
    each file into the specified `output_dir` if not already present,
    ensuring that duplicate downloads are avoided. At the end, it
    prints the total number of files found and the cumulative size
    of the downloaded files.

    Args:
        base_url (str): The URL of the index page containing links to
            pageview files.
        output_dir (str): The local directory where downloaded files
            should be stored. Created if it does not exist.
    """
    # Scrape the index page to get all file names
    resp = requests.get(base_url)
    soup = BeautifulSoup(resp.text, "html.parser")

    files = [
        a["href"]
        for a in soup.find_all("a")
        if a["href"].startswith("pageviews-2025") and a["href"].endswith(".gz")
    ]

    print(f"Found {len(files)} files to download")

    # Download files if not already present
    os.makedirs(output_dir, exist_ok=True)

    for f in files:
        url = base_url + f
        out_path = os.path.join(output_dir, f)
        if not os.path.exists(out_path):
            print(f"Downloading {f}...")
            r = requests.get(url, stream=True)
            with open(out_path, "wb") as f_out:
                for chunk in r.iter_content(chunk_size=8192):
                    f_out.write(chunk)
        else:
            print(f"Already downloaded {f}")

    # Compute total size of all downloaded files
    sizes = []
    names = []

    for f in files:
        out_path = os.path.join(output_dir, f)
        size = os.path.getsize(out_path)  # bytes
        sizes.append(size)
        names.append(f)

    total_bytes = sum(sizes)
    print(f"\nTotal bytes downloaded: {total_bytes:,}")


def create_spark(app_name: str = "Wikimedia Processing") -> SparkSession:
    """
    Create and configure a Spark session with Delta Lake support.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.files.maxPartitionBytes", "256MB")
        .config("spark.driver.memory", "12g")
        .config("spark.executor.memory", "12g")
        .config("spark.executor.cores", "8")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "3g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Tune Spark shuffle and file writing
    spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
    spark.conf.set("spark.sql.files.maxRecordsPerFile", 2_000_000)

    return spark


def write_delta(
    df: DataFrame,
    delta_path: Union[Path, str],
    mode_str: str = "append",
    partition: str = "file_date",
) -> None:
    """
    Write DataFrame to Delta Table partitioned by file_date.

    Args:
        df (DataFrame): Input dataframe
        delta_path (str): Destination folder for Delta table
    """
    (
        df.write.format("delta")
        .option("compression", "snappy")
        .mode(mode_str)
        .partitionBy(partition)
        .save(delta_path)
    )
    print(f"✅ Delta Table written to {delta_path}")


def read_delta_table(spark: SparkSession, delta_table_path: Union[Path, str]) -> DataFrame:
    """
    Read Delta table.

    Args:
        spark (SparkSession): active Spark session
        delta_table_path (str): path to Delta table

    Returns:
        DataFrame: delta table dataframe
    """
    return spark.read.format("delta").load(delta_table_path)


def upsert_partition(
    spark: SparkSession,
    delta_path: Union[Path, str],
    late_df: DataFrame,
    partition_date: str,
    dedup_cols: list,
) -> None:
    """
    Safely handle late data for a partitioned Delta table.

    Args:
        delta_path (str): Path to Delta table (e.g., "data/silver/pageviews")
        late_df (DataFrame): DataFrame with new/late records
        partition_date (str): The date of the partition to fix (format "YYYY-MM-DD")
        dedup_cols (list): Columns to use for deduplication (e.g. ["page_title","language", "domain_code", "namespace"])
    """

    # 1. Load the existing partition
    existing_df = (
        spark.read.format("delta").load(delta_path).filter(F.col("file_date") == partition_date)
    )

    # 2. Union old + new
    combined_df = existing_df.unionByName(late_df)

    # 3. Deduplicate (keep latest row if duplicates exist)
    window = Window.partitionBy(*dedup_cols).orderBy(
        F.col("ingestion_time").desc()
    )  # if you track ingestion timestamp
    deduped_df = (
        combined_df.withColumn("rn", F.row_number().over(window)).filter("rn = 1").drop("rn")
    )

    # 4. Overwrite just that partition
    deduped_df.write.format("delta").mode("overwrite").option(
        "replaceWhere", f"file_date = '{partition_date}'"
    ).save(delta_path)

    print(f"✅ Partition {partition_date} updated with late data.")
