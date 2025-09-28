from typing import Union, Optional
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def language_filter(
    df: DataFrame, language: str = "English", path: Optional[Union[Path, str]] = None
) -> DataFrame:
    """
    Filter the input DataFrame for rows matching a specific language

    Args:
        df (DataFrame): Input Spark DataFrame with at least a 'language' and 'file_date' column.
        language (str, optional): Language filter (e.g., "English" or "en"). Defaults to "English".
        path (Union[Path, str], optional): If provided, the filtered DataFrame will be written
                                           in Delta format partitioned by 'file_date'.

    Returns:
        DataFrame: Filtered DataFrame containing only rows of the given language.
    """

    single_language_filter = df.filter(col("language") == language)

    if path:
        save_path = str(path) if isinstance(path, Path) else path

        dates = [
            row.file_date for row in single_language_filter.select("file_date").distinct().collect()
        ]

        for d in dates:
            (
                single_language_filter.filter(col("file_date") == d)
                .write.format("delta")
                .option("compression", "snappy")
                .mode("append")
                .partitionBy("file_date")
                .save(save_path)
            )
        print(f"Filtered data written to {save_path}")

    return single_language_filter


def filtered_only_language_defined(
    df: DataFrame, language: str = "English", path: Optional[Union[Path, str]] = None
) -> DataFrame:
    filtered_only_language = df.filter(col("language").isNotNull())

    if path:
        save_path = str(path) if isinstance(path, Path) else path

        dates = [
            row.file_date for row in filtered_only_language.select("file_date").distinct().collect()
        ]

        for d in dates:
            (
                filtered_only_language.filter(col("file_date") == d)
                .write.format("delta")
                .option("compression", "snappy")
                .mode("append")
                .partitionBy("file_date")
                .save(save_path)
            )
        print(f"Filtered data written to {save_path}")

    return filtered_only_language
