from wikimedia_yard_reaas_test.cleaning_pipeline import (
    language_filter,
    compute_daily_pageviews,
    compute_daily_summary,
)
from wikimedia_yard_reaas_test.utils import create_spark, read_delta_table


# ----------------------
# example of silver step
# ----------------------

# Spark session
spark = create_spark()

# delta paths
silver_path = "data/silver/pageviews/2025-01"

gold_path_daily_page = "data/daily_page/pageviews/2025-01"
gold_path_daily_summary = "data/daily_summary/pageviews/2025-01"
gold_path_languages = "data/languages/pageviews/2025-01"


silver_df = read_delta_table(spark, silver_path)

silver_df.show(10)

# Page-level pre-aggregation
daily_page = compute_daily_pageviews(silver_df)

# Project-level daily summary
daily_summary = compute_daily_summary(daily_page)

# Optional: export English-only for dashboards
languages = ["English", "Italian", "French", "German", "Spanish"]
language_filter(silver_df, languages=languages, path=gold_path_languages)


# todo improve titles
