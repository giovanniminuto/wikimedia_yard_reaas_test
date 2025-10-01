from wikimedia_yard_reaas_test.cleaning_pipeline import (
    language_filter,
    build_ml_dataset,
)
from wikimedia_yard_reaas_test.utils import create_spark, read_delta_table, write_delta


# Spark session
spark = create_spark()

# delta paths
silver_path = "data/silver/pageviews/2025-01"

gold_path_test_set = "data/test_set/pageviews/2025-01"
gold_path_train_set = "data/train_set/pageviews/2025-01"
gold_path_languages = "data/languages/pageviews/2025-01"


silver_df = read_delta_table(spark, silver_path)


# generate single-language delta lake db
languages = ["English", "Italian", "French", "German", "Spanish"]
list_of_db = language_filter(silver_df, languages=languages, path=gold_path_languages)


train_set = build_ml_dataset(
    list_of_db[1],  # italian delta lake
    obs_start="2025-01-01",
    obs_end="2025-01-23",
    lbl_start="2025-01-24",
    lbl_end="2025-01-27",
)

test_set = build_ml_dataset(
    list_of_db[1],  # italian delta lake
    obs_start="2025-01-05",
    obs_end="2025-01-27",
    lbl_start="2025-01-28",
    lbl_end="2025-01-31",
)
train_set.show(10)
write_delta(
    df=train_set, delta_path=gold_path_train_set, mode_str="overwrite", partition="days_active"
)
write_delta(
    df=test_set, delta_path=gold_path_test_set, mode_str="overwrite", partition="days_active"
)


print("Train rows:", train_set.count())
print("Test rows:", test_set.count())
