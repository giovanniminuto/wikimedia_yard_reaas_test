from wikimedia_yard_reaas_test.cleaning_pipeline import (
    get_raw_schema,
    bronze_read_and_modify_raw_files,
)

from wikimedia_yard_reaas_test.utils import create_spark, write_delta

# ----------------------
# example of bronze step
# ----------------------

# Spark session
spark = create_spark()
schema = get_raw_schema()

# data paths
input_dir = "data/raw/pageviews/2025-01/"  # read all files in this folder
bronze_path = "data/bronze/pageviews/2025-01"

# raw file trasformation
df = bronze_read_and_modify_raw_files(spark, input_dir, schema)

df.show(10, truncate=False)

# Write Bronze DF as Delta
write_delta(df, bronze_path)
