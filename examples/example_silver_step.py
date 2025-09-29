from wikimedia_yard_reaas_test.cleaning_pipeline import (
    create_spark,
    read_delta_table,
    silver_apply_quality_checks,
    silver_transform_domain_step,
    silver_drop_total_response_size,
    silver_transform_page_title,
    write_delta,
)
from wikimedia_yard_reaas_test.maps import get_lang_map_expr

# ----------------------
# example of silver step
# ----------------------

# Spark session
spark = create_spark()

# delta paths
bronze_path = "data/bronze/pageviews/2025-01"
silver_path = "data/silver/pageviews/2025-01"

bronze_df = read_delta_table(spark, bronze_path)
bronze_df.show(10)

# df trasformation
df_checked = silver_apply_quality_checks(bronze_df)
df_domain = silver_transform_domain_step(df_checked, get_lang_map_expr)
df_no_size = silver_drop_total_response_size(df_domain)
df_page_title = silver_transform_page_title(df_no_size)

df_page_title.show(10)

# Write Silver DF as Delta
write_delta(df_page_title, silver_path, mode_str="overwrite")


# todo remove capitol letters, understand if we want to remove also _ and substitude them with spaces
