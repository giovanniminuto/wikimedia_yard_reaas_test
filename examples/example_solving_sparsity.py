# Log-transform the page-level counts
# log_scaled = silver_df_page_title.withColumn("log_views", F.log1p("count_views"))

from pyspark.sql import functions as F
from wikimedia_yard_reaas_test.cleaning_pipeline import read_delta_table, create_spark
import matplotlib.pyplot as plt

# here I show a basic hipotesis, this could staisfy basically the request
delta_italian_path = "data/languages/pageviews/2025-01/Italian"

spark = create_spark()
delta_italian = read_delta_table(spark, delta_italian_path)  # to change
# df = (
#     delta_italian
#     .withColumn("file_date", F.to_date("file_date", "yyyy-MM-dd"))
#     .withColumn("hour", F.substring("file_time", 1, 2).cast("int"))
#     .withColumn("day_of_week", F.date_format("file_date", "E"))     # Mon, Tue, ...
#     .withColumn("dow_index", (((F.dayofweek("file_date") + 5) % 7) + 1))  # Mon=1 … Sun=7
# )


df_sample = delta_italian.sample(fraction=0.00001)  # 0.1% of raw data
df_sample.show(10)

# # Define threshold for rare pages (e.g. < 5 daily views)
# daily_page = (
#     df_sample
#     .withColumn("dt", F.to_date("file_date", "yyyyMMdd"))
#     .groupBy("dt","language","database_name","page_title")
#     .agg(F.sum("count_views").alias("views_page"))
# )

# # Bucket rare pages into "Other"
# bucketed = daily_page.withColumn(
#     "page_bucket",
#     F.when(F.col("views_page") < 2, F.lit("Other")).otherwise(F.col("page_title"))
# )


# bucketed_agg = (
#     bucketed.groupBy("dt","language","database_name","page_bucket")
#     .agg(F.sum("views_page").alias("views_bucket"))
# )

# bucketed_agg.show(1000)
