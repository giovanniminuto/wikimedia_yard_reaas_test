from pyspark.sql import functions as F
from wikimedia_yard_reaas_test.cleaning_pipeline import read_delta_table, create_spark
import matplotlib.pyplot as plt
from pyspark.sql.window import Window


# todo use this for ass 2


# todo use this for ass 2


# todo use this for ass 2


# todo use this for ass 2


# here I show a basic hipotesis, this could staisfy basically the request
delta_italian_path = "data/languages/pageviews/2025-01/Italian"

spark = create_spark()
delta_italian = read_delta_table(spark, delta_italian_path)  # to change

df = delta_italian.sample(fraction=0.001)
# df = (
#     delta_italian
#     .withColumn("day_of_week", F.date_format("dt", "E"))     # Mon, Tue, ...
#     .withColumn("dow_index", (((F.dayofweek("dt") + 5) % 7) + 1))  # Mon=1 … Sun=7
# )

# Aggregate by day of week
# dow_df = (
#     df.groupBy("dow_index", "day_of_week")
#     .agg(F.sum("count_views").alias("total_views"))
#     .orderBy("dow_index")
# )

#  Normalise by Rolling 7-Day Window

w = (
    Window.partitionBy("language", "database_name")
    .orderBy("file_date")
    .rowsBetween(-6, 0)  # no leakage
)

df_norm_rolling = df.withColumn("rolling_mean", F.avg("count_views").over(w)).withColumn(
    "views_norm", F.col("count_views") / F.col("rolling_mean")
)

df.show(10)
df_norm_rolling.show(1000)


# # Aggregate by day of week
# dow_df_rolling = (
#     df_norm_rolling.groupBy("dow_index", "day_of_week")
#     .agg(F.sum("views_norm").alias("norm_views"))
#     .orderBy("dow_index")
# )

# dow_pd = dow_df.toPandas()
# dow_pd_rolling = dow_df_rolling.toPandas()


# plt.figure(figsize=(10,4))
# plt.plot(dow_pd["day_of_week"], dow_pd["total_views"], marker="o")
# plt.title("Weekly Seasonality in Wikipedia Pageviews (Jan 2025)")
# plt.ylabel("Total Views")
# plt.xlabel("Day of Week")


# plt.figure(figsize=(10,4))
# plt.plot(dow_pd_rolling["day_of_week"], dow_pd_rolling["norm_views"], marker="o", label = "Rolling")

# plt.title("Weekly Seasonality in Wikipedia Pageviews (Jan 2025)")
# plt.ylabel("Total Views")
# plt.xlabel("Day of Week")


# plt.show()

# # Option A: Normalise by Day-of-Week Average
# # Compute average per day-of-week
# dow_avg = (
#     df.groupBy("dow_index")
#     .agg(F.avg("count_views").alias("avg_dow_views"))
# )

# # Join back and compute normalised views
# df_norm_static = (
#     df.join(dow_avg, on="dow_index", how="left")
#     .withColumn("views_norm", F.col("count_views") / F.col("avg_dow_views"))
# )

# df.show(10)
# df_norm_static.show(10)

# # Aggregate by day of week
# dow_df_static = (
#     df_norm_static.groupBy("dow_index", "day_of_week")
#     .agg(F.sum("views_norm").alias("norm_views"))
#     .orderBy("dow_index")
# )

# dow_pd_static = dow_df_static.toPandas()

# plt.plot(dow_pd_static["day_of_week"], dow_pd_static["norm_views"], marker="o", label = "Static")

# todo be carfull at leakage problem


# Aggregate by hour of day
# hour_df = (
#     df.groupBy("hour")
#     .agg(F.sum("count_views").alias("total_views"))
#     .orderBy("hour")
# )

# hour_pd = hour_df.toPandas()


# plt.figure(figsize=(10,4))
# plt.plot(hour_pd["hour"], hour_pd["total_views"], marker="o")
# plt.title("Hourly Seasonality in Wikipedia Pageviews (Jan 2025)")
# plt.ylabel("Total Views")
# plt.xlabel("Hour of Day")

# # Aggregate by hour of day
# hour_df_static = (
#     df_norm_static.groupBy("hour")
#     .agg(F.sum("views_norm").alias("norm_views"))
#     .orderBy("hour")
# )

# # Aggregate by hour of day
# hour_df_rolling = (
#     df_norm_rolling.groupBy("hour")
#     .agg(F.sum("views_norm").alias("norm_views"))
#     .orderBy("hour")
# )

# hour_pd_rolling = hour_df_rolling.toPandas()
# hour_pd_static = hour_df_static.toPandas()


# plt.figure(figsize=(10,4))
# plt.plot(hour_pd_rolling["hour"], hour_pd_rolling["norm_views"], marker="o", label = "Rolling")
# plt.plot(hour_pd_static["hour"], hour_pd_static["norm_views"], marker="o", label = "Static")

# plt.title("Hourly Seasonality in Wikipedia Pageviews (Jan 2025)")
# plt.ylabel("Total Views")
# plt.xlabel("Hour of Day")
# plt.legend()
