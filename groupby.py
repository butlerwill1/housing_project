#
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col, avg, count, expr, year, lag, when
from pyspark.sql.window import Window
import re
import functions as func
import importlib
importlib.reload(func)
import os

# Parameters to assess the quality of a sample of a group of property transactions created when performing a groupby
sample_quality_params = {
    'min_transactions': 30, # Considered a minimum sample size in Central Limit Theorem
    'max_coef_var': 50, # Maximum allowed Coefficient of variance 
    'max_median_mean_diff_pct': 10, # Maximum allowed percentage difference between the median and mean, to see whether a few large property prices skew the mean
    'max_iqr_pct': 25 # Maximum allowed percentage of the Interquartile range divided by the mediam, another measure of skewnwss
}
#
spark = SparkSession.builder \
        .appName("GroupBy").getOrCreate()

#
parquet_folder_path = "s3a://landregistryproject/land_registry_data_processed.parquet"

# Usage in a DataFrame
df = spark.read.parquet(parquet_folder_path)

#
classify_postcode_london_udf = udf(func.classify_london_postcode, StringType())

df = df.withColumn("is_london?", classify_postcode_london_udf(df['postcode_area'], df['postcode_district']))

df = df.withColumn("date_transfer", col("date_transfer").cast("timestamp"))

df = df.withColumn("year", year(col("date_transfer")))


# ----------------------------------------------------------------------------------------
# Perform groupby operations on the postcode sections and find average and median prices
# ----------------------------------------------------------------------------------------
            
groupby_cols = ['is_london?', 'property_type' ,'year']

area_groupby_df = func.groupby_calc_price(df, ['postcode_area'] + groupby_cols)
print(f"Number of rows in area_groupby_df = {area_groupby_df.count()}")

district_groupby_df = func.groupby_calc_price(df, ['postcode_district'] + groupby_cols)
print(f"Number of rows in district_groupby_df = {district_groupby_df.count()}")

sector_grouby_df = func.groupby_calc_price(df, ['postcode_sector'] + groupby_cols)
print(f"Number of rows in sector_grouby_df = {sector_grouby_df.count()}")
# ----------------------------------------------------------------------------------------
#  Now calculate the percentage differences between the average prices
# ----------------------------------------------------------------------------------------

# Usage of the function
area_pct_change = func.calculate_pct_change(area_groupby_df, ['postcode_area'] + groupby_cols)
district_pct_change = func.calculate_pct_change(district_groupby_df, ['postcode_district'] + groupby_cols)
sector_pct_change = func.calculate_pct_change(sector_grouby_df, ['postcode_sector'] + groupby_cols)

# Show results
area_pct_change.show()
district_pct_change.show()
sector_pct_change.show()

# %%
area_pct_change = func.evaluate_sample_quality(area_groupby_df, sample_quality_params)
district_pct_change = func.evaluate_sample_quality(district_pct_change, sample_quality_params)
sector_pct_change = func.evaluate_sample_quality(sector_pct_change, sample_quality_params)
#%%
# area_pct_change.write.mode('overwrite').csv("s3a://landregistryproject/area_pct_change.csv")
# district_pct_change.write.mode('overwrite').csv("s3a://landregistryproject/district_pct_change.csv")
# sector_pct_change.write.mode('overwrite').csv("s3a://landregistryproject/sector_pct_change.csv")

area_pct_change.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("s3a://landregistryproject/area_pct_change.csv")
district_pct_change.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("s3a://landregistryproject/district_pct_change.csv")
sector_pct_change.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("s3a://landregistryproject/sector_pct_change.csv")