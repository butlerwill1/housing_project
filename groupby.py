#%%
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
#%%
spark = SparkSession.builder \
        .appName("GroupBy").getOrCreate()

#%%
parquet_folder_path = "s3a://landregistryproject/land_registry_data_processed.parquet"

# Usage in a DataFrame
df = spark.read.parquet(parquet_folder_path)

#%%

classify_postcode_london_udf = udf(func.classify_london_postcode, StringType())

df = df.withColumn("is_london?", classify_postcode_london_udf(df['postcode_area'], df['postcode_district']))

df = df.withColumn("date_transfer", col("date_transfer").cast("timestamp"))

df = df.withColumn("year", year(col("date_transfer")))


#%%
area_groupby_df = func.groupby_calc_price(df, group_cols=['postcode_area', 'is_london?', 'property_type' ,'year'])
print(f"Number of rows in area_groupby_df = {area_groupby_df.count()}")

district_groupby_df = func.groupby_calc_price(df, group_cols=['postcode_district', 'is_london?', 'property_type', 'year'])
print(f"Number of rows in district_groupby_df = {district_groupby_df.count()}")

sector_grouby_df = func.groupby_calc_price(df, group_cols=['postcode_sector', 'is_london?', 'property_type', 'year'])
print(f"Number of rows in sector_grouby_df = {sector_grouby_df.count()}")
#%%

# Usage of the function
area_pct_change = func.calculate_pct_change(area_groupby_df, "postcode_area")
district_pct_change = func.calculate_pct_change(district_groupby_df, "postcode_district")
sector_pct_change = func.calculate_pct_change(sector_grouby_df, "postcode_sector")

# Show results
area_pct_change.show()
district_pct_change.show()
sector_pct_change.show()

# %%

# area_pct_change.write.mode('overwrite').csv("s3a://landregistryproject/area_pct_change.csv")
# district_pct_change.write.mode('overwrite').csv("s3a://landregistryproject/district_pct_change.csv")
# sector_pct_change.write.mode('overwrite').csv("s3a://landregistryproject/sector_pct_change.csv")

area_pct_change.coalesce(1).write.format("csv").option("header", "true").save("s3a://landregistryproject/area_pct_change.csv")
district_pct_change.coalesce(1).write.format("csv").option("header", "true").save("s3a://landregistryproject/district_pct_change.csv")
sector_pct_change.coalesce(1).write.format("csv").option("header", "true").save("s3a://landregistryproject/sector_pct_change.csv")