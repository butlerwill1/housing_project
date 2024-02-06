from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col, avg, count, expr, year, lag, when
from pyspark.sql.window import Window
import re
import functions as func
import importlib
importlib.reload(func)

spark = SparkSession.builder. \
        appName("Split Postcode"). \
        getOrCreate()

# Define the schema for the UDF return type
schema = StructType([
    StructField("postcode_area", StringType(), True),
    StructField("postcode_district", StringType(), True),
    StructField("postcode_sector", StringType(), True)
])

# Register the function as a UDF
# split_postcode_udf = udf(func.split_postcode, schema)
parquet_folder_path = "s3a://landregistryproject/land_registry_data.parquet"

# Usage in a DataFrame
df = spark.read.parquet(parquet_folder_path)
# df = df.withColumn("postcode_parts", split_postcode_udf(df["postcode"]))
# df = df.select("*", "postcode_parts.*")  # Flatten the struct into separate columns

# # Write to Parquet
# parquet_output_path = "s3a://landregistryproject/land_registry_data_processed.parquet"
# df.write.parquet(parquet_output_path)

# df.show(n=5, truncate=False)
# # Stop the Spark Session
# spark.stop()
classify_postcode_london_udf = udf(func.classify_postcode_london, StringType())

df = df.withColumn("is_london?", classify_postcode_london_udf(df['postcode_area'], df['postcode_district']))

df = df.withColumn("date_transfer", col("date_transfer").cast("timestamp"))

df = df.withColumn("year", year(col("date_transfer")))


#%%
area_groupby_df = func.groupby_calc_price(df, group_cols=['area', 'year'])

district_groupby_df = func.groupby_calc_price(df, group_cols=['district', 'year'])

sector_grouby_df = func.groupby_calc_price(df, group_cols=['sector', 'year'])

#%%
# Function to calculate percentage changes for a given level (area, district, sector)


# Usage of the function
area_pct_change = func.calculate_pct_change(df, "area")
district_pct_change = func.calculate_pct_change(df, "district")
sector_pct_change = func.calculate_pct_change(df, "sector")

# Show results
area_pct_change.show()
district_pct_change.show()
sector_pct_change.show()