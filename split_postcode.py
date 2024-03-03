from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col, avg, count, expr, year, lag, when
from pyspark.sql.window import Window
import re
import pyspark_functions as func
import importlib
importlib.reload(func)

#%% ---------------------------------------------------------------------------------------------------
#                 This code is used to split the postcode into Areas, Districts Postcode
# -----------------------------------------------------------------------------------------------------

# The given split_postcode function works by dissecting a UK postcode into three components: the area, district, and sector. Here's how it operates:

# Area: Extracted by removing any digit or special character from the first part of the postcode 
#   (i.e., everything before the space). This part typically represents a larger geographic region or city.

# District: Obtained by removing any special character from the first part of the postcode, 
#   keeping both letters and digits. This part usually narrows down the location to a smaller area within 
#   the larger region denoted by the area.

# Sector: Constructed by combining the district with the first digit of the second part of the postcode 
#   (if available), separated by a hyphen. The sector further refines the location, pinpointing a specific area or neighborhood.

# Example 1: "SW1A 1AA"

    # Area: "SW" (South West London)
    # District: "SW1A"
    # Sector: "SW1A-1"

# Example 2: "B1 1TT"

    # Area: "B" (Birmingham)
    # District: "B1"
    # Sector: "B1-1"


spark = SparkSession.builder. \
        appName("Split Postcode"). \
        getOrCreate()

# Define the schema for the UDF return type
output_schema = StructType([
    StructField("postcode_area", StringType(), True),
    StructField("postcode_district", StringType(), True),
    StructField("postcode_sector", StringType(), True)
])

# Define AWS S3 Bucket paths
parquet_folder_path = "s3a://landregistryproject/land_registry_data.parquet"
parquet_output_path = "s3a://landregistryproject/land_registry_data_processed.parquet"

# Register the function as a UDF
split_postcode_udf = udf(func.split_postcode, output_schema)

# Usage in a DataFrame
df = spark.read.parquet(parquet_folder_path)
df = df.withColumn("postcode_parts", split_postcode_udf(df["postcode"]))
df = df.select("*", "postcode_parts.*")  # Flatten the struct into separate columns

# Write to Parquet
df.write.mode("overwrite").parquet(parquet_output_path)

df.show(n=5, truncate=False)

# Stop the Spark Session
spark.stop()
