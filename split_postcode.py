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
df = df.withColumn("postcode_parts", func.split_postcode_udf(df["postcode"]))
df = df.select("*", "postcode_parts.*")  # Flatten the struct into separate columns

# Write to Parquet
parquet_output_path = "s3a://landregistryproject/land_registry_data_processed.parquet"
df.write.parquet(parquet_output_path)

df.show(n=5, truncate=False)
# Stop the Spark Session
spark.stop()
