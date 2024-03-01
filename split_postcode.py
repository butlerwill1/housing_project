from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col, avg, count, expr, year, lag, when
from pyspark.sql.window import Window
import re
import pyspark_functions as func
import importlib
importlib.reload(func)

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
