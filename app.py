from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CSV to Parquet Converter") \
    .getOrCreate()

# Read the CSV file
input_csv_path = "s3a://landregistryproject/land_registry_data.csv"
df = spark.read.csv(input_csv_path, header=True, inferSchema=True)

# Transformations (if any)

# Write to Parquet
output_parquet_path = "s3a://landregistryproject/land_registry_data.parquet"
df.write.parquet(output_parquet_path)

# Stop the Spark Session
spark.stop()
