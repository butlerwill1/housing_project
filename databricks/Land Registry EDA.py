# Databricks notebook source
# MAGIC %pip install regex

# COMMAND ----------

import regex as re
from pyspark.sql.functions import udf, col, avg, count, expr, year, lag, when
from pyspark.sql.types import StringType, StructType, StructField


# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Parquet Files of Land Registry Dataset of all UK Property Transactions over the last 30 years

# COMMAND ----------

base_path = "s3a://landregistryproject/land_registry_data.parquet/"

df = spark.read.format("parquet").load(base_path)

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

def split_postcode(postcode):
    """Splits a given UK postcode into its area, district, and sector components.

    A UK postcode is structured into two main parts: the "outward" code and the "inward" code, 
    separated by a space.

    Outward Code: This part of the postcode identifies the postal district and post town for 
    the delivery office. It typically consists of one or two letters followed by one or two 
    digits, and sometimes an additional letter at the end. The initial letter(s) represent 
    the postcode area, which is usually based on a key town or city in the region (e.g., "L" 
    for Liverpool, "SW" for South West London). The digits (and sometimes an additional letter)
      that follow define the postcode district within that area.

    Inward Code: This part is used to sort mail at the local delivery office. It starts with 
    a single digit that represents the postcode sector within the district, followed by two 
    letters which further pinpoint the delivery point within the sector.

    Here's how the function breaks down a UK postcode:

    Area: Extracted from the initial letters of the outward code. It represents the broader 
    geographical segment of the UK defined by the postcode. For example, in "SW1A", "SW" is 
    the area indicating South West London.

    District: Comprises both the letters and digits of the outward code but without any 
    additional characters. It narrows down the location to a specific postal district within
      the area. For instance, "SW1A" as a whole is considered the district.

    Sector: Combines the outward code district with the first digit of the inward code, 
    separated by a hyphen. This further refines the location within the district. For example,
    in "SW1A 1", the sector is "SW1A-1", indicating a specific part of the South West London area.

    Args:
        postcode (str): A UK postcode in standard format (e.g., "SW1A 1AA").

    Returns:
        tuple: A tuple containing the area, district, and sector of the postcode.
             If the postcode is None, returns ('Unknown', 'Unknown', 'Unknown').

    Examples:
        >>> split_postcode("SW1A 1AA") 
        ('SW1', 'SW1A', 'SW1A-1')

        >>> split_postcode("WC1A 1AY") 
        ('WC1', 'WC1A', 'WC1A 1')
        
        >>> split_postcode("BT7 3AP")
        ('BT', 'BT7', 'BT7-3')

        >>> split_postcode("N6 5UQ")
        ('N6', 'BT7', 'BT7-3')
        
        >>> split_postcode(None)
        ('Unknown', 'Unknown', 'Unknown')
    """
    if postcode is None:
        return 'Unknown', 'Unknown', 'Unknown'
    
    parts = postcode.split()
   
    area = re.sub(r'[^A-Za-z]+', '', parts[0])  # Extracts letters before the space
    
    district = parts[0]  # Extracts letters and digits before the space
    
    # Check if there's a second part for the postcode
    if len(parts) > 1 and parts[1]:
        sector = district + "-" + parts[1][0]  # Adds the first digit of the second part of the postcode
    else:
        sector = 'Unknown'
    
    return area, district, sector

# COMMAND ----------

def classify_london_postcode(area_code):
    
    # Central London postcode areas and specific districts
    inner_london_areas = ['EC', 'WC', 'N', 'NW', 'E', 'SE', 'SW', 'W']

    outer_london_areas = ['HA', 'UB', 'EN', 'KT', 'TW', 'BR', 'RM']
    
    if any(var is None for var in [area_code]):
        return 'Unknown'
    
    # Check if the postcode is in Central London
    if area_code in inner_london_areas:
        return 'Inner London'
    
    # Check if the postcode is in Greater London
    if area_code in outer_london_areas:
        return 'Outer London'
    
    return 'Outside London'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create an output data type that will be returned by the User Defined Function

# COMMAND ----------

split_output_schema = StructType([
    StructField("postcode_area", StringType(), True),
    StructField("postcode_district", StringType(), True),
    StructField("postcode_sector", StringType(), True)
])

split_postcode_udf = udf(split_postcode, split_output_schema)

# COMMAND ----------

df = df.withColumn("postcode_parts", split_postcode_udf("postcode"))
df = df.select("*", "postcode_parts.*")

# COMMAND ----------

display(df)

# COMMAND ----------

classify_london_postcode_udf = udf(classify_london_postcode, StringType())
df = df.withColumn("is_london?", classify_london_postcode_udf(df['postcode_area']))

# COMMAND ----------

df.groupBy("is_london?").count().show()

# COMMAND ----------

# Central London postcode areas and specific districts
inner_london_areas = ['EC', 'WC', 'N', 'NW', 'E', 'SE', 'SW', 'W']

outer_london_areas = ['HA', 'UB', 'EN', 'KT', 'TW', 'BR', 'RM']

df = df.withColumn("is_london?", when(df["postcode_area"].isin(inner_london_area), "Inner London") \
                                .when(df["postcode_area"].isin(outer_london_area), "Outer London") \
                                .otherwise("Outside London"))

# COMMAND ----------

df.groupBy("is_london?").count().show()

# COMMAND ----------

df.groupBy("is_london?").count().show()

# COMMAND ----------

df.groupBy("is_london?").count().show()

# COMMAND ----------

greater_london_sample = df.filter(col('is_london?')=='Greater London').sample(withReplacement=False, fraction=0.01).limit(100)
central_london_sample = df.filter(col('is_london?')=='Central London').sample(withReplacement=False, fraction=0.01).limit(100)
outside_london_sample = df.filter(col('is_london?')=='Outside London').sample(withReplacement=False, fraction=0.01).limit(100)

# COMMAND ----------

greater_london_sample.select("postcode", "town/city", "district", "county").show(100, truncate=False)

# COMMAND ----------

central_london_sample.select("postcode", "town/city", "district", "county").show(100, truncate=False)

# COMMAND ----------

outside_london_sample.select("postcode", "town/city", "district", "county").show(100, truncate=False)

# COMMAND ----------


