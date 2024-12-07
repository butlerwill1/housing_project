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

df.count()

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
        ('SW', 'SW1A', 'SW1A-1')
        
        >>> split_postcode("BT7 3AP")
        ('BT', 'BT7', 'BT7-3')
        
        >>> split_postcode(None)
        ('Unknown', 'Unknown', 'Unknown')
    """
    if postcode is None:
        return 'Unknown', 'Unknown', 'Unknown'
    
    parts = postcode.split()

    if postcode[:2] == 'W1': # The exception to the rule, a central London Postcode that's area code is one letter and one number 
        area = 'W1'
    else:
        area = re.sub(r'[^A-Za-z]', '', parts[0])  # Extracts letters before the space
    
    district = re.sub(r'[^A-Za-z0-9]', '', parts[0])  # Extracts letters and digits before the space
    
    # Check if there's a second part for the postcode
    if len(parts) > 1 and parts[1]:
        sector = district + "-" + parts[1][0]  # Adds the first digit of the second part of the postcode
    else:
        sector = 'Unknown'
    
    return area, district, sector

# COMMAND ----------

postcode = 'W1D 1BB'

# COMMAND ----------

postcode[:2]

# COMMAND ----------

def classify_london_postcode(area_code, district_code):
    """ Classifies a London postcode into Central London, Greater London, or Outside London based on the area code and district code.

    Args:
        area_code (str): The area code of the postcode, which is the initial letter(s) before any digits (e.g., "EC").
        district_code (str): The district code of the postcode, which may include letters and digits (e.g., "EC1").

    Returns:
        str: A string indicating the classification of the postcode. It can be 'Central London', 'Greater London', or 'Outside London'. 
             If either `area_code` or `district_code` is None, it returns 'Unknown'.

    Central London is defined by specific 'EC' and 'WC' and 'W1' area codes,  
    Greater London encompasses a broader range of area codes, including those that overlap with Central London and several others 
    that cover the wider metropolitan area. If the area or district codes do not match any within these specified lists, the location 
    is considered to be Outside London.

    Examples:
        - classify_london_postcode('EC', 'EC1A') -> 'Central London'
        - classify_london_postcode('W', 'W1') -> 'Central London'
        - classify_london_postcode('N', 'N1') -> 'Central London'
        - classify_london_postcode('BR', 'BR1') -> 'Greater London'
        - classify_london_postcode('ME', 'ME1') -> 'Outside London'
        - classify_london_postcode(None, 'EC2A') -> 'Unknown'
    """
    # Central London postcode areas and specific districts
    central_london_areas = ['EC', 'WC', 'W1']
    central_london_districts = ['WC1', 'WC2', 'EC1', 'EC2', 'EC3', 'EC4',
                                'EC1A', 'EC1M', 'EC1N', 'EC1P', 'EC1R', # These are the exceptions, Extra districts
                                'EC1V', 'EC1Y', 'EC2A', 'EC2M', 'EC2N', # were made because of high population density
                                'EC2P', 'EC2R', 'EC2V', 'EC2Y', 'EC3A', # in central London
                                'EC3M', 'EC3N', 'EC3P', 'EC3R', 'EC3V',
                                'EC4A', 'EC4M', 'EC4N', 'EC4P', 'EC4R',
                                'EC4V', 'EC4Y', 'EC50']
    
    # Greater London postcode areas (including those overlapping with Central London)
    greater_london_areas = central_london_areas + ['E', 'N', 'NW', 'SE', 
                                'SW', 'W', 'BR', 'CR', 'DA', 'EN', 'HA', 
                                'IG', 'KT', 'RM', 'SM', 'TW', 'UB', 'WD']
    
    if any(var is None for var in [area_code, district_code]):
        return 'Unknown'
    
    # Check if the postcode is in Central London
    if area_code in central_london_areas or district_code in central_london_districts:
        return 'Central London'
    
    # Check if the postcode is in Greater London
    if area_code in greater_london_areas:
        return 'Greater London'
    
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
df = df.withColumn("is_london?", classify_london_postcode_udf(df['postcode_area'], df['postcode_district']))

# COMMAND ----------

df.groupBy("is_london?").count().show()

# COMMAND ----------

central_london_areas = ['EC', 'WC', 'W1']
central_london_districts = ['WC1', 'WC2', 'EC1', 'EC2', 'EC3', 'EC4',
                                'EC1A', 'EC1M', 'EC1N', 'EC1P', 'EC1R', # These are the exceptions, Extra districts
                                'EC1V', 'EC1Y', 'EC2A', 'EC2M', 'EC2N', # were made because of high population density
                                'EC2P', 'EC2R', 'EC2V', 'EC2Y', 'EC3A', # in central London they have an extra letter on the first part ofthe postcode
                                'EC3M', 'EC3N', 'EC3P', 'EC3R', 'EC3V',
                                'EC4A', 'EC4M', 'EC4N', 'EC4P', 'EC4R',
                                'EC4V', 'EC4Y', 'EC50']

# Greater London postcode areas (including those overlapping with Central London)
greater_london_areas = central_london_areas + ['E', 'N', 'NW', 'SE', 
                                'SW', 'W', 'BR', 'CR', 'DA', 'EN', 'HA', 
                                'IG', 'KT', 'RM', 'SM', 'TW', 'UB', 'WD']
    
df = df.withColumn("is_london?",
                   when((df['postcode_area'].isin(central_london_areas))| (df['postcode_district'].isin(central_london_districts)), "Central London") \
                   .when(df['postcode_area'].isin(greater_london_areas), "Greater London") \
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


