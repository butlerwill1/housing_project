from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, count, expr, year, lag, when, stddev, abs, round, skewness, kurtosis
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.window import Window
import pandas as pd
import re

#%% ---------------------------------------------------------------------------------------------------
#                                   Functions for PySpark operations
# -----------------------------------------------------------------------------------------------------

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
    area = re.sub(r'[^A-Za-z]', '', parts[0])  # Extracts letters before the space
    district = re.sub(r'[^A-Za-z0-9]', '', parts[0])  # Extracts letters and digits before the space
    
    # Check if there's a second part for the postcode
    if len(parts) > 1 and parts[1]:
        sector = district + "-" + parts[1][0]  # Adds the first digit of the second part of the postcode
    else:
        sector = 'Unknown'
    
    return area, district, sector

def classify_london_postcode(area_code, district_code):
    """ Classifies a London postcode into Central London, Greater London, or Outside London based on the area code and district code.

    Args:
        area_code (str): The area code of the postcode, which is the initial letter(s) before any digits (e.g., "EC").
        district_code (str): The district code of the postcode, which may include letters and digits (e.g., "EC1").

    Returns:
        str: A string indicating the classification of the postcode. It can be 'Central London', 'Greater London', or 'Outside London'. 
             If either `area_code` or `district_code` is None, it returns 'Unknown'.

    Central London is defined by specific 'EC' and 'WC' area codes, as well as certain districts that have high population densities.
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
    central_london_areas = ['EC', 'WC']
    central_london_districts = ['W1', 'SW1', 'NW1', 'SE1', 'E1', 'N1', 
                                'WC1', 'WC2', 'EC1', 'EC2', 'EC3', 'EC4',
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

def groupby_calc_price(df, group_cols):
    # Ensure group_cols is a list, even if only one column is provided
    if not isinstance(group_cols, list):
        group_cols = [group_cols]
    
    # Perform the groupBy operation with the dynamic list of columns
    # Unpack the list of columns using *
    aggregated_df = df.groupBy(*group_cols) \
        .agg(
            count("*").alias("num_transactions"),   
            round(avg("price"),1).alias("avg_price"),
            round(stddev("price"),1).alias("stddev_price"),
            round(expr("percentile_approx(price, 0.25)"), 1).alias("25th_percentile_price"),
            round(expr("percentile_approx(price, 0.5)"), 1).alias("median_price"),
            round(expr("percentile_approx(price, 0.75)"), 1).alias("75th_percentile_price"),
            round(expr("percentile_approx(price, 0.90)"), 1).alias("90th_percentile_price"),
            round(expr("percentile_approx(price, 0.95)"), 1).alias("95th_percentile_price"),
            round(skewness("price"), 2).alias("skewness_price"),
            round(kurtosis("price"), 2).alias("kurtosis_price")
        ) \
        .withColumn("coef_var", round((col("stddev_price") / col("avg_price")) * 100, 1)) \
        .withColumn("iqr", round((col("75th_percentile_price") - col("25th_percentile_price")), 1)) \
        .withColumn("median_mean_diff", round((col("median_price") - col("avg_price")), 1))
    
    aggregated_df = aggregated_df.withColumn("median_mean_diff_pct",
                                             round(abs(col("median_mean_diff") / col("median_price")) * 100, 1)) \
                                 .withColumn("iqr_pct",
                                             round(col("iqr") / col("median_price") * 100, 1))
                                            
    
    return aggregated_df


def calculate_pct_change(df, partition_cols):
    # Ensure the partitioning columns do not include 'year'
    partition_cols = [col for col in partition_cols if col != 'year']

    # Define the Window specification for lag and rolling calculations
    windowSpec = Window.partitionBy(*partition_cols).orderBy("year")
    
    # Calculate the median price for the previous year
    df = df.withColumn("lag_median_price", lag("median_price", 1).over(windowSpec))
    
    # Calculate the year-over-year percentage change based on median price
    df = df.withColumn("median_pct_change_1_year",
                       (col("median_price") - col("lag_median_price")) * 100 / col("lag_median_price"))

    # Define the Window specification for rolling average calculations
    # For a 2-year rolling average
    windowSpec2Year = windowSpec.rowsBetween(-1, 0)  # 2-year rolling window
    # For a 5-year rolling average
    windowSpec5Year = windowSpec.rowsBetween(-4, 0)  # 5-year rolling window
    
    # Calculate the 2-year and 5-year rolling average percentage changes based on median price
    df = df.withColumn("roll_median_pct_2_year",
                       avg(col("median_pct_change_1_year")).over(windowSpec2Year))
    df = df.withColumn("roll_median_pct_5_year",
                       avg(col("median_pct_change_1_year")).over(windowSpec5Year))

    # Round the calculated percentage changes
    df = df.withColumn("median_pct_change_1_year", round(col("median_pct_change_1_year"), 1))   
    df = df.withColumn("roll_median_pct_2_year",
                       round(col("roll_median_pct_2_year"), 1))
    df = df.withColumn("roll_median_pct_5_year",
                       round(col("roll_median_pct_5_year"), 1))
    

    windowSpecPartitionOnly = Window.partitionBy(*partition_cols)

    # Handle Missing Values 
    df = df.fillna(0)  # This replaces nulls with 0, adjust as per your requirements

    return df

def evaluate_sample_quality(df, params):
    min_transactions = params.get("min_transactions", 30)
    max_coef_var = params.get("max_coef_var", 50)
    max_median_mean_diff_pct = params.get("max_median_mean_diff_pct", 30)
    max_iqr_pct = params.get("max_iqr_pct", 25)

    df = df.withColumn("is_good_sample",
                           (col("num_transactions") >= min_transactions) &
                           (col("coef_var") <= max_coef_var) &
                           (col("median_mean_diff_pct") <= max_median_mean_diff_pct) &
                           (col("iqr_pct") <= max_iqr_pct))
                            # (col("skewness_price") <= s) &
    
    return df
