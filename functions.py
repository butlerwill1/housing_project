from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, count, expr, year, lag, when, stddev, abs, round, skewness, kurtosis
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.window import Window
import pandas as pd
import re

def split_postcode(postcode):
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
    # Central London postcode areas and specific districts
    central_london_areas = ['EC', 'WC']
    central_london_districts = ['W1', 'SW1', 'NW1', 'SE1', 'E1', 'N1', 
                                'WC1', 'WC2', 'EC1', 'EC2', 'EC3', 'EC4',
                                'EC1A', 'EC1M', 'EC1N', 'EC1P', 'EC1R',
                                'EC1V', 'EC1Y', 'EC2A', 'EC2M', 'EC2N',
                                'EC2P', 'EC2R', 'EC2V', 'EC2Y', 'EC3A',
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
    # Calculate the standard deviation of skewness and kurtosis across years within each group
    # df = df.withColumn("stddev_skewness", stddev("skewness_price").over(windowSpecPartitionOnly))
    # df = df.withColumn("stddev_kurtosis", stddev("kurtosis_price").over(windowSpecPartitionOnly))
    # df = df.withColumn("avg_skewness", stddev("skewness_price").over(windowSpecPartitionOnly))
    # df = df.withColumn("avg_kurtosis", stddev("kurtosis_price").over(windowSpecPartitionOnly))

    # df = df.withColumn("perc_stddev_skewness", col("stddev_skewness") * 100 / col("avg_skewness"))
    # df = df.withColumn("perc_stddev_kurtosis", col("stddev_kurtosis") * 100 / col("avg_kurtosis"))

    # # Round the calculated standard deviations
    # df = df.withColumn("stddev_skewness", round(col("stddev_skewness"), 2))
    # df = df.withColumn("stddev_kurtosis", round(col("stddev_kurtosis"), 2))

    # Handle Missing Values (Optional)
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



# Function to calculate percentage changes for a given level (area, district, sector)
# def calculate_pct_change(df, partition_cols):

#     if "year" in partition_cols:
#         partition_cols.remove("year")

#     # Define the Window specification
#     windowSpec = Window.partitionBy(partition_cols).orderBy("year")
    
#     # 2. Lag Features
#     for i in [1, 2, 5]:  # Change intervals as needed
#         df = df.withColumn(f"lag_{i}_year", round(lag("avg_price", i).over(windowSpec),1))

#     # 3. Calculate Percentage Change
#     for i in [1, 2, 5]:
#         df = df.withColumn(f"pct_change_{i}_year", 
#                     (round(col("avg_price") - col(f"lag_{i}_year")) * 100 / col(f"lag_{i}_year"),1))

#     # 4. Handle Missing Values (Optional)
#     # Fill or filter out missing values as needed
#     df = df.fillna(0)  # This replaces nulls with 0, adjust as per your requirements

#     return df