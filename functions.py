from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, count, expr, year, lag, when, stddev
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.window import Window
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
                                'WC1', 'WC2', 'EC1', 'EC2', 'EC3', 'EC4']
    
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
            avg("price").alias("avg_price"),
            stddev("price").alias("stddev_price"),
            expr("percentile_approx(price, 0.25)").alias("25th_percentile_price"),
            expr("percentile_approx(price, 0.5)s").alias("50th_percentile_price"),
            expr("percentile_approx(price, 0.75)").alias("75th_percentile_price")
        ) \
        .withColumn("cv", (col("stddev_price") / col("avg_price")) * 100) \
        .withColumn("iqr", col("75th_percentile") - col("25th_percentile")) \
        .withColumn("median_mean_diff", col("median_price") - col("avg_price"))
    
    aggregated_df = aggregated_df.withColumn("median_mean_diff_pct",
                                             (abs(col("median_mean_diff")) / col("50th_percentile_price")) * 100) \
                                 .withColumn("iqt_pct",
                                             (col("iqr") / col("median_price")) * 100)
                                            
    
    return aggregated_df


# Function to calculate percentage changes for a given level (area, district, sector)
def calculate_pct_change(df, level, cols_to_order=None):

    if cols_to_order is None:
        cols_to_order = ["year"]
    else:
        cols_to_order = list(cols_to_order)
        if "year" not in cols_to_order:
            cols_to_order.append("year")

    # Define the Window specification
    windowSpec = Window.partitionBy(level).orderBy(*cols_to_order)
    
    # 2. Lag Features
    for i in [1, 2, 5]:  # Change intervals as needed
        df = df.withColumn(f"lag_{i}_year", lag("avg_price", i).over(windowSpec))

    # 3. Calculate Percentage Change
    for i in [1, 2, 5]:
        df = df.withColumn(f"pct_change_{i}_year", 
                    (col("avg_price") - col(f"lag_{i}_year")) * 100 / col(f"lag_{i}_year"))

    # 4. Handle Missing Values (Optional)
    # Fill or filter out missing values as needed
    df = df.fillna(0)  # This replaces nulls with 0, adjust as per your requirements

    return df