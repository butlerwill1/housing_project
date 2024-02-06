#%%
import pandas as pd 
from pyspark.sql import SparkSession
#%%
import os
print(os.getenv('SPARK_HOME'))
print(os.getenv('PYSPARK_PYTHON'))
print(os.getenv('JAVA_HOME'))
print(os.getenv('SPARK_LOCAL_IP'))
#%%
spark = SparkSession.builder \
        .master('local') \
        .appName("Land Registry") \
        .getOrCreate()
        # .config("spark.driver.memory", "2g") \
        # .config("spark.executor.memory", "2g") \
        
#%%
reg_data = pd.read_csv("pp-complete.csv")
# %%
reg_data.columns = ['transactions_id', 'price', 'date_transfer', 'postcode',
                    'property_type', 'old_new', 'duration', 'paon', 'saon',
                    'street', 'locality', 'town/city', 'district', 'county',
                    'ppd_category_type','record_status']
# %%
# there are 28 million records

# Property Type
# D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other
# reg_data.to_csv("pp-complete.csv")

# PPD Category Type
# A = Standard Price Paid entry, includes single residential property sold for value.
# B = Additional Price Paid entry including transfers under a power of sale/repossessions, buy-to-lets (where they can be identified by a Mortgage), transfers to non-private individuals and sales where the property type is classed as ‘Other’.

# Duration
# Relates to the tenure: F = Freehold, L= Leasehold

# %%
reg_data['postcode'].sample(50)
# %%
# Function to split postcode into different levels
def split_postcode(postcode):
    if pd.isna(postcode):
        return 'Unknown', 'Unknown', 'Unknown'
    
    parts = postcode.split()
    area = ''.join(filter(str.isalpha, postcode.split()[0]))  # Extracts letters before the space
    district = ''.join(filter(lambda x: x.isalpha() or x.isdigit(), postcode.split()[0]))  # Extracts letters and digits before the space
    
    # Check if there's a second part for the postcode
    if len(parts) > 1 and parts[1]:
        sector = district + parts[1][0]  # Adds the first digit of the second part of the postcode
    else:
        sector = 'Unknown'
    
    return area, district, sector
#%%

reg_data['postcode'].isna().sum()
# Apply the function to each postcode
reg_data[['area', 'district', 'sector']] = reg_data['postcode'].apply(lambda x: split_postcode(x)).apply(pd.Series)
# %%

reg_data['area'].unique()

# %%
