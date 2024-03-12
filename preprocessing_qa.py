#%%
import pandas as pd
import regex as re
import matplotlib.pyplot as plt
import functions as func
import importlib
importlib.reload(func)
import geopandas as gpd
from shapely.validation import explain_validity

#%%----------------------------------------------------------------------------------------------------
#                     QA the Land Registry Dataset although too big to properly work on in pandas
#------------------------------------------------------------------------------------------------------
land_registry_data = pd.read_csv("land_registry_data.csv")

land_registry_data['postcode'].isna().sum()
#%%
land_registry_data[land_registry_data['postcode'].isna()].sample(20)
# Can't really do much about missing postcodes but they are a small fraction of the total dataset 46000
#%%
land_registry_data_notna = land_registry_data[land_registry_data['postcode'].notna()]
#%% Check for postcodes that don't have a space in them, ie. they are just contiguous words
# and therefore not real postcodes. There appears to be only one, "UNKNOWN"
land_registry_data_notna[~land_registry_data_notna['postcode'].str.contains(" ")]

#%% The Price data seems to be fine, no null values and is an integer column so no strings
land_registry_data[land_registry_data['price'].isna()]

#%% Check the data column, the date ranges are sensible and there are no NaT values when converting to datetimes
land_registry_data['date_transfer'].min()
land_registry_data['date_transfer'].max()

land_registry_data['date_transfer'] = pd.to_datetime(land_registry_data['date_transfer'], errors='coerce')

land_registry_data['date_transfer'].isna().sum()
#%%----------------------------------------------------------------------------------------------------
#                     QA the Transaction Dataset returned from the Pyspark Processes
#------------------------------------------------------------------------------------------------------
district_groupby = pd.read_csv("District_Transaction_Groupby.csv")

# Checking the postcode districts have actual numbers and that the numbers start from
# 0 or 1 and increase by 1 each time, e.g. BR1, BR2, BR3
# Some postcode districts don't follow this rule but most should and this print out helps
# make the checking easier

unique_districts_transactions = district_groupby['postcode_district'].unique().tolist()

func.check_districts(unique_districts_transactions)

#%%----------------------------------------------------------------------------------------------------
#                                   QA the Polygon Datasets
#------------------------------------------------------------------------------------------------------

postcode_dist_poly = gpd.read_file("GB_Postcodes/PostalDistrict.shp")
socio_economic = gpd.read_file("English_IMD_2019/IMD_2019.shp")
#%%
unique_poly_districts = postcode_dist_poly['PostDist'].unique().tolist()
# %% You can compare the problems with the poly districts and the problems with the groupby transactions districts
func.check_districts(unique_poly_districts)
#
# %% Compare the set of Postdistricts in the transaction groupby as are in the postcode polygons
# Unfortunately there are quite a few districts that are in one dataset but not the other

[district for district in unique_districts_transactions if district not in unique_poly_districts]

# %%
[district for district in unique_poly_districts if district not in unique_districts_transactions]
# %% Check for invalid geometries and use shapely explanations

invalid_postcode_geometries = postcode_dist_poly[~postcode_dist_poly.is_valid]
invalid_socio_geometries = socio_economic[~socio_economic.is_valid]

for index, row in invalid_socio_geometries.iterrows():
    explanation = explain_validity(row.geometry)
    print(f"Row {index}: {explanation}")

# All of the invalid geometries are self intersections
# %% Fix these slight intersections by using the buffer(0) method
postcode_dist_poly['geometry'] = postcode_dist_poly.apply(lambda x: x.geometry.buffer(0) if not x.geometry.is_valid else x.geometry, axis=1)

socio_economic['geometry'] = socio_economic.apply(lambda x: x.geometry.buffer(0) if not x.geometry.is_valid else x.geometry, axis=1)
# %%
postcode_dist_poly.to_file("GB_Postcodes/PostalDistrict.shp")
socio_economic.to_file("English_IMD_2019/IMD_2019.shp")
# %%
