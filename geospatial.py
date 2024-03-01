#%%
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import functions as func
#%% ---------------------------------------------------------------------------------------------------
#                Merge together Postcode and Socio Economic Indicator data on their polygons
# -----------------------------------------------------------------------------------------------------
#%%
postcode_dist_poly = gpd.read_file("GB_Postcodes/PostalDistrict.shp")

postcode_dist_poly['AreaKm2'] = postcode_dist_poly.geometry.area / 1000000
#%%
print(postcode_dist_poly.crs)
# Convert into a geograhic CRS 
postcode_dist_poly = postcode_dist_poly.to_crs("EPSG:4326")
# Rename the geometry to another column so that you can merge it back later after the aggregation
postcode_dist_poly['postcode_dist_geometry'] = postcode_dist_poly.geometry
# %%
# os_code_point = gpd.read_file("vmdvec_gb.gpkg")
# %%
socio_economic = gpd.read_file("English_IMD_2019/IMD_2019.shp")
print(socio_economic.crs)

renaming_dict = {col: func.clean_socio_columns(col) for col in socio_economic.columns}
renaming_dict['lsoa11nmw'] = 'AreaName'
# Convert into a geograhic CRS 
socio_economic = socio_economic.rename(columns=renaming_dict)

socio_economic = socio_economic.to_crs("EPSG:4326")
# %% Do a spatial join where the smaller socio economic Lower-Layer Super Output Areas (LSOAs) are within the
# postcode district polygons. The op = 'within' argument means the socio_economic polygon must be completely inside 
# a postcode distribution polygon
postcode_socio_economic = gpd.sjoin(socio_economic, postcode_dist_poly, how='inner', op='within')
#%% Now do a spatial join where we keep the geometry of the low level smaller areas of the socio economic data
socio_economic_postcode = gpd.sjoin(socio_economic, postcode_dist_poly, how='left', op='within')

socio_economic_postcode.rename(columns={'lsoa11nmw':'AreaName'})
socio_economic_postcode.drop(columns=['postcode_dist_geometry']).to_file("socio_economic_postcode.gpkg", layer='socio', driver="GPKG")
#%% In this dataset, an area with a rank of 1 is the most deprived, and it will have the highest score 


aggregated_data = postcode_socio_economic.groupby("PostDist").apply(func.postcode_socio_grouby_agg).reset_index()

#%% Pick up the dataset from the Groupby Aggregations on the land registry dataset
# This dataset contains no geometry data, only the string code Postcode district
district_groupby = pd.read_csv("District_Ordered_Average%.csv")

#%%
district_groupby_socio_economic = pd.merge(district_groupby, aggregated_data, 
                                           how='left', left_on='postcode_district',
                                           right_on='PostDist')

# %% After the aggregation, ensure the dataframe is a geodataframe

district_groupby_socio_economic_gdf = gpd.GeoDataFrame(district_groupby_socio_economic, geometry='geometry')
# %%
# district_groupby_socio_economic.to_excel("district_groupby_socio_economic.xlsx")

# district_groupby_socio_economic = district_groupby_socio_economic[district_groupby_socio_economic['PostDist']=='E3']
district_groupby_socio_economic_gdf = district_groupby_socio_economic_gdf[ \
            (district_groupby_socio_economic_gdf['PostDist'].notna())&
            (district_groupby_socio_economic_gdf['year']==2023)]

district_groupby_socio_economic_gdf.to_file("district_groupby_socio_economic.gpkg", layer='socio', driver="GPKG")


# %%-----------------------------------------------------------------------------------------------
#                                                Graphs
# -------------------------------------------------------------------------------------------------
test = district_groupby_socio_economic[(district_groupby_socio_economic['is_london?']!='Outside London')&(district_groupby_socio_economic['property_type']=='F')]
plt.title("Population Density Vs Number of transactions")
plt.xlabel("Population Density")
plt.ylabel("Number of Transactions")

plt.scatter(test['PopulationDensity'],test['num_transactions'])
# %%
test = district_groupby_socio_economic[(district_groupby_socio_economic['is_london?']!='Outside London')&(district_groupby_socio_economic['property_type']=='F')]
plt.title("CountLowLevelAreas Vs AreaKm2")
plt.xlabel("CountLowLevelAreas")
plt.ylabel("AreaKm2")

plt.scatter(test['CountLowLevelAreas'],test['AreaKm2'])
# %%
