#%%
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

#%%
postcode_dist_poly = gpd.read_file("GB_Postcodes/PostalDistrict.shp")

postcode_dist_poly['AreaKm2'] = postcode_dist_poly.geometry.area / 1000000
#%%
print(postcode_dist_poly.crs)
# Convert into a UK specific CRS
postcode_dist_poly = postcode_dist_poly.to_crs("EPSG:4326")
# Rename the geometry to another column so that you can merge it back later after the aggregation
postcode_dist_poly['postcode_dist_geometry'] = postcode_dist_poly.geometry
# %%
# os_code_point = gpd.read_file("vmdvec_gb.gpkg")
# %%
socio_economic = gpd.read_file("English_IMD_2019/IMD_2019.shp")
print(socio_economic.crs)
# Convert into a UK specific CRS
socio_economic = socio_economic.to_crs("EPSG:4326")
# %%
socio_economic_postcode = gpd.sjoin(socio_economic, postcode_dist_poly, how='inner', op='within')

# In this dataset, an area with a rank of 1 is the most deprived, and it will have the highest score 
#%%
def postcode_socio_grouby_agg(x):

    d = {}
    d['geometry'] = x['postcode_dist_geometry'].iloc[0]
    d['AreaName'] = x['LADnm'].iloc[0]
    d['Locale'] = x['Locale'].iloc[0]
    d['CountLowLevelAreas'] = x.shape[0]
    d['AreaKm2'] = x['AreaKm2'].iloc[0]
    d['Population'] = x['TotPop'].sum()
    d['Population_16-59'] = x['Pop16_59'].sum()
    d['Population_60+'] = x['Pop60+'].sum()
    d['PopulationDensity'] = round(d['Population'] / d['AreaKm2'], 2)

    d['IMDScore_Avg'] = round(x['IMDScore'].mean(), 2)
    d['IMDRank0_Avg'] = round(x['IMDRank0'].mean(), 2)
    d['IMDScore_Median'] = round(x['IMDScore'].median(), 2)
    d['IMDScore_Max'] = x['IMDScore'].max()
    d['IMDScore_Min'] = x['IMDScore'].min()

    # Income Score
    d['IncScore_Avg'] = round(x['IncScore'].mean(), 2)
    d['IncRank_Avg'] = round(x['IncRank'].mean(), 2)
    d['IncScore_Median'] = round(x['IncScore'].median(), 2)
    d['IncScore_Max'] = x['IncScore'].max()
    d['IncScore_Min'] = x['IncScore'].min()

    # Education, skills and training
    d['EduScore_Avg'] = round(x['EduScore'].mean(), 2)
    d['EduRank_Avg'] = round(x['EduRank'].mean(), 2)
    d['EduScore_Median'] = round(x['EduScore'].median(), 2)
    d['EduScore_Max'] = x['EduScore'].max()
    d['EduScore_Min'] = x['EduScore'].min()

    # Crime Score
    d['CriScore_Avg'] = round(x['CriScore'].mean(), 2)
    d['CriRank_Avg'] = round(x['CriRank'].mean(), 2)
    d['CriScore_Median'] = round(x['CriScore'].median(), 2)
    d['CriScore_Max'] = x['CriScore'].max()
    d['CriScore_Min'] = x['CriScore'].min()
    
    # Environment Score, measures the quality of the local environment
    d['EnvScore_Avg'] = round(x['EnvScore'].mean(), 2)
    d['EnvRank_Avg'] = round(x['EnvRank'].mean(), 2)
    d['EnvScore_Median'] = round(x['EnvScore'].median(), 2)
    d['EnvScore_Max'] = x['EnvScore'].max()
    d['EnvScore_Min'] = x['EnvScore'].min()

    # Geographical Barriers Score
    d['GBScore_Avg'] = round(x['GBScore'].mean(), 2)
    d['GBRank_Avg'] = round(x['GBRank'].mean(), 2)
    d['GBScore_Median'] = round(x['GBScore'].median(), 2)
    d['GBScore_Max'] = x['GBScore'].max()
    d['GBScore_Min'] = x['GBScore'].min()

    # Indoor Living Environment Score
    d['IndScore_Avg'] = round(x['IndScore'].mean(), 2)
    d['IndRank_Avg'] = round(x['IndRank'].mean(), 2)
    d['IndScore_Median'] = round(x['IndScore'].median(), 2)
    d['IndScore_Max'] = x['IndScore'].max()
    d['IndScore_Min'] = x['IndScore'].min()

    # Childrens and Young People's service
    d['CYPScore_Avg'] = round(x['CYPScore'].mean(), 2)
    d['CYPRank_Avg'] = round(x['CYPRank'].mean(), 2)
    d['CYPScore_Median'] = round(x['CYPScore'].median(), 2)
    d['CYPScore_Max'] = x['CYPScore'].max()
    d['CYPScore_Min'] = x['CYPScore'].min()

    return pd.Series(d, index=list(d.keys()))

aggregated_data = socio_economic_postcode.groupby("PostDist").apply(postcode_socio_grouby_agg).reset_index()

#%% Pick up the dataset from the Groupby Aggregations on the land registry dataset
# This dataset contains no geometry data, only the string code Postcode district
district_groupby = pd.read_csv("District_Ordered_Average%.csv")

#%%
district_groupby_socio_economic = pd.merge(district_groupby, aggregated_data, 
                                           how='left', left_on='postcode_district',
                                           right_on='PostDist')

# %%
cols = ['postcode_district', 'is_london?', 'AreaName', 'Locale',
        'property_type', 'year',
       'num_transactions', 'avg_price', 'stddev_price',
       '25th_percentile_price', '50th_percentile_price',
       '75th_percentile_price', 'coef_var', 'iqr', 'median_mean_diff',
       'median_mean_diff_pct', 'iqr_pct', 'lag_median_price',
       'median_pct_change_1_year', 'rolling_avg_median_pct_change_2_year',
       'rolling_avg_median_pct_change_5_year', 'is_good_sample',
       '2023_rolling_5_average', 'PostDist', 'geometry',
       'CountLowLevelAreas', 'AreaKm2', 'Population', 'Population_16-59',
       'Population_60+', 'PopulationDensity', 'IMDScore_Avg', 'IMDRank0_Avg',
       'IMDScore_Median', 'IMDScore_Max', 'IMDScore_Min', 'IncScore_Avg',
       'IncRank_Avg', 'IncScore_Median', 'IncScore_Max', 'IncScore_Min',
       'EduScore_Avg', 'EduRank_Avg', 'EduScore_Median', 'EduScore_Max',
       'EduScore_Min', 'CriScore_Avg', 'CriRank_Avg', 'CriScore_Median',
       'CriScore_Max', 'CriScore_Min', 'EnvScore_Avg', 'EnvRank_Avg',
       'EnvScore_Median', 'EnvScore_Max', 'EnvScore_Min', 'GBScore_Avg',
       'GBRank_Avg', 'GBScore_Median', 'GBScore_Max', 'GBScore_Min',
       'IndScore_Avg', 'IndRank_Avg', 'IndScore_Median', 'IndScore_Max',
       'IndScore_Min', 'CYPScore_Avg', 'CYPRank_Avg', 'CYPScore_Median',
       'CYPScore_Max', 'CYPScore_Min']

#%%
district_groupby_socio_economic = district_groupby_socio_economic[cols]

district_groupby_socio_economic_gdf = gpd.GeoDataFrame(district_groupby_socio_economic, geometry='geometry')
# %%
# district_groupby_socio_economic.to_excel("district_groupby_socio_economic.xlsx")

# district_groupby_socio_economic = district_groupby_socio_economic[district_groupby_socio_economic['PostDist']=='E3']

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
