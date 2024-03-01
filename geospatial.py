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

    # Weighted Score of multiple factors, weightings can be found in "English_Socio_Economic.md"   
    d['OverallAvg'] = round(x['IMDScore'].mean(), 2)
    d['OverallRankAvg'] = round(x['IMDRank0'].mean(), 2)
    d['OverallMedian'] = round(x['IMDScore'].median(), 2)
    d['OverallMax'] = x['IMDScore'].max()
    d['OverallMin'] = x['IMDScore'].min()

    # Income Score
    d['IncomeAvg'] = round(x['IncScore'].mean(), 2)
    d['InccomeRankAvg'] = round(x['IncRank'].mean(), 2)
    d['IncomeMedian'] = round(x['IncScore'].median(), 2)
    d['IncomeMax'] = x['IncScore'].max()
    d['IncomeMin'] = x['IncScore'].min()

    # Education, skills and training
    d['EducationAvg'] = round(x['EduScore'].mean(), 2)
    d['EducationRankAvg'] = round(x['EduRank'].mean(), 2)
    d['EducationMedian'] = round(x['EduScore'].median(), 2)
    d['EducationMax'] = x['EduScore'].max()
    d['EducationMin'] = x['EduScore'].min()

    # Crime Score
    d['CrimeAvg'] = round(x['CriScore'].mean(), 2)
    d['CrimeRankAvg'] = round(x['CriRank'].mean(), 2)
    d['CrimeMedian'] = round(x['CriScore'].median(), 2)
    d['CrimeMax'] = x['CriScore'].max()
    d['CrimeMin'] = x['CriScore'].min()
    
    # Environment Score, measures the quality of the local environment
    d['EnvironmentAvg'] = round(x['EnvScore'].mean(), 2)
    d['EnvironmentRankAvg'] = round(x['EnvRank'].mean(), 2)
    d['EnvironmentMedian'] = round(x['EnvScore'].median(), 2)
    d['EnvironmentMax'] = x['EnvScore'].max()
    d['EnvironmentMin'] = x['EnvScore'].min()

    # Geographical Barriers Score, physical proximity to local services
    d['GeographBarriersAvg'] = round(x['GBScore'].mean(), 2)
    d['GeographBarriersRankAvg'] = round(x['GBRank'].mean(), 2)
    d['GeographBarriersMedian'] = round(x['GBScore'].median(), 2)
    d['GeographBarriersMax'] = x['GBScore'].max()
    d['GeographBarriersMin'] = x['GBScore'].min()

    # Indoor Living Environment Score
    d['IndoorLivingAvg'] = round(x['IndScore'].mean(), 2)
    d['IndRankAvg'] = round(x['IndRank'].mean(), 2)
    d['IndoorLivingMedian'] = round(x['IndScore'].median(), 2)
    d['IndoorLivingMax'] = x['IndScore'].max()
    d['IndoorLivingMin'] = x['IndScore'].min()

    # Childrens and Young People's service
    d['YoungServicesAvg'] = round(x['CYPScore'].mean(), 2)
    d['YoungServicesAvg'] = round(x['CYPRank'].mean(), 2)
    d['YoungServicesMedian'] = round(x['CYPScore'].median(), 2)
    d['YoungServicesMax'] = x['CYPScore'].max()
    d['YoungServicesMin'] = x['CYPScore'].min()

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
