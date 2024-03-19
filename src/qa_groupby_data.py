#%%----------------------------------------------------------------------------------------------------
#                     QA and prepare the Groupby Data processed by Apache Spark
#----------------------------------------------------------------------------------------------------
import pandas as pd
import regex as re
import matplotlib.pyplot as plt
import src.functions as func
import importlib
importlib.reload(func)

# %% 

district_groupby = pd.read_csv("District_Transaction_Groupby%.csv")
district_groupby['postcode_area'] = district_groupby['postcode_district'].apply(lambda x: re.sub(r'[^A-Za-z]', '', x))

prop_type_dict = {'T':'Terraced', 'S':'Semi-Detached', 'D':'Detached', 'F':'Flat', 'O':'Other'}
district_groupby['property_type'] = district_groupby['property_type'].map(prop_type_dict)

unique_districts = district_groupby['postcode_district'].unique().tolist()
#%%
property_type_groupby = district_groupby[district_groupby['year']==2023].groupby(['postcode_district','property_type'])['num_transactions'].sum().reset_index()
property_type_groupby.to_csv("property_type_groupby.csv")
# %%---------------------------------------------------------------------------------------
#          Order the dataset by largest % price increase in 2023, then show every
#            year after that so you can easily see in a table how the price has
#                       changed for that postcode district
# ---------------------------------------------------------------------------------------
# 
district_groupby = district_groupby[district_groupby['property_type']=='Flat']

# %% Optionally filter the dataset by Region of the UK

# district_groupby = district_groupby[district_groupby['is_london?']!='Outside London']
# %% Optionally filter the dataset by year

# district_groupby = district_groupby[district_groupby['year'] >= ]
# %%  Don't don't include Districts that have their most recent data below a certain number of samples

num_transactions_threshold = 60

districts_below_transactions_thresh = \
        district_groupby[(district_groupby['num_transactions']<=num_transactions_threshold) & \
                         (district_groupby['year']>= 2018)] \
                        ['postcode_district'].unique().tolist()
# %%
district_groupby = district_groupby[~district_groupby['postcode_district'].isin(districts_below_transactions_thresh)]

#%% Calculate the 5 year rolling average percentage price rise for the latest year (2023)

perc_price_rise_2023 = district_groupby[district_groupby['year']==2023][\
        ['postcode_district', 'property_type', 'rolling_avg_median_pct_change_5_year']]. \
        rename(columns={'rolling_avg_median_pct_change_5_year':'5YearAvg%PriceInc'})

#%% Merge the 2023 5 year rolling average onto the groupby so you can order by it for each postcode 
# district. 

district_groupby = pd.merge(district_groupby, perc_price_rise_2023, 
                           on=['postcode_district', 'property_type'])
#%%
district_groupby.sort_values(['5YearAvg%PriceInc', 'postcode_district', 
                            'year'],
                             ascending=[False, True, False], inplace=True)

#%% Remove unnecessary columns and then push to a csv file that will be read by the geospatial_merge
# function which merges this onto geospatial data and then creates a file which is read by streamlit
cols_to_remove = ['coef_var', 'iqr', 'median_mean_diff', 'median_mean_diff_pct',
                  'iqr_pct', 'lag_median_price', 'median_pct_change_1_year', 
                  'rolling_avg_median_pct_change_2_year', 'is_good_sample']

district_groupby.drop(columns=cols_to_remove).to_csv("District_Transaction_Groupby%.csv")

#%% Create a dataset of how property prices have changed over the years with minimal columns 
# to save memory for processing by streamlit

cols = ['postcode_district', 'is_london?', 'property_type','year',
       'num_transactions','avg_price', '50th_percentile_price']

district_groupby_price_graph = district_groupby[cols]

district_groupby_price_graph.to_csv("district_groupby_price_graph.csv")
# %%
plt.title("Average Price of Flat Vs Percentage Change By Postcode District")
plt.xlabel("Average Price")
plt.ylabel("5 Year Rolling Average % Change")
plt.scatter(district_groupby['avg_price'],district_groupby['rolling_avg_median_pct_change_5_year'])
# %%
test = district_groupby[(district_groupby['is_london?']!='Outside London')&(district_groupby['property_type']=='F')]
plt.title("Average Price of Flat Vs Number of transactions")
plt.xlabel("Average Price")
plt.ylabel("Number of Transactions")
plt.xlim(300000, 400000)
plt.ylim(50, 200)
plt.scatter(test['avg_price'],test['num_transactions'])
# %%
