#%%----------------------------------------------------------------------------------------------------
#                                       Pandas Test Area
#----------------------------------------------------------------------------------------------------

import pandas as pd
import regex as re
import matplotlib.pyplot as plt
import veetility
#%%----------------------------------------------------------------------------------------------------
#                     View the Land registry dataset although too large to work with in pandas
#------------------------------------------------------------------------------------------------------

land_registry_data = pd.read_csv("land_registry_data.csv")
# %%
land_registry_data.columns
#%%
land_registry_data['postcode'].fillna('', inplace=True)
#%%
[postcode for postcode in land_registry_data['postcode'].unique() if postcode.startswith("EC1V")]
# %%
[postcode for postcode in land_registry_data['postcode'].unique() if postcode.startswith("E32")]
# %% Sample Code for postcode splitting
postcode = 'E3 2PU'

parts = postcode.split()
area = re.sub(r'[^A-Za-z]', '', parts[0])  # Extracts letters before the space
district = re.sub(r'[^A-Za-z0-9]', '', parts[0])  # Extracts letters and digits before the space

# Check if there's a second part for the postcode
if len(parts) > 1 and parts[1]:
    sector = district + '-' + parts[1][0]  # Adds the first digit of the second part of the postcode
else:
    sector = 'Unknown'

print(area)
print(district)
print(sector)
# %%


# %%---------------------------------------------------------------------------------------
#                 Order the dataset by largest % price increase in 2023
# ---------------------------------------------------------------------------------------
# %% Dataset from EMR Cluster groupby aggregation of Land Registry Dataset
district_groupby = pd.read_csv("District_Prop_Type_Groupby.csv")
district_groupby = district_groupby[district_groupby['property_type']=='F']

# %%
# district_groupby = district_groupby[district_groupby['is_london?']!='Outside London']
# %%
# district_groupby = district_groupby[district_groupby['year'] >= ]
# %%  Districts below a certain number of samples, don't include any of their rows
num_transactions_threshold = 60

districts_below_transactions_thresh = \
        district_groupby[district_groupby['num_transactions']<=num_transactions_threshold] \
        ['postcode_district'].unique().tolist()
# %%
district_groupby = district_groupby[~district_groupby['postcode_district'].isin(districts_below_transactions_thresh)]

#%% Calculate the 5 year rolling average percentage price rise for the latest year (2023)
perc_price_rise_2023 = district_groupby[district_groupby['year']==2023][\
        ['postcode_district', 'property_type', 'rolling_avg_median_pct_change_5_year']]. \
        rename(columns={'rolling_avg_median_pct_change_5_year':'2023_rolling_5_average'})

#%% 
district_groupby = pd.merge(district_groupby, perc_price_rise_2023, 
                           on=['postcode_district', 'property_type'])
#%%
district_groupby.sort_values(['2023_rolling_5_average', 'postcode_district', 
                            'year'],
                             ascending=[False, True, False], inplace=True)

#%%
district_groupby.to_csv("District_Ordered_Average%.csv")

#%% Create a dataset of how property prices have changed over the years with minimal columns to save memory
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
