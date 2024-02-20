#%%----------------------------------------------------------------------------------------------------
#                                       Pandas test area
#%%----------------------------------------------------------------------------------------------------

import pandas as pd
import regex as re
import matplotlib.pyplot as plt
import veetility
#%%
land_registry_data = pd.read_csv("land_registry_data.csv")
# %%
land_registry_data.columns
#%%
land_registry_data['postcode'].fillna('', inplace=True)
#%%
[postcode for postcode in land_registry_data['postcode'].unique() if postcode.startswith("EC1V")]
# %%
[postcode for postcode in land_registry_data['postcode'].unique() if postcode.startswith("E32")]
# %%
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

# %%
district_groupby = pd.read_csv("District_Prop_Type_Groupby.csv")
# %%
district_groupby = district_groupby[district_groupby['property_type']=='F']

# %%
# district_groupby = district_groupby[district_groupby['is_london?']!='Outside London']
# %%
# district_groupby = district_groupby[district_groupby['year'] >= ]
# %%
districts_below_transactions_thresh = district_groupby[district_groupby['num_transactions']<=60]['postcode_district'].unique().tolist()
# %%
district_groupby = district_groupby[~district_groupby['postcode_district'].isin(districts_below_transactions_thresh)]

#%%
perc_price_rise_2023 = district_groupby[district_groupby['year']==2023][['postcode_district', 'property_type', 'rolling_avg_median_pct_change_5_year']].rename(columns={'rolling_avg_median_pct_change_5_year':'2023_rolling_5_average'})

#%%
district_groupby = pd.merge(district_groupby, perc_price_rise_2023, 
                           on=['postcode_district', 'property_type'])
#%%
district_groupby.sort_values(['2023_rolling_5_average', 'postcode_district', 
                            'year'],
                             ascending=[False, True, False], inplace=True)

#%%
district_groupby.to_csv("District_Ordered_AverageALLYears%.csv")
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
